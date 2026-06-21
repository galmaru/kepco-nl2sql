"""SQLite(data/kepco.db) → PostgreSQL 마이그레이션 스크립트.

사용법:
  python migrate_to_pg.py          # 전체 테이블 마이그레이션
  python migrate_to_pg.py --drop   # 기존 테이블 삭제 후 재생성
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).resolve().parent / ".env")

SQLITE_PATH = Path(__file__).resolve().parent / "data" / "kepco.db"
BATCH_SIZE = 5000

# SQLite 타입 → PostgreSQL 타입 매핑
TYPE_MAP = {
    "TEXT":    "TEXT",
    "INTEGER": "INTEGER",
    "REAL":    "DOUBLE PRECISION",
    "BLOB":    "BYTEA",
    "NUMERIC": "NUMERIC",
    "":        "TEXT",  # 타입 미지정
}


def sqlite_to_pg_type(sqlite_type: str) -> str:
    upper = sqlite_type.upper().strip()
    for key, pg in TYPE_MAP.items():
        if upper.startswith(key):
            return pg
    return "TEXT"


def get_sqlite_tables(sq_conn) -> list[str]:
    rows = sq_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def get_column_info(sq_conn, table: str) -> list[dict]:
    """PRAGMA table_info로 컬럼 정보 반환."""
    rows = sq_conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [
        {"name": r[1], "type": r[2], "notnull": r[3]}
        for r in rows
    ]


def build_create_table(table: str, cols: list[dict]) -> str:
    col_defs = []
    for c in cols:
        pg_type = sqlite_to_pg_type(c["type"])
        not_null = " NOT NULL" if c["notnull"] else ""
        col_defs.append(f'  "{c["name"]}" {pg_type}{not_null}')
    return f'CREATE TABLE "{table}" (\n' + ",\n".join(col_defs) + "\n)"


def migrate_table(sq_conn, pg_conn, table: str, drop: bool) -> int:
    cols = get_column_info(sq_conn, table)
    if not cols:
        print(f"  [{table}] 컬럼 없음 — 건너뜀")
        return 0

    pg_cursor = pg_conn.cursor()

    if drop:
        pg_cursor.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
        pg_conn.commit()

    # 테이블 생성 (이미 있으면 스킵)
    create_sql = build_create_table(table, cols)
    try:
        pg_cursor.execute(create_sql)
        pg_conn.commit()
    except psycopg2.errors.DuplicateTable:
        pg_conn.rollback()
        print(f"  [{table}] 이미 존재 — 데이터만 삽입")

    # 기존 데이터 삭제 후 삽입
    pg_cursor.execute(f'DELETE FROM "{table}"')
    pg_conn.commit()

    # 데이터 복사 (배치)
    col_names = [c["name"] for c in cols]
    placeholders = ",".join(["%s"] * len(col_names))
    col_list = ",".join([f'"{c}"' for c in col_names])
    insert_sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'

    rows = sq_conn.execute(f'SELECT * FROM "{table}"').fetchall()
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        psycopg2.extras.execute_batch(pg_cursor, insert_sql, batch)
        pg_conn.commit()
        total += len(batch)

    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop", action="store_true", help="기존 테이블 삭제 후 재생성")
    parser.add_argument("--tables", nargs="+", help="특정 테이블만 마이그레이션")
    args = parser.parse_args()

    if not SQLITE_PATH.exists():
        print(f"[오류] SQLite DB 없음: {SQLITE_PATH}")
        return

    sq_conn = sqlite3.connect(SQLITE_PATH)
    pg_conn = psycopg2.connect(os.environ["DATABASE_URL"])

    tables = args.tables or get_sqlite_tables(sq_conn)
    print(f"마이그레이션 대상: {len(tables)}개 테이블 (drop={args.drop})\n")

    total_rows = 0
    for table in tables:
        print(f"  [{table}] 처리 중...", end=" ", flush=True)
        try:
            n = migrate_table(sq_conn, pg_conn, table, drop=args.drop)
            print(f"{n:,}행 완료")
            total_rows += n
        except Exception as e:
            pg_conn.rollback()
            print(f"실패: {e}")

    sq_conn.close()
    pg_conn.close()
    print(f"\n완료 — 총 {total_rows:,}행 이전")


if __name__ == "__main__":
    main()
