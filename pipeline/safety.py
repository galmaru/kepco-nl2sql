"""[3-4] SQL 안전성 검사: SELECT/WITH 전용, 위험 키워드 차단."""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "kepco.db"

_FORBIDDEN = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|ATTACH|DETACH|VACUUM|REINDEX|ANALYZE)\b',
    re.IGNORECASE,
)

_WRITE_PRAGMA = re.compile(r'\bPRAGMA\b.*(=|\bOFF\b|\bON\b|\bDELETE\b|\bFULL\b|\bWAL\b)',
                            re.IGNORECASE)

_ALLOWED_FIRST = {"SELECT", "WITH", "EXPLAIN"}


def _allowed_tables() -> set[str]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    conn.close()
    return {r[0] for r in rows}


def check(sql: str) -> tuple[bool, str]:
    """
    Returns:
        (ok, reason) — ok=True이면 실행 허용
    """
    # 주석 제거
    cleaned = re.sub(r'--[^\n]*', ' ', sql)
    cleaned = re.sub(r'/\*.*?\*/', ' ', cleaned, flags=re.DOTALL)
    stripped = cleaned.strip()

    if not stripped:
        return False, "빈 SQL"

    first = stripped.split()[0].upper()
    if first not in _ALLOWED_FIRST:
        return False, f"허용되지 않는 구문: {first} (SELECT/WITH/EXPLAIN만 허용)"

    if _FORBIDDEN.search(cleaned):
        m = _FORBIDDEN.search(cleaned)
        return False, f"금지 키워드 포함: {m.group()}"

    if _WRITE_PRAGMA.search(cleaned):
        return False, "쓰기 PRAGMA 금지"

    # 테이블명 검사 (FROM/JOIN 뒤에 오는 단어)
    referenced = set(re.findall(r'(?:FROM|JOIN)\s+(\w+)', cleaned, re.IGNORECASE))
    allowed = _allowed_tables()
    # CTE 및 서브쿼리 별칭은 허용 (WITH name AS (...) 또는 (...) AS name)
    cte_aliases = set(re.findall(r'\b(\w+)\s+AS\s*\(', cleaned, re.IGNORECASE))
    unknown = referenced - allowed - {"common_code"} - cte_aliases
    if unknown:
        return False, f"허용되지 않는 테이블: {unknown}"

    return True, "ok"
