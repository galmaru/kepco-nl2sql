#!/usr/bin/env python3
"""
Collect BIRD dev database/table/column metadata with real SQLite samples.

Input:
  data/bird/dev_20240627/dev_tables.json
  data/bird/dev_20240627/dev.json
  data/bird/dev_20240627/dev_databases/{db_id}/{db_id}.sqlite
  data/bird/dev_20240627/dev_databases/{db_id}/database_description/*.csv

Output:
  data/bird/bird_dev_schema_samples.json
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SAMPLE_ROWS = 3
MAX_TEXT_CHARS = 500


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def clean_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str) and len(value) > MAX_TEXT_CHARS:
        return value[:MAX_TEXT_CHARS] + "...[truncated]"
    return value


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def extract_db_from_zip(db_zip: Path, db_id: str, temp_dir: Path) -> Path:
    prefix = f"train_databases/{db_id}/"
    target_root = temp_dir / "train_databases"
    target_db_dir = target_root / db_id
    if target_db_dir.exists():
        shutil.rmtree(target_db_dir)

    with zipfile.ZipFile(db_zip) as zf:
        members = [
            name
            for name in zf.namelist()
            if name.startswith(prefix) and not name.endswith(".DS_Store")
        ]
        if not members:
            raise FileNotFoundError(f"No members found for {db_id} in {db_zip}")
        for member in members:
            zf.extract(member, temp_dir)

    db_path = target_db_dir / f"{db_id}.sqlite"
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not extracted: {db_path}")
    return target_root


def read_description_csv(path: Path) -> dict[str, dict[str, str]]:
    descriptions: dict[str, dict[str, str]] = {}
    if not path.exists():
        return descriptions

    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with path.open(encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    original = (row.get("original_column_name") or "").strip()
                    if not original:
                        continue
                    descriptions[original] = {
                        "column_name": (row.get("column_name") or "").strip(),
                        "column_description": (row.get("column_description") or "").strip(),
                        "data_format": (row.get("data_format") or "").strip(),
                        "value_description": (row.get("value_description") or "").strip(),
                    }
            return descriptions
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error:
        raise last_error
    return descriptions


def build_bird_column_map(table_meta: dict[str, Any]) -> dict[str, dict[str, Any]]:
    table_names = table_meta["table_names_original"]
    readable_table_names = table_meta.get("table_names", table_names)
    by_table: dict[str, dict[str, Any]] = defaultdict(dict)

    for idx, ((table_idx, original_name), (_, readable_name)) in enumerate(
        zip(table_meta["column_names_original"], table_meta["column_names"])
    ):
        if table_idx < 0:
            continue
        table_name = table_names[table_idx]
        by_table[table_name][original_name] = {
            "bird_column_index": idx,
            "bird_name": readable_name,
            "bird_type": table_meta["column_types"][idx],
            "bird_table_name": readable_table_names[table_idx],
        }
    return by_table


def normalize_primary_keys(primary_keys: list[Any]) -> set[int]:
    keys: set[int] = set()
    for key in primary_keys:
        if isinstance(key, list):
            keys.update(int(k) for k in key)
        else:
            keys.add(int(key))
    return keys


def build_bird_fk_map(table_meta: dict[str, Any]) -> dict[int, list[dict[str, str]]]:
    column_names = table_meta["column_names_original"]
    table_names = table_meta["table_names_original"]
    fk_map: dict[int, list[dict[str, str]]] = defaultdict(list)

    for source_idx, target_idx in table_meta.get("foreign_keys", []):
        source_table_idx, source_col = column_names[source_idx]
        target_table_idx, target_col = column_names[target_idx]
        fk_map[source_idx].append(
            {
                "from_table": table_names[source_table_idx],
                "from_column": source_col,
                "to_table": table_names[target_table_idx],
                "to_column": target_col,
            }
        )
    return fk_map


def fetch_table_samples(conn: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"SELECT * FROM {quote_ident(table_name)} LIMIT {SAMPLE_ROWS}"
    ).fetchall()
    return [{key: clean_value(row[key]) for key in row.keys()} for row in rows]


def collect_sqlite_table(conn: sqlite3.Connection, db_path: Path, table_name: str) -> dict[str, Any]:
    columns = conn.execute(f"PRAGMA table_info({quote_ident(table_name)})").fetchall()
    foreign_keys = conn.execute(f"PRAGMA foreign_key_list({quote_ident(table_name)})").fetchall()
    row_count = conn.execute(f"SELECT COUNT(*) FROM {quote_ident(table_name)}").fetchone()[0]
    sample_rows = fetch_table_samples(conn, table_name)

    fk_by_column: dict[str, list[dict[str, str]]] = defaultdict(list)
    for fk in foreign_keys:
        fk_by_column[fk[3]].append(
            {
                "to_table": fk[2],
                "from_column": fk[3],
                "to_column": fk[4],
                "on_update": fk[5],
                "on_delete": fk[6],
            }
        )

    return {
        "table_name": table_name,
        "row_count": row_count,
        "columns": columns,
        "foreign_keys": foreign_keys,
        "foreign_keys_by_column": fk_by_column,
        "sample_rows": sample_rows,
        "sqlite_path": str(db_path),
    }


def collect(
    *,
    split_name: str,
    bird_dir: Path,
    tables_json: Path,
    questions_json: Path,
    db_dir: Path,
    out_path: Path,
    db_zip: Path | None = None,
    temp_dir: Path | None = None,
) -> dict[str, Any]:
    tables_meta = read_json(tables_json)
    question_rows = read_json(questions_json)

    question_counts = Counter(row["db_id"] for row in question_rows)
    difficulty_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in question_rows:
        difficulty_counts[row["db_id"]][row.get("difficulty", "")] += 1

    databases = []
    total_tables = 0
    total_columns = 0

    for table_meta in tables_meta:
        db_id = table_meta["db_id"]
        active_db_dir = db_dir
        if db_zip is not None:
            if temp_dir is None:
                raise ValueError("temp_dir is required when db_zip is set")
            active_db_dir = extract_db_from_zip(db_zip, db_id, temp_dir)

        db_path = active_db_dir / db_id / f"{db_id}.sqlite"
        if not db_path.exists():
            raise FileNotFoundError(f"SQLite DB not found: {db_path}")

        bird_columns_by_table = build_bird_column_map(table_meta)
        bird_primary_keys = normalize_primary_keys(table_meta.get("primary_keys", []))
        bird_fk_map = build_bird_fk_map(table_meta)

        tables = []
        with sqlite3.connect(db_path) as conn:
            for table_idx, table_name in enumerate(table_meta["table_names_original"]):
                readable_table_name = table_meta.get("table_names", [])[table_idx]
                desc_path = active_db_dir / db_id / "database_description" / f"{table_name}.csv"
                descriptions = read_description_csv(desc_path)
                sqlite_table = collect_sqlite_table(conn, db_path, table_name)
                sqlite_fks_by_column = sqlite_table["foreign_keys_by_column"]

                columns = []
                for cid, name, sqlite_type, not_null, default_value, pk_position in sqlite_table["columns"]:
                    bird_info = bird_columns_by_table.get(table_name, {}).get(name, {})
                    bird_index = bird_info.get("bird_column_index")
                    desc = descriptions.get(name, {})
                    sample_values = [
                        row.get(name)
                        for row in sqlite_table["sample_rows"]
                        if row.get(name) is not None
                    ]

                    columns.append(
                        {
                            "name": name,
                            "bird_name": bird_info.get("bird_name", ""),
                            "sqlite_type": sqlite_type,
                            "bird_type": bird_info.get("bird_type", ""),
                            "not_null": bool(not_null),
                            "default_value": clean_value(default_value),
                            "primary_key_position": pk_position,
                            "is_primary_key": bool(pk_position)
                            or (bird_index in bird_primary_keys if bird_index is not None else False),
                            "foreign_keys": sqlite_fks_by_column.get(name, [])
                            + (bird_fk_map.get(bird_index, []) if bird_index is not None else []),
                            "description": desc.get("column_description", ""),
                            "description_name": desc.get("column_name", ""),
                            "data_format": desc.get("data_format", ""),
                            "value_description": desc.get("value_description", ""),
                            "sample_values": sample_values,
                        }
                    )

                tables.append(
                    {
                        "name": table_name,
                        "bird_name": readable_table_name,
                        "row_count": sqlite_table["row_count"],
                        "description_csv": str(desc_path) if desc_path.exists() else None,
                        "columns": columns,
                        "sample_rows": sqlite_table["sample_rows"],
                    }
                )
                total_columns += len(columns)

        total_tables += len(tables)
        databases.append(
            {
                "db_id": db_id,
                "sqlite_path": (
                    f"{db_zip}::train_databases/{db_id}/{db_id}.sqlite"
                    if db_zip is not None
                    else str(db_path)
                ),
                "table_count": len(tables),
                "column_count": sum(len(t["columns"]) for t in tables),
                "question_count": question_counts[db_id],
                "difficulty_counts": dict(difficulty_counts[db_id]),
                "tables": tables,
            }
        )

        if db_zip is not None and temp_dir is not None:
            extracted_dir = active_db_dir / db_id
            if extracted_dir.exists():
                shutil.rmtree(extracted_dir)

    return {
        "metadata": {
            "source": f"BIRD {split_name}",
            "split": split_name,
            "bird_dir": str(bird_dir),
            "questions_json": str(questions_json),
            "tables_json": str(tables_json),
            "databases_dir": str(db_dir),
            "databases_zip": str(db_zip) if db_zip is not None else None,
            "sample_rows_per_table": SAMPLE_ROWS,
            "database_count": len(databases),
            "table_count": total_tables,
            "column_count": total_columns,
            "question_count": len(question_rows),
        },
        "databases": databases,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--split-name", default="dev_20240627")
    parser.add_argument("--bird-dir", type=Path, default=Path("data/bird/dev_20240627"))
    parser.add_argument("--tables-json", type=Path)
    parser.add_argument("--questions-json", type=Path)
    parser.add_argument("--db-dir", type=Path)
    parser.add_argument("--db-zip", type=Path)
    parser.add_argument("--temp-dir", type=Path, default=Path("data/bird/_tmp_db_extract"))
    parser.add_argument("--out", type=Path, default=Path("data/bird/bird_dev_schema_samples.json"))
    args = parser.parse_args()

    tables_json = args.tables_json or args.bird_dir / "dev_tables.json"
    questions_json = args.questions_json or args.bird_dir / "dev.json"
    db_dir = args.db_dir or args.bird_dir / "dev_databases"

    catalog = collect(
        split_name=args.split_name,
        bird_dir=args.bird_dir,
        tables_json=tables_json,
        questions_json=questions_json,
        db_dir=db_dir,
        out_path=args.out,
        db_zip=args.db_zip,
        temp_dir=args.temp_dir,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)

    meta = catalog["metadata"]
    print(f"Wrote {args.out}")
    print(
        f"databases={meta['database_count']} tables={meta['table_count']} "
        f"columns={meta['column_count']} questions={meta['question_count']}"
    )


if __name__ == "__main__":
    main()
