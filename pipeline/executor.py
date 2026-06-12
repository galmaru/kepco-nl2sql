"""[3-5] 읽기 전용 SQLite 실행기."""
from __future__ import annotations

import re
import sqlite3
import threading
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "kepco.db"
MAX_ROWS = 100
TIMEOUT_SEC = 5


def execute(sql: str) -> dict[str, Any]:
    """
    SQL을 읽기 전용으로 실행하고 결과 반환.

    Returns:
        {
            "success": bool,
            "columns": list[str],
            "rows": list[dict],
            "row_count": int,
            "error": str | None,
            "truncated": bool,
        }
    """
    # 이미 LIMIT 없으면 추가
    if not re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
        sql = sql.rstrip('; \n') + f' LIMIT {MAX_ROWS}'

    result: dict[str, Any] = {
        "success": False,
        "columns": [],
        "rows": [],
        "row_count": 0,
        "error": None,
        "truncated": False,
    }

    def _run() -> None:
        try:
            conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            result["columns"] = list(rows[0].keys()) if rows else (
                [d[0] for d in cursor.description] if cursor.description else []
            )
            result["rows"] = [dict(r) for r in rows]
            result["row_count"] = len(rows)
            result["truncated"] = len(rows) == MAX_ROWS
            result["success"] = True
            conn.close()
        except Exception as e:
            result["error"] = str(e)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=TIMEOUT_SEC)

    if thread.is_alive():
        result["error"] = f"실행 타임아웃 ({TIMEOUT_SEC}초 초과)"

    return result
