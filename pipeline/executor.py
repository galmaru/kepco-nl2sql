"""[3-5] 읽기 전용 PostgreSQL 실행기."""
from __future__ import annotations

import re
import threading
from typing import Any

from .db import connect

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
            conn = connect(readonly=True)
            cursor = conn.cursor()
            cursor.execute(f"SET statement_timeout = {TIMEOUT_SEC * 1000}")
            cursor.execute(sql)
            rows_raw = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            result["columns"] = columns
            result["rows"] = [dict(zip(columns, row)) for row in rows_raw]
            result["row_count"] = len(rows_raw)
            result["truncated"] = len(rows_raw) == MAX_ROWS
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
