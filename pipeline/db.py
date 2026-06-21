"""PostgreSQL 연결 공통 헬퍼."""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def connect(readonly: bool = False):
    """DATABASE_URL 환경변수로 PostgreSQL 연결.

    readonly=True 시 세션 수준 읽기 전용 설정.
    """
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    if readonly:
        conn.set_session(readonly=True, autocommit=True)
    return conn
