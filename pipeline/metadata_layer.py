"""메타데이터 레이어: DDL + 컬럼별 예시값."""
from __future__ import annotations

from .db import connect

EXCLUDE_FROM_FULL: set[str] = {"ev_charge_manage", "dispersed_gen"}

EXAMPLES: dict[str, str] = {
    "contract_type": (
        "contract_type 예시: 주택용, 일반용, 산업용, 교육용, 농사용, 가로등, 심야\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'\n"
        "metro_code 예시: 11, 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41"
    ),
    "industry_type": (
        "biz_code 예시: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'\n"
        "metro_code 예시: 11, 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41"
    ),
    "billing_type": (
        "bill_type 예시: 이메일, 모바일, 카카오, 우편, '' (공백=미분류)\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'"
    ),
    "welfare_discount": (
        "welfare_type 예시: 기초수급자, 장애우, 유공자, 다자녀, 대가족, 사회복지시설, 생명유지장치, 차상위\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'"
    ),
    "industry_cust_change": (
        "biz_code 예시: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, KEPCO01\n"
        "metro_code 존재값: 11, 21, 26, 31, 41  |  year 예시: '2021'~'2026'  |  month 예시: '01'~'12'"
    ),
    "house_avg": (
        "metro_code 존재값: 11, 21, 26, 31, 41\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'"
    ),
    "renew_energy": (
        "gen_source 예시: 태양광, 풍력, 소수력, 연료전지, 바이오에너지\n"
        "year 예시: 2024 (2024년 데이터만 존재)"
    ),
    "ev_charge": (
        "metro_code 예시: 11, 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41"
    ),
    "common_code": (
        "code_type 예시: metroCd, cityCd, cntrCd, bizCd, bizTypeCd\n"
        "code 예시(metroCd): 11, 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41\n"
        "code 예시(bizCd): A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U"
    ),
    "contract":          "",
    "dispersed_gen":     "",
    "ev_charge_manage":  "",
}


def get_ddl_rows() -> list[tuple[str, str]]:
    """public 스키마의 테이블 목록과 DDL을 반환."""
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cursor.fetchall()]

    result = []
    for table in tables:
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        cols = cursor.fetchall()
        if not cols:
            result.append((table, ""))
            continue
        col_defs = ",\n  ".join(f"{col[0]} {col[1].upper()}" for col in cols)
        ddl = f"CREATE TABLE {table} (\n  {col_defs}\n)"
        result.append((table, ddl))

    conn.close()
    return result
