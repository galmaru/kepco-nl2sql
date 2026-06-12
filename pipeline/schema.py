"""선택된 테이블의 스키마 컨텍스트 문자열 생성."""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "kepco.db"

# 테이블별 추가 설명 (DDL 주석 외 NL2SQL 주의사항)
_TABLE_NOTES: dict[str, str] = {
    "contract_type":     "계약종 값: 주택용/일반용/산업용/교육용/농사용/가로등/심 야",
    "industry_type":     "biz_code: bizCd (A~U, KEPCO01). 지역별 KSIC 대분류 전력 통계",
    "business_type":     "biz_type_code: bizTypeCd 01~38 (한전 세분류). 전국 집계 중심",
    "billing_type":      "bill_type 값: 모바일/우편/이메일/인편",
    "welfare_discount":  "welfare_type 값: 기초수급자/장애우/유공자/다자녀/대가족/사회복지시설/생명유지장치",
    "industry_cust_change": "biz_code: bizCd. 5개 광역시도만 존재 (서울·부산·울산·경기·세종)",
    "house_avg":         "5개 광역시도만 존재 (서울·부산·울산·경기·세종). power_usage 단위: kWh",
    "renew_energy":      "2024년 데이터만 존재. gen_source 값: 태양광/풍력/소수력/연료전지 등",
    "ev_charge":         "충전기 수 통계 (급속/완속). station_place·station_addr은 텍스트",
    "ev_charge_manage":  "status_updated_at 전체 빈값 — 사용 불가. city_code 없음",
    "contract":          "입찰·계약 공고 정보. presumed_amount는 낙찰 전 NULL 정상",
    "dispersed_gen":     "지역코드 없음. 변전소·배전선로 단위 분산전원 연계 정보",
    "common_code":       "code_type: metroCd/cityCd/cntrCd/bizCd/bizTypeCd",
}

_COMMON_CODE_NOTE = """
-- common_code JOIN 패턴:
--   지역명 표시: JOIN common_code m ON m.code_type='metroCd' AND m.code=t.metro_code
--   시군구 표시: JOIN common_code c ON c.code_type='cityCd' AND c.code=t.city_code AND c.upper_code=t.metro_code
--   city JOIN에 upper_code 조건 필수 (city_code는 광역시도 내 로컬 번호)
-- 지역 코드 조회: metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
-- 업종 코드 조회: biz_code   = (SELECT code FROM common_code WHERE code_type='bizCd'   AND code_name='제조업')
-- SQLite 주의: LEFT() 미지원 → SUBSTR(col,1,N) 사용
-- 집계행(metro='전체') 없음 — 전국 집계는 WHERE 없이 SUM/AVG
-- month 컬럼은 두 자리 문자열: '01'~'12' (한 자리 '1','2' 형태 사용 금지)
-- 평균 판매단가 계산: ROUND(SUM(bill)*1.0/NULLIF(SUM(power_usage),0),4) — AVG(unit_cost) 사용 금지
-- 여러 테이블 JOIN 시 데이터 없는 행 포함 필요 → LEFT JOIN 사용"""


def build_context(tables: list[str]) -> str:
    """선택 테이블의 DDL + 주의사항을 합쳐 프롬프트용 문자열 반환."""
    conn = sqlite3.connect(DB_PATH)
    parts: list[str] = []

    for tbl in tables:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
        ).fetchone()
        if not row:
            continue
        ddl = row[0]
        note = _TABLE_NOTES.get(tbl, "")
        parts.append(f"{ddl};\n-- {note}" if note else ddl + ";")

    conn.close()

    if not parts:
        return ""

    return "\n\n".join(parts) + "\n" + _COMMON_CODE_NOTE
