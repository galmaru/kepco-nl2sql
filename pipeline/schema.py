"""스키마 컨텍스트 생성.

build_full_context(semantic) 의 semantic 옵션:
  "none"     — DDL만 (컬럼명+타입+간단의미)
  "examples" — DDL + 컬럼별 예시값
  "full"     — DDL + 예시값 + 의미·단위·규칙
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "kepco.db"

# 전체 스키마 투입 시 제외할 테이블 (데이터 없거나 NL2SQL 무관)
_EXCLUDE_FROM_FULL = {"ev_charge_manage", "dispersed_gen"}

# ---------------------------------------------------------------------------
# 시맨틱 레이어 1: 컬럼별 예시값만
# ---------------------------------------------------------------------------
_SEMANTIC_EXAMPLES: dict[str, str] = {
    "contract_type": (
        "contract_type 예시: 주택용, 일반용, 산업용, 교육용, 농사용, 가로등, 심 야\n"
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

# ---------------------------------------------------------------------------
# 시맨틱 레이어 2: 예시값 + 의미·단위·규칙 (레이어 1 포함)
# ---------------------------------------------------------------------------
_SEMANTIC_FULL: dict[str, str] = {
    "contract_type": (
        "contract_type 예시: 주택용, 일반용, 산업용, 교육용, 농사용, 가로등, 심 야\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'\n"
        "metro_code: 광역시도 코드(metroCd) → common_code JOIN. city_code: 시군구 코드(cityCd)\n"
        "metro_code 값: 11=서울, 21=부산, 22=대구, 23=인천, 24=광주, 25=대전, 26=울산, "
        "31=경기, 32=강원, 33=충북, 34=충남, 35=전북, 36=전남, 37=경북, 38=경남, 39=제주, 41=세종\n"
        "power_usage 단위: kWh  |  bill 단위: 원  |  unit_cost 단위: 원/kWh  |  contract_power 단위: kW\n"
        "contract_type 컬럼 값은 텍스트 그대로 필터링 — 서브쿼리 불필요 (예: WHERE contract_type='산업용')\n"
        "unit_cost 직접 평균 금지 → 가중평균: ROUND(SUM(bill)/NULLIF(SUM(power_usage),0),4)\n"
        "주의: biz_code 컬럼 없음 — KSIC 산업별 조회는 industry_type 사용\n"
        "데이터 범위: 2021~2026년, 전국+광역시도+시군구"
    ),
    "industry_type": (
        "biz_code 예시: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U\n"
        "biz_code 의미: A=농업·임업·어업, B=광업, C=제조업, D=전기·가스·수도, E=하수·환경, "
        "F=건설업, G=도소매, H=운수, I=숙박음식, J=정보통신, K=금융보험, L=부동산, "
        "M=전문과학기술, N=사업시설관리, O=공공행정, P=교육, Q=보건복지, "
        "R=예술·스포츠, S=협회·단체, T=가구내고용, U=국제기관\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'\n"
        "metro_code: 광역시도 코드(metroCd). metro_code 값: 11=서울, 21=부산, 22=대구, 23=인천, "
        "24=광주, 25=대전, 26=울산, 31=경기, 32=강원, 33=충북, 34=충남, 35=전북, "
        "36=전남, 37=경북, 38=경남, 39=제주, 41=세종\n"
        "power_usage 단위: kWh  |  bill 단위: 원  |  unit_cost 단위: 원/kWh\n"
        "unit_cost 직접 평균 금지 → 가중평균: ROUND(SUM(bill)/NULLIF(SUM(power_usage),0),4)\n"
        "주의: contract_power 컬럼 없음 — KSIC 산업별(biz_code) 계약전력 조회 불가\n"
        "데이터 범위: 2021~2026년, 전국+광역시도+시군구"
    ),
    "billing_type": (
        "bill_type 예시: 이메일, 모바일, 카카오, 우편, '' (공백=미분류·기타)\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'\n"
        "bill_count: 해당 월 해당 유형 청구서 발송 건수\n"
        "전자청구서(이메일+모바일+카카오) 합산 방법 — bill_type별 GROUP BY 행 분리 금지:\n"
        "  SUM(CASE WHEN bill_type IN ('이메일','모바일','카카오') THEN bill_count ELSE 0 END) AS electronic_count\n"
        "  SUM(CASE WHEN bill_type = '우편' THEN bill_count ELSE 0 END) AS paper_count\n"
        "데이터 범위: 전국+광역시도+시군구"
    ),
    "welfare_discount": (
        "welfare_type 예시: 기초수급자, 장애우, 유공자, 다자녀, 대가족, 사회복지시설, 생명유지장치, 차상위\n"
        "year 예시: '2021'~'2026'  |  month 예시: '01'~'12'\n"
        "welfare_count: 해당 복지 유형 할인 수혜 고객 수\n"
        "데이터 범위: 전국+광역시도+시군구"
    ),
    "industry_cust_change": (
        "biz_code 예시: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, KEPCO01\n"
        "biz_code 의미: A=농업·임업·어업, C=제조업, F=건설업, I=숙박음식 등 (KSIC 대분류). KEPCO01=주택용(한전 자체)\n"
        "metro_code 존재값: 11(서울), 21(부산), 26(울산), 31(경기), 41(세종)\n"
        "주의: 5개 광역시도에만 데이터 존재 — 전국 집계 불가\n"
        "new_count=신규 계약, expansion_count=증설, cancel_count=해지\n"
        "데이터 범위: 2021~2026년"
    ),
    "house_avg": (
        "metro_code 존재값: 11(서울), 21(부산), 26(울산), 31(경기), 41(세종)\n"
        "주의: 5개 광역시도에만 데이터 존재 — 전국 집계 불가\n"
        "power_usage: 이미 가구당 평균(kWh) — AVG() 재집계 금지\n"
        "bill: 이미 가구당 평균 전기요금(원) — AVG() 재집계 금지\n"
        "데이터 범위: 2021~2026년"
    ),
    "renew_energy": (
        "gen_source 예시: 태양광, 풍력, 소수력, 연료전지, 바이오에너지\n"
        "year 예시: 2024 (2024년 데이터만 존재)\n"
        "capacity: 개별 발전소 용량(kW) — SUM하면 중복 집계됨, 지역 전체 설비용량으로 사용 금지\n"
        "area_capacity: 광역시도 전체 신재생 설비 합계(kW) — 같은 metro의 모든 행에 동일값 → 반드시 MAX(area_capacity) 사용\n"
        "지역 전체 신재생 설비용량 조회 예시: SELECT metro_code, MAX(area_capacity) FROM renew_energy GROUP BY metro_code\n"
        "area_count: 광역시도+발전원의 전체 발전소 수 — MAX(area_count) 사용"
    ),
    "ev_charge": (
        "rapid_count: 급속 충전기 수  |  slow_count: 완속 충전기 수\n"
        "급속/완속 합계 조회 시 SUM(rapid_count), SUM(slow_count), SUM(rapid_count)+SUM(slow_count) AS total 함께 포함\n"
        "metro_code 예시: 11, 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41"
    ),
    "common_code": (
        "code_type 값: metroCd(광역시도), cityCd(시군구), cntrCd(계약종), bizCd(KSIC 대분류), bizTypeCd(한전 세분류)\n"
        "metroCd 값: 11=서울, 21=부산, 22=대구, 23=인천, 24=광주, 25=대전, 26=울산, "
        "31=경기, 32=강원, 33=충북, 34=충남, 35=전북, 36=전남, 37=경북, 38=경남, 39=제주, 41=세종\n"
        "bizCd 값: A=농업·임업·어업, B=광업, C=제조업, D=전기·가스·수도, E=하수·환경, "
        "F=건설업, G=도소매, H=운수, I=숙박음식, J=정보통신, K=금융보험, L=부동산, "
        "M=전문과학기술, N=사업시설관리, O=공공행정, P=교육, Q=보건복지, "
        "R=예술·스포츠, S=협회·단체, T=가구내고용, U=국제기관\n"
        "지역명 → 코드: SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시'\n"
        "cityCd JOIN 시 upper_code 조건 필수: AND c.upper_code = t.metro_code"
    ),
    "contract":          "전력 사용량·요금 통계와 무관. 전자입찰 공고·계약 정보 전용",
    "dispersed_gen":     "지역 코드(metro_code/city_code) 없음 — 지역별 집계 불가",
    "ev_charge_manage":  "사용 불가: status_updated_at 전체 빈값. EV 조회 → ev_charge 사용",
}

# ---------------------------------------------------------------------------
# 공통 주의사항 (시맨틱 레이어별)
# ---------------------------------------------------------------------------
_COMMON_NOTE_EXAMPLES = """
-- ===== 공통 =====
-- month: '01'~'12' 두 자리 문자열
-- metro_code/city_code: common_code 테이블 JOIN으로 지역명 변환"""

_COMMON_NOTE_FULL = """
-- ===== SQL 작성 공통 주의사항 =====
-- [지역명 JOIN]
--   광역시도: JOIN common_code m ON m.code_type='metroCd' AND m.code=t.metro_code
--   시군구:   JOIN common_code c ON c.code_type='cityCd'  AND c.code=t.city_code AND c.upper_code=t.metro_code
-- [코드 → 명칭 조회]
--   SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시'
--   SELECT code FROM common_code WHERE code_type='bizCd'   AND code_name='제조업'
-- [SQLite]
--   LEFT() 미지원 → SUBSTR(col, 1, N) 사용
--   month: '01'~'12' 두 자리 문자열 (한 자리 '1','2' 형태 사용 금지)
-- [집계]
--   집계행 없음 — 전국 합계는 WHERE 없이 SUM/AVG
-- [지역명 통합]
--   강원도/강원특별자치도 → metro_code='32'
--   전라북도/전북특별자치도 → metro_code='35'
-- [JOIN]
--   여러 테이블 JOIN 시 LEFT JOIN 사용
-- [조회 불가 데이터]
--   KSIC 산업별(biz_code A~U) 계약전력: industry_type에 contract_power 컬럼 없음 → 조회 불가
--   계약종별(주택용/산업용 등) 계약전력: contract_type.contract_power(kW) 컬럼으로 조회 가능"""


def build_full_context(semantic: str = "full") -> str:
    """전체 테이블 DDL + 시맨틱 레이어를 합쳐 프롬프트용 문자열 반환.

    Args:
        semantic: "none" | "examples" | "full"
    """
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    conn.close()

    note_map = {
        "none":     {},
        "examples": _SEMANTIC_EXAMPLES,
        "full":     _SEMANTIC_FULL,
    }[semantic]

    parts: list[str] = []
    for name, ddl in rows:
        if name in _EXCLUDE_FROM_FULL or not ddl:
            continue
        note = note_map.get(name, "")
        if note:
            parts.append(f"{ddl};\n-- [시맨틱] {note}")
        else:
            parts.append(ddl + ";")

    common = {
        "none":     "",
        "examples": _COMMON_NOTE_EXAMPLES,
        "full":     _COMMON_NOTE_FULL,
    }[semantic]

    return "\n\n".join(parts) + common
