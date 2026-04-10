"""
한전 Open API JSON 데이터 → SQLite DB 변환 스크립트
data/raw/ 하위 13개 카테고리 폴더를 읽어 data/kepco.db에 적재
"""

import json
import sqlite3
import glob
import os
from pathlib import Path

RAW_DIR = Path("data/raw")
DB_PATH = Path("data/kepco.db")


def get_year_month_from_filename(filename: str):
    """2022_01.json → (2022, 01) 파싱"""
    stem = Path(filename).stem  # e.g. "2022_01"
    parts = stem.split("_")
    if len(parts) == 2:
        return parts[0], parts[1]
    elif len(parts) == 1 and parts[0].isdigit():
        return parts[0], None
    return None, None


def load_json(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def create_tables(conn: sqlite3.Connection):
    cur = conn.cursor()

    # 1. 청구서 유형별 발송 현황 (billing_type)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS billing_type (
            -- 청구서 유형별(모바일, 우편 등) 발송 건수 통계 테이블
            year        TEXT NOT NULL,       -- 연도 (e.g. '2022')
            month       TEXT NOT NULL,       -- 월 (e.g. '01')
            metro       TEXT NOT NULL,       -- 광역시도명
            city        TEXT NOT NULL,       -- 시군구명
            bill_type   TEXT NOT NULL,       -- 청구서 유형 (모바일/우편/이메일/인편)
            bill_count  INTEGER              -- 발송 건수
        )
    """)

    # 2. 업종별 전력사용 현황 - 업종 대분류 (business_type)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS business_type (
            -- 상세 업종별(반도체, 자동차 등) 전력 사용량 및 계약전력 통계 테이블
            year            TEXT NOT NULL,   -- 연도
            month           TEXT NOT NULL,   -- 월
            metro           TEXT NOT NULL,   -- 광역시도명
            city            TEXT NOT NULL,   -- 시군구명
            biz_type        TEXT NOT NULL,   -- 업종명
            cust_count      INTEGER,         -- 고객 수
            power_usage     REAL,            -- 전력 사용량 (kWh)
            contract_power  REAL             -- 계약전력 (kW)
        )
    """)

    # 3. 입찰/계약 공고 (contract)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contract (
            -- 한전 전자입찰계약 정보 및 공고 현황 테이블
            year                            TEXT,    -- 공고 연도 (noticeDate에서 파싱)
            month                           TEXT,    -- 공고 월
            purchase_type                   TEXT,    -- 구매 유형 (ConstructionService/Product 등)
            company_id                      TEXT,    -- 발주처 ID
            notice_no                       TEXT,    -- 공고번호
            notice_name                     TEXT,    -- 공고명
            presumed_price                  REAL,    -- 추정 가격
            notice_date                     TEXT,    -- 공고일
            bid_begin_datetime              TEXT,    -- 입찰 시작일시
            bid_end_datetime                TEXT,    -- 입찰 마감일시
            create_datetime                 TEXT,    -- 등록일시
            competition_type                TEXT,    -- 경쟁 방식 (Limited/Open 등)
            vendor_award_type               TEXT,    -- 낙찰 방식
            bid_type                        TEXT,    -- 입찰 유형 (QualifiedEval 등)
            place_name                      TEXT,    -- 담당 처소
            contract_req_dept               TEXT,    -- 계약 요청 부서
            presumed_amount                 REAL,    -- 추정 금액
            item_type                       TEXT,    -- 품목 유형 (Construction/Product 등)
            progress_state                  TEXT,    -- 진행 상태
            delivery_location               TEXT,    -- 납품 장소
            delivery_due_date               TEXT,    -- 납품 기한
            emergency_notice_yn             TEXT     -- 긴급 공고 여부
        )
    """)

    # 4. 계약종별 전력사용 현황 (contract_type)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contract_type (
            -- 전기 공급 계약종별(주택용, 산업용 등) 사용량 및 요금 통계 테이블
            year            TEXT NOT NULL,   -- 연도
            month           TEXT NOT NULL,   -- 월
            metro           TEXT NOT NULL,   -- 광역시도명
            city            TEXT NOT NULL,   -- 시군구명
            contract_type   TEXT NOT NULL,   -- 계약종 (주택용/일반용/산업용 등)
            cust_count      INTEGER,         -- 고객 수
            power_usage     REAL,            -- 전력 사용량 (kWh)
            bill            REAL,            -- 전기요금 (원)
            unit_cost       REAL,            -- 단가 (원/kWh)
            contract_power  REAL             -- 계약전력 (kW)
        )
    """)

    # 5. 산업별 고객 변동 현황 (industry_cust_change)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS industry_cust_change (
            -- 산업별 신규, 증설, 해지 등 고객 변동 현황 테이블
            year            TEXT NOT NULL,   -- 연도
            month           TEXT NOT NULL,   -- 월
            metro           TEXT NOT NULL,   -- 광역시도명
            city            TEXT NOT NULL,   -- 시군구명
            biz             TEXT NOT NULL,   -- 업종명
            new_count       INTEGER,         -- 신규 고객 수
            expansion_count INTEGER,         -- 증설 고객 수
            cancel_count    INTEGER          -- 해지 고객 수
        )
    """)

    # 6. 업종별 전력사용 현황 - 산업 대분류 (industry_type)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS industry_type (
            -- 한국표준산업분류(KSIC) 기반 대분류별 전력 사용량 및 요금 통계 테이블
            year        TEXT NOT NULL,   -- 연도
            month       TEXT NOT NULL,   -- 월
            metro       TEXT NOT NULL,   -- 광역시도명
            city        TEXT NOT NULL,   -- 시군구명
            biz         TEXT NOT NULL,   -- 산업 분류명
            cust_count  INTEGER,         -- 고객 수
            power_usage REAL,            -- 전력 사용량 (kWh)
            bill        REAL,            -- 전기요금 (원)
            unit_cost   REAL             -- 단가 (원/kWh)
        )
    """)

    # 7. 분산형 자원 현황 (dispersed_gen)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dispersed_gen (
            -- 변전소 및 배전선로별 분산형 전원 연계 정보 테이블
            subst_code      TEXT,    -- 변전소 코드
            subst_name      TEXT,    -- 변전소명
            js_subst_power  REAL,    -- 주상 변전소 전력
            subst_power     REAL,    -- 변전소 전력 용량
            meter_no        TEXT,    -- 계량기 번호
            js_meter_power  REAL,    -- 주상 계량기 전력
            meter_power     REAL,    -- 계량기 전력
            dl_code         TEXT,    -- 배전선로 코드
            dl_name         TEXT,    -- 배전선로명
            js_dl_power     REAL,    -- 주상 배전선로 전력
            dl_power        REAL,    -- 배전선로 전력
            vol1            REAL,    -- 전압 1 (용량)
            vol2            REAL,    -- 전압 2 (용량)
            vol3            REAL     -- 전압 3 (용량)
        )
    """)

    # 8. EV 충전소 현황 (ev_charge)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ev_charge (
            -- 지역별 전기차 충전소 위치 및 충전기 설치 수 현황 테이블
            metro           TEXT,    -- 광역시도명
            city            TEXT,    -- 시군구명
            station_place   TEXT,    -- 충전소 명칭
            station_addr    TEXT,    -- 충전소 주소
            rapid_count     INTEGER, -- 급속 충전기 수
            slow_count      INTEGER, -- 완속 충전기 수
            car_type        TEXT     -- 호환 차종 목록 (쉼표 구분)
        )
    """)

    # 9. EV 충전기 운영 현황 (ev_charge_manage)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ev_charge_manage (
            -- 전기차 충전기별 실시간 상태 및 운영 정보 테이블
            metro               TEXT,    -- 광역시도명 (파일명에서 파싱)
            station_id          TEXT,    -- 충전소 ID
            station_name        TEXT,    -- 충전소명
            charger_id          TEXT,    -- 충전기 ID
            charger_name        TEXT,    -- 충전기명
            charger_type        TEXT,    -- 충전기 타입 코드
            charge_type         TEXT,    -- 충전 방식 코드 (1:AC완속, 2:DC급속 등)
            charger_status      TEXT,    -- 충전기 상태 코드 (1:운영중 등)
            addr                TEXT,    -- 주소
            lat                 TEXT,    -- 위도
            lng                 TEXT,    -- 경도
            status_updated_at   TEXT     -- 상태 갱신 일시
        )
    """)

    # 10. 가구당 평균 전력사용량 (house_avg)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS house_avg (
            -- 지역별 가구수 대비 평균 전력 사용량 및 요금 통계 테이블
            year        TEXT NOT NULL,   -- 연도
            month       TEXT NOT NULL,   -- 월
            metro       TEXT NOT NULL,   -- 광역시도명
            city        TEXT NOT NULL,   -- 시군구명
            house_count INTEGER,         -- 가구 수
            power_usage REAL,            -- 평균 전력 사용량 (kWh)
            bill        REAL             -- 평균 전기요금 (원)
        )
    """)

    # 11. 신재생에너지 현황 (renew_energy)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS renew_energy (
            -- 발전원별(태양광, 풍력 등) 신재생에너지 설치소 및 용량 통계 테이블
            year            TEXT,    -- 기준 연도 (파일명에서 파싱)
            gen_source      TEXT,    -- 발전원 (태양광/풍력/소수력 등)
            metro           TEXT,    -- 광역시도명
            city            TEXT,    -- 시군구명
            count           INTEGER, -- 발전소 수
            capacity        REAL,    -- 발전 용량 (kW)
            area_count      INTEGER, -- 지역 내 전체 발전소 수
            area_capacity   REAL     -- 지역 내 전체 발전 용량 (kW)
        )
    """)

    # 12. 복지 할인 현황 (welfare_discount)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS welfare_discount (
            -- 복지 유형별(기초수급자, 장애인 등) 전기요금 할인 혜택 수혜자 현황 테이블
            year            TEXT NOT NULL,   -- 연도
            month           TEXT NOT NULL,   -- 월
            metro           TEXT NOT NULL,   -- 광역시도명
            city            TEXT NOT NULL,   -- 시군구명
            welfare_type    TEXT NOT NULL,   -- 복지 유형 (기초수급자/장애우/유공자 등)
            welfare_count   INTEGER          -- 복지 할인 고객 수
        )
    """)

    # 13. 공통 코드 (common_code)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS common_code (
            -- API 및 시스템에서 사용하는 각종 코드(지역, 계약종 등) 매핑 테이블
            code_type       TEXT NOT NULL,   -- 코드 유형 (metroCd/cityCd/cntrCd 등)
            upper_code      TEXT,            -- 상위 코드
            upper_code_name TEXT,            -- 상위 코드명
            code            TEXT NOT NULL,   -- 코드
            code_name       TEXT             -- 코드명
        )
    """)

    conn.commit()
    print("테이블 생성 완료")


# ─── 각 카테고리 적재 함수 ───────────────────────────────────────────────────

def load_billing_type(conn):
    files = sorted(glob.glob(str(RAW_DIR / "billing_type" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        # totData(전체 집계) + data(지역별) 모두 적재
        for key in ("totData", "data"):
            for item in data.get(key, []):
                rows.append((
                    item["year"], item["month"],
                    item["metro"], item["city"],
                    item["billTy"], item.get("billCnt"),
                ))
    conn.executemany(
        "INSERT INTO billing_type (year, month, metro, city, bill_type, bill_count) VALUES (?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_business_type(conn):
    files = sorted(glob.glob(str(RAW_DIR / "business_type" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                item["year"], item["month"],
                item["metro"], item["city"],
                item["bizType"],
                item.get("custCnt"),
                item.get("powerUsage"),
                item.get("cntrPwr"),
            ))
    conn.executemany(
        "INSERT INTO business_type (year, month, metro, city, biz_type, cust_count, power_usage, contract_power) VALUES (?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_contract(conn):
    files = sorted(glob.glob(str(RAW_DIR / "contract" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            notice_date = item.get("noticeDate", "")
            year = notice_date[:4] if notice_date else None
            month = notice_date[4:6] if notice_date and len(notice_date) >= 6 else None
            rows.append((
                year, month,
                item.get("purchaseType"),
                item.get("companyId"),
                item.get("no"),
                item.get("name"),
                item.get("presumedPrice"),
                notice_date,
                item.get("beginDatetime"),
                item.get("endDatetime"),
                item.get("createDatetime"),
                item.get("competitionType"),
                item.get("vendorAwardType"),
                item.get("bidType"),
                item.get("placeName"),
                item.get("contractReqDepartmentName"),
                item.get("presumedAmount"),
                item.get("itemType"),
                item.get("progressState"),
                item.get("deliveryLocation"),
                item.get("deliveryDueDate"),
                item.get("emergencyNoticeYn"),
            ))
    conn.executemany(
        """INSERT INTO contract
           (year, month, purchase_type, company_id, notice_no, notice_name,
            presumed_price, notice_date, bid_begin_datetime, bid_end_datetime,
            create_datetime, competition_type, vendor_award_type, bid_type,
            place_name, contract_req_dept, presumed_amount, item_type,
            progress_state, delivery_location, delivery_due_date, emergency_notice_yn)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows
    )
    conn.commit()
    return len(rows)


def load_contract_type(conn):
    files = sorted(glob.glob(str(RAW_DIR / "contract_type" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for key in ("totData", "data"):
            for item in data.get(key, []):
                rows.append((
                    item["year"], item["month"],
                    item["metro"], item["city"],
                    item["cntr"],
                    item.get("custCnt"),
                    item.get("powerUsage"),
                    item.get("bill"),
                    item.get("unitCost"),
                    item.get("cntrPwr"),
                ))
    conn.executemany(
        "INSERT INTO contract_type (year, month, metro, city, contract_type, cust_count, power_usage, bill, unit_cost, contract_power) VALUES (?,?,?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_industry_cust_change(conn):
    files = sorted(glob.glob(str(RAW_DIR / "industry_cust_change" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                item["year"], item["month"],
                item["metro"], item["city"],
                item["biz"],
                item.get("new"),
                item.get("expansion"),
                item.get("cancel"),
            ))
    conn.executemany(
        "INSERT INTO industry_cust_change (year, month, metro, city, biz, new_count, expansion_count, cancel_count) VALUES (?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_industry_type(conn):
    files = sorted(glob.glob(str(RAW_DIR / "industry_type" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for key in ("totData", "data"):
            for item in data.get(key, []):
                rows.append((
                    item["year"], item["month"],
                    item["metro"], item["city"],
                    item["biz"],
                    item.get("custCnt"),
                    item.get("powerUsage"),
                    item.get("bill"),
                    item.get("unitCost"),
                ))
    conn.executemany(
        "INSERT INTO industry_type (year, month, metro, city, biz, cust_count, power_usage, bill, unit_cost) VALUES (?,?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_dispersed_gen(conn):
    files = sorted(glob.glob(str(RAW_DIR / "dispersed_gen" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                item.get("substCd"), item.get("substNm"),
                item.get("jsSubstPwr"), _to_float(item.get("substPwr")),
                item.get("mtrNo"),
                item.get("jsMtrPwr"), _to_float(item.get("mtrPwr")),
                item.get("dlCd"), item.get("dlNm"),
                item.get("jsDlPwr"), item.get("dlPwr"),
                item.get("vol1"), item.get("vol2"), item.get("vol3"),
            ))
    conn.executemany(
        """INSERT INTO dispersed_gen
           (subst_code, subst_name, js_subst_power, subst_power,
            meter_no, js_meter_power, meter_power,
            dl_code, dl_name, js_dl_power, dl_power,
            vol1, vol2, vol3)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows
    )
    conn.commit()
    return len(rows)


def load_ev_charge(conn):
    files = sorted(glob.glob(str(RAW_DIR / "ev_charge" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                item.get("metro"), item.get("city"),
                item.get("stnPlace"), item.get("stnAddr"),
                item.get("rapidCnt"), item.get("slowCnt"),
                item.get("carType"),
            ))
    conn.executemany(
        "INSERT INTO ev_charge (metro, city, station_place, station_addr, rapid_count, slow_count, car_type) VALUES (?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_ev_charge_manage(conn):
    files = sorted(glob.glob(str(RAW_DIR / "ev_charge_manage" / "*.json")))
    rows = []
    for path in files:
        metro = Path(path).stem  # 파일명이 광역시도명
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                metro,
                item.get("csId"), item.get("csNm"),
                item.get("cpId"), item.get("cpNm"),
                item.get("cpTp"), item.get("chargeTp"),
                item.get("cpStat"),
                item.get("addr"),
                item.get("lat"), item.get("longi"),
                item.get("statUpdatedatetime"),
            ))
    conn.executemany(
        """INSERT INTO ev_charge_manage
           (metro, station_id, station_name, charger_id, charger_name,
            charger_type, charge_type, charger_status,
            addr, lat, lng, status_updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows
    )
    conn.commit()
    return len(rows)


def load_house_avg(conn):
    files = sorted(glob.glob(str(RAW_DIR / "house_avg" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                item["year"], item["month"],
                item["metro"], item["city"],
                item.get("houseCnt"),
                item.get("powerUsage"),
                item.get("bill"),
            ))
    conn.executemany(
        "INSERT INTO house_avg (year, month, metro, city, house_count, power_usage, bill) VALUES (?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_renew_energy(conn):
    files = sorted(glob.glob(str(RAW_DIR / "renew_energy" / "*.json")))
    rows = []
    for path in files:
        year, _ = get_year_month_from_filename(path)
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                year,
                item.get("genSrc"),
                item.get("metro"), item.get("city"),
                item.get("cnt"),
                item.get("capacity"),
                item.get("areaCnt"),
                item.get("areaCapacity"),
            ))
    conn.executemany(
        "INSERT INTO renew_energy (year, gen_source, metro, city, count, capacity, area_count, area_capacity) VALUES (?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_welfare_discount(conn):
    files = sorted(glob.glob(str(RAW_DIR / "welfare_discount" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                item["year"], item["month"],
                item["metro"], item["city"],
                item["wfType"],
                item.get("wfCnt"),
            ))
    conn.executemany(
        "INSERT INTO welfare_discount (year, month, metro, city, welfare_type, welfare_count) VALUES (?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_common_code(conn):
    files = sorted(glob.glob(str(RAW_DIR / "common_code" / "*.json")))
    rows = []
    for path in files:
        data = load_json(path)
        for item in data.get("data", []):
            rows.append((
                item.get("codeTy"),
                item.get("uppoCd"),
                item.get("uppoCdNm"),
                item.get("code"),
                item.get("codeNm"),
            ))
    conn.executemany(
        "INSERT INTO common_code (code_type, upper_code, upper_code_name, code, code_name) VALUES (?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def _to_float(val):
    """숫자형 문자열을 float으로 변환 (실패 시 None)"""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def print_row_counts(conn):
    tables = [
        "billing_type", "business_type", "contract", "contract_type",
        "industry_cust_change", "industry_type", "dispersed_gen",
        "ev_charge", "ev_charge_manage", "house_avg",
        "renew_energy", "welfare_discount", "common_code",
    ]
    print("\n" + "=" * 45)
    print(f"{'테이블명':<25} {'rows':>10}")
    print("=" * 45)
    total = 0
    for t in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"{t:<25} {cnt:>10,}")
        total += cnt
    print("=" * 45)
    print(f"{'합계':<25} {total:>10,}")


def main():
    # DB 파일이 이미 있으면 삭제 후 재생성
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"기존 DB 삭제: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    print(f"DB 생성: {DB_PATH}\n")
    create_tables(conn)

    loaders = [
        ("billing_type",         load_billing_type),
        ("business_type",        load_business_type),
        ("contract",             load_contract),
        ("contract_type",        load_contract_type),
        ("industry_cust_change", load_industry_cust_change),
        ("industry_type",        load_industry_type),
        ("dispersed_gen",        load_dispersed_gen),
        ("ev_charge",            load_ev_charge),
        ("ev_charge_manage",     load_ev_charge_manage),
        ("house_avg",            load_house_avg),
        ("renew_energy",         load_renew_energy),
        ("welfare_discount",     load_welfare_discount),
        ("common_code",          load_common_code),
    ]

    for name, fn in loaders:
        cnt = fn(conn)
        print(f"  ✓ {name:<25} {cnt:>8,} rows 적재")

    print_row_counts(conn)
    conn.close()
    print(f"\nDB 저장 완료: {DB_PATH}")


if __name__ == "__main__":
    main()
