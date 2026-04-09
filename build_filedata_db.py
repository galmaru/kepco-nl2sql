"""
파일데이터셋 → SQLite DB 추가 적재 스크립트
data/filedatasets/files/ → data/kepco.db (기존 DB에 추가)

추가 테이블 12종:
  A1. ev_station_location   - 전기차충전소 위경도
  A2. ev_charger_capacity   - 충전소별 충전기 용량
  B1. sales_stat            - 영업통계 (시도·계약종별)
  B2. hourly_power          - 전국 시간별 전력사용량
  C1. dong_industry_power   - 산업분류별 법정동별 전력사용량
  C2. industry_utilization  - 산업분류별 월별 수용률
  C3. industry_code         - 산업 분류 코드 테이블
  D1. ppa_by_source         - 발전원별 PPA 계약현황
  D2. ppa_by_region         - 지역별 PPA 계약현황
  D3. net_metering_usage    - 법정동별 상계거래 전력사용량
  D4. net_metering_surplus  - 법정동별 상계거래 잉여전력량
  E1. tariff_adjustment     - 전기요금 조정율
"""

import csv
import io
import sqlite3
from pathlib import Path

FILES_DIR = Path("data/filedatasets/files")
DB_PATH = Path("data/kepco.db")


# ── 헬퍼 ────────────────────────────────────────────────────────────────────

def read_csv(pk: str, encoding: str = "utf-8-sig") -> tuple[list[str], list[list[str]]]:
    """파일데이터셋 CSV 읽기 → (headers, rows)"""
    matches = list(FILES_DIR.glob(f"{pk}_*"))
    if not matches:
        raise FileNotFoundError(f"pk={pk} 파일 없음")
    path = matches[0]
    raw = path.read_bytes().decode(encoding, errors="replace")
    reader = csv.reader(io.StringIO(raw))
    rows = [r for r in reader if any(c.strip() for c in r)]
    return rows[0], rows[1:]


def safe_int(v):
    try:
        return int(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def safe_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def bulk_insert(conn, table: str, cols: list[str], rows: list[tuple], batch=5000):
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    cur = conn.cursor()
    for i in range(0, len(rows), batch):
        cur.executemany(sql, rows[i : i + batch])
    conn.commit()
    print(f"  → {table}: {len(rows):,}행 적재 완료")


# ── 테이블 생성 ──────────────────────────────────────────────────────────────

def create_tables(conn):
    conn.executescript("""
        -- A1. 전기차충전소 위경도
        CREATE TABLE IF NOT EXISTS ev_station_location (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id  TEXT,       -- 충전소ID (ev_charge_manage.station_id 연계)
            station_name TEXT,      -- 충전소명
            address     TEXT,       -- 충전소주소
            lat         REAL,       -- 위도
            lng         REAL        -- 경도
        );

        -- A2. 충전소별 충전기 용량
        CREATE TABLE IF NOT EXISTS ev_charger_capacity (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id      TEXT,   -- 충전소ID
            station_name    TEXT,   -- 충전소명
            charger_id      TEXT,   -- 충전기ID (ev_charge_manage.charger_id 연계)
            charger_name    TEXT,   -- 충전기명
            capacity_kw     REAL    -- 충전기 용량(kW)
        );

        -- B1. 영업통계 (시도·계약종별)
        CREATE TABLE IF NOT EXISTS sales_stat (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            period          TEXT,   -- 조회기간 (YYYY-MM)
            metro           TEXT,   -- 시도
            contract_type   TEXT,   -- 계약종별
            cust_count      INTEGER, -- 고객호수
            power_usage     REAL,   -- 판매량 (kWh)
            revenue         REAL    -- 판매수입 (원)
        );

        -- B2. 전국 시간별 전력사용량
        CREATE TABLE IF NOT EXISTS hourly_power (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT,       -- 기준일자 (YYYY-MM-DD)
            hour        INTEGER,    -- 기준시 (0~23)
            region      TEXT,       -- 본부명
            power_usage REAL,       -- 전력사용량 (MWh)
            cust_count  INTEGER     -- 고객호수
        );

        -- C1. 산업분류별 법정동별 전력사용량
        CREATE TABLE IF NOT EXISTS dong_industry_power (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            year            TEXT,   -- 년도
            month           TEXT,   -- 월
            metro           TEXT,   -- 시도
            city            TEXT,   -- 시군구
            dong            TEXT,   -- 읍면동(법정동)
            biz_code_large  TEXT,   -- 산업분류코드(대)
            biz_name_large  TEXT,   -- 산업분류명(대)
            biz_code_mid    TEXT,   -- 산업분류코드(중)
            biz_name_mid    TEXT,   -- 산업분류명(중)
            cust_count      INTEGER, -- 고객호수
            power_usage     REAL,   -- 판매량 (kWh)
            revenue         REAL    -- 판매요금 (원)
        );

        -- C2. 산업분류별 월별 수용률
        CREATE TABLE IF NOT EXISTS industry_utilization (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            year            TEXT,   -- 연도
            biz_name        TEXT,   -- 산업분류명
            power_range     TEXT,   -- 계약전력구간
            util_jan        REAL,   -- 수용률_1월
            util_feb        REAL,   -- 수용률_2월
            util_mar        REAL,   -- 수용률_3월
            util_apr        REAL,   -- 수용률_4월
            util_may        REAL,   -- 수용률_5월
            util_jun        REAL,   -- 수용률_6월
            util_jul        REAL,   -- 수용률_7월
            util_aug        REAL,   -- 수용률_8월
            util_sep        REAL,   -- 수용률_9월
            util_oct        REAL,   -- 수용률_10월
            util_nov        REAL,   -- 수용률_11월
            util_dec        REAL    -- 수용률_12월
        );

        -- C3. 산업 분류 코드 테이블
        CREATE TABLE IF NOT EXISTS industry_code (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            seq         INTEGER,    -- 순번
            code        TEXT,       -- 산업분류코드
            name        TEXT        -- 항목명(업종명)
        );

        -- D1. 발전원별 PPA 계약현황
        CREATE TABLE IF NOT EXISTS ppa_by_source (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            year            TEXT,   -- 년도
            gen_source      TEXT,   -- 발전원 (renew_energy.gen_source 연계)
            vendor_count    INTEGER, -- 사업자수
            capacity_kw     REAL    -- 설비용량 (kW)
        );

        -- D2. 지역별 PPA 계약현황
        CREATE TABLE IF NOT EXISTS ppa_by_region (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            gen_source      TEXT,   -- 발전원
            region          TEXT,   -- 지역구분 (광역)
            city            TEXT,   -- 시도구분 (시군구)
            count           INTEGER, -- 개수
            capacity_kw     REAL,   -- 용량(kW)
            region_count    INTEGER, -- 지역개수
            region_capacity REAL    -- 지역용량(kW)
        );

        -- D3. 법정동별 상계거래 전력사용량
        CREATE TABLE IF NOT EXISTS net_metering_usage (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            metro       TEXT,       -- 시도
            city        TEXT,       -- 시군구
            dong        TEXT,       -- 법정동
            year        TEXT,       -- 년도
            month       TEXT,       -- 월
            total_count INTEGER,    -- 전체호수
            power_usage REAL        -- 전력사용량 (kWh)
        );

        -- D4. 법정동별 상계거래 잉여전력량
        CREATE TABLE IF NOT EXISTS net_metering_surplus (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            metro           TEXT,   -- 시도
            city            TEXT,   -- 시군구
            dong            TEXT,   -- 법정동
            year            TEXT,   -- 년도
            month           TEXT,   -- 월
            total_count     INTEGER, -- 전체호수
            surplus_power   REAL    -- 잉여전력량 (kWh)
        );

        -- E1. 전기요금 조정율
        CREATE TABLE IF NOT EXISTS tariff_adjustment (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            year        TEXT,       -- 조정연도
            month       TEXT,       -- 조정월
            residential REAL,       -- 주택용 (%)
            general     REAL,       -- 일반용 (%)
            education   REAL,       -- 교육용 (%)
            industrial  REAL,       -- 산업용 (%)
            agriculture REAL,       -- 농사용 (%)
            street_lamp REAL,       -- 가로등 (%)
            total       REAL        -- 종합 (%)
        );
    """)
    conn.commit()
    print("테이블 생성 완료")


# ── 적재 함수들 ──────────────────────────────────────────────────────────────

def load_ev_station_location(conn):
    """A1. 전기차충전소위경도 (UTF-8)"""
    headers, rows = read_csv("15102458")
    data = []
    for r in rows:
        if len(r) < 5:
            continue
        data.append((r[0].strip(), r[1].strip(), r[2].strip(),
                     safe_float(r[3]), safe_float(r[4])))
    bulk_insert(conn, "ev_station_location",
                ["station_id", "station_name", "address", "lat", "lng"], data)


def load_ev_charger_capacity(conn):
    """A2. 충전소별 충전기 용량 정보 (EUC-KR)"""
    headers, rows = read_csv("15039557", "euc-kr")
    data = []
    for r in rows:
        if len(r) < 5:
            continue
        data.append((r[0].strip(), r[1].strip(), r[2].strip(),
                     r[3].strip(), safe_float(r[4])))
    bulk_insert(conn, "ev_charger_capacity",
                ["station_id", "station_name", "charger_id", "charger_name", "capacity_kw"], data)


def load_sales_stat(conn):
    """B1. 영업통계정보 (UTF-8)"""
    headers, rows = read_csv("15053175")
    data = []
    for r in rows:
        if len(r) < 6:
            continue
        data.append((r[0].strip(), r[1].strip(), r[2].strip(),
                     safe_int(r[3]), safe_float(r[4]), safe_float(r[5])))
    bulk_insert(conn, "sales_stat",
                ["period", "metro", "contract_type", "cust_count", "power_usage", "revenue"], data)


def load_hourly_power(conn):
    """B2. 전국 시간별 전력사용량 (UTF-8, 394K행)"""
    headers, rows = read_csv("15151157")
    data = []
    for r in rows:
        if len(r) < 5:
            continue
        data.append((r[0].strip(), safe_int(r[1]), r[2].strip(),
                     safe_float(r[3]), safe_int(r[4])))
    bulk_insert(conn, "hourly_power",
                ["date", "hour", "region", "power_usage", "cust_count"], data)


def load_dong_industry_power(conn):
    """C1. 산업분류별 법정동별 전력사용량 (EUC-KR, 554K행)"""
    headers, rows = read_csv("15104908", "euc-kr")
    data = []
    for r in rows:
        if len(r) < 12:
            continue
        data.append((
            r[0].strip(), r[1].strip(), r[2].strip(), r[3].strip(), r[4].strip(),
            r[5].strip(), r[6].strip(), r[7].strip(), r[8].strip(),
            safe_int(r[9]), safe_float(r[10]), safe_float(r[11])
        ))
    bulk_insert(conn, "dong_industry_power",
                ["year", "month", "metro", "city", "dong",
                 "biz_code_large", "biz_name_large", "biz_code_mid", "biz_name_mid",
                 "cust_count", "power_usage", "revenue"], data)


def load_industry_utilization(conn):
    """C2. 산업분류별 월별 수용률 현황 (UTF-8)"""
    headers, rows = read_csv("15152702")
    data = []
    for r in rows:
        if len(r) < 15:
            continue
        data.append((
            r[0].strip(), r[1].strip(), r[2].strip(),
            safe_float(r[3]), safe_float(r[4]), safe_float(r[5]),
            safe_float(r[6]), safe_float(r[7]), safe_float(r[8]),
            safe_float(r[9]), safe_float(r[10]), safe_float(r[11]),
            safe_float(r[12]), safe_float(r[13]), safe_float(r[14])
        ))
    bulk_insert(conn, "industry_utilization",
                ["year", "biz_name", "power_range",
                 "util_jan", "util_feb", "util_mar", "util_apr", "util_may", "util_jun",
                 "util_jul", "util_aug", "util_sep", "util_oct", "util_nov", "util_dec"], data)


def load_industry_code(conn):
    """C3. 산업 분류 현황 (EUC-KR)"""
    headers, rows = read_csv("15017232", "euc-kr")
    data = []
    for r in rows:
        if len(r) < 3:
            continue
        data.append((safe_int(r[0]), r[1].strip(), r[2].strip()))
    bulk_insert(conn, "industry_code", ["seq", "code", "name"], data)


def load_ppa_by_source(conn):
    """D1. 발전원별 PPA 계약현황 (EUC-KR)"""
    headers, rows = read_csv("15039559", "euc-kr")
    data = []
    for r in rows:
        if len(r) < 4:
            continue
        data.append((r[0].strip(), r[1].strip(), safe_int(r[2]), safe_float(r[3])))
    bulk_insert(conn, "ppa_by_source",
                ["year", "gen_source", "vendor_count", "capacity_kw"], data)


def load_ppa_by_region(conn):
    """D2. 지역별 PPA 계약현황 (UTF-8)"""
    headers, rows = read_csv("15039560")
    data = []
    for r in rows:
        if len(r) < 7:
            continue
        data.append((r[0].strip(), r[1].strip(), r[2].strip(),
                     safe_int(r[3]), safe_float(r[4]),
                     safe_int(r[5]), safe_float(r[6])))
    bulk_insert(conn, "ppa_by_region",
                ["gen_source", "region", "city", "count", "capacity_kw",
                 "region_count", "region_capacity"], data)


def load_net_metering_usage(conn):
    """D3. 법정동별 상계거래 전력사용량 (UTF-8)"""
    headers, rows = read_csv("15111712")
    data = []
    for r in rows:
        if len(r) < 7:
            continue
        data.append((r[0].strip(), r[1].strip(), r[2].strip(),
                     r[3].strip(), r[4].strip(),
                     safe_int(r[5]), safe_float(r[6])))
    bulk_insert(conn, "net_metering_usage",
                ["metro", "city", "dong", "year", "month", "total_count", "power_usage"], data)


def load_net_metering_surplus(conn):
    """D4. 법정동별 상계거래 잉여전력량 (UTF-8)"""
    headers, rows = read_csv("15111708")
    data = []
    for r in rows:
        if len(r) < 7:
            continue
        data.append((r[0].strip(), r[1].strip(), r[2].strip(),
                     r[3].strip(), r[4].strip(),
                     safe_int(r[5]), safe_float(r[6])))
    bulk_insert(conn, "net_metering_surplus",
                ["metro", "city", "dong", "year", "month", "total_count", "surplus_power"], data)


def load_tariff_adjustment(conn):
    """E1. 전기요금 조정율 (EUC-KR)"""
    headers, rows = read_csv("15066949", "euc-kr")
    data = []
    for r in rows:
        if len(r) < 9:
            continue
        data.append((
            r[0].strip(), r[1].strip(),
            safe_float(r[2]), safe_float(r[3]), safe_float(r[4]),
            safe_float(r[5]), safe_float(r[6]), safe_float(r[7]), safe_float(r[8])
        ))
    bulk_insert(conn, "tariff_adjustment",
                ["year", "month", "residential", "general", "education",
                 "industrial", "agriculture", "street_lamp", "total"], data)


# ── 인덱스 생성 ──────────────────────────────────────────────────────────────

def create_indexes(conn):
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_ev_station_loc_sid ON ev_station_location(station_id);
        CREATE INDEX IF NOT EXISTS idx_ev_charger_cap_sid ON ev_charger_capacity(station_id);
        CREATE INDEX IF NOT EXISTS idx_ev_charger_cap_cid ON ev_charger_capacity(charger_id);
        CREATE INDEX IF NOT EXISTS idx_sales_stat_period  ON sales_stat(period, metro);
        CREATE INDEX IF NOT EXISTS idx_hourly_date_hour   ON hourly_power(date, hour);
        CREATE INDEX IF NOT EXISTS idx_hourly_region      ON hourly_power(region, date);
        CREATE INDEX IF NOT EXISTS idx_dong_ind_loc       ON dong_industry_power(metro, city, dong);
        CREATE INDEX IF NOT EXISTS idx_dong_ind_biz       ON dong_industry_power(biz_code_large, biz_code_mid);
        CREATE INDEX IF NOT EXISTS idx_dong_ind_ym        ON dong_industry_power(year, month);
        CREATE INDEX IF NOT EXISTS idx_ind_util_year      ON industry_utilization(year, biz_name);
        CREATE INDEX IF NOT EXISTS idx_ppa_src_year       ON ppa_by_source(year, gen_source);
        CREATE INDEX IF NOT EXISTS idx_ppa_region_src     ON ppa_by_region(gen_source, region);
        CREATE INDEX IF NOT EXISTS idx_net_usage_loc      ON net_metering_usage(metro, city, dong);
        CREATE INDEX IF NOT EXISTS idx_net_usage_ym       ON net_metering_usage(year, month);
        CREATE INDEX IF NOT EXISTS idx_net_surp_loc       ON net_metering_surplus(metro, city, dong);
        CREATE INDEX IF NOT EXISTS idx_tariff_year        ON tariff_adjustment(year);
    """)
    conn.commit()
    print("인덱스 생성 완료")


# ── 메인 ────────────────────────────────────────────────────────────────────

LOADERS = [
    ("A1. 전기차충전소위경도",          load_ev_station_location),
    ("A2. 충전소별 충전기 용량",         load_ev_charger_capacity),
    ("B1. 영업통계정보",                load_sales_stat),
    ("B2. 전국 시간별 전력사용량",       load_hourly_power),
    ("C1. 산업분류별 법정동별 전력사용량", load_dong_industry_power),
    ("C2. 산업분류별 월별 수용률",        load_industry_utilization),
    ("C3. 산업 분류 코드",               load_industry_code),
    ("D1. 발전원별 PPA 계약현황",        load_ppa_by_source),
    ("D2. 지역별 PPA 계약현황",          load_ppa_by_region),
    ("D3. 법정동별 상계거래 전력사용량",  load_net_metering_usage),
    ("D4. 법정동별 상계거래 잉여전력량",  load_net_metering_surplus),
    ("E1. 전기요금 조정율",              load_tariff_adjustment),
]


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    print("=== 파일데이터셋 DB 적재 시작 ===\n")
    create_tables(conn)
    print()

    for label, loader in LOADERS:
        print(f"[{label}]")
        try:
            loader(conn)
        except Exception as e:
            print(f"  ✗ 오류: {e}")
        print()

    create_indexes(conn)

    # 최종 확인
    print("\n=== 적재 결과 ===")
    tables = [
        "ev_station_location", "ev_charger_capacity", "sales_stat", "hourly_power",
        "dong_industry_power", "industry_utilization", "industry_code",
        "ppa_by_source", "ppa_by_region", "net_metering_usage",
        "net_metering_surplus", "tariff_adjustment"
    ]
    for t in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {cnt:,}행")

    conn.close()
    print("\n완료")
