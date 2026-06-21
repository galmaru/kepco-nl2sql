"""
빈 테이블 수집 + agg 테이블 삭제
- house_avg, industry_cust_change: metroCd 필수 → metro별 요청 후 병합
- ev_charge: metroCd별 1회 수집
- agg_* 6개 테이블 삭제
"""

import os
import json
import time
import sqlite3
import requests
import glob
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("KEPCO_API_KEY")
BASE_URL = "https://bigdata.kepco.co.kr/openapi/v1"
RAW_DIR = Path("data/raw")
DB_PATH = Path("data/kepco.db")

YEARS = ["2021", "2022", "2023", "2024", "2025", "2026"]
MONTHS = [f"{m:02d}" for m in range(1, 13)]


def parse_multi_json(text):
    decoder = json.JSONDecoder()
    result = {}
    pos = 0
    text = text.strip()
    while pos < len(text):
        try:
            obj, pos = decoder.raw_decode(text, pos)
            for k, v in obj.items():
                if k in result and isinstance(result[k], list):
                    result[k].extend(v)
                else:
                    result[k] = v
            pos = pos + len(text[pos:]) - len(text[pos:].lstrip())
        except Exception:
            break
    return result


def fetch(url, params, retries=3, timeout=30):
    params["apiKey"] = API_KEY
    params["returnType"] = "json"
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=timeout)
            if res.status_code == 200:
                return parse_multi_json(res.text)
            if res.status_code == 404:
                return None
            print(f"  [HTTP {res.status_code}]")
            return None
        except Exception as e:
            print(f"  [오류] {e} (시도 {attempt+1}/{retries})")
            time.sleep(3)
    return None


def save(name, key, data):
    path = RAW_DIR / name
    path.mkdir(parents=True, exist_ok=True)
    with open(path / f"{key}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_metro_codes():
    path = RAW_DIR / "common_code/metroCd.json"
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return [item["code"] for item in data["data"]]


# ─── 수집 함수 ────────────────────────────────────────────────────────────────

def collect_house_avg(metro_codes):
    """가구당 평균 전력사용량 (metroCd 필수, 월별)"""
    print("\n[1/3] 가구당 평균 전력사용량 (house_avg)")
    total = 0
    for year in YEARS:
        for month in MONTHS:
            if year == "2026" and int(month) > 3:
                continue
            merged = {"data": []}
            for metro in metro_codes:
                result = fetch(
                    f"{BASE_URL}/powerUsage/houseAve.do",
                    {"year": year, "month": month, "metroCd": metro}
                )
                if result:
                    merged["data"].extend(result.get("data", []))
                time.sleep(0.15)
            save("house_avg", f"{year}_{month}", merged)
            cnt = len(merged["data"])
            total += cnt
            if cnt > 0:
                print(f"  {year}-{month}: {cnt}건")
    print(f"  → 총 {total}건 저장")


def collect_industry_cust_change(metro_codes):
    """산업별 고객 증감 (metroCd 필수, 월별)"""
    print("\n[2/3] 산업별 고객 증감 (industry_cust_change)")
    total = 0
    for year in YEARS:
        for month in MONTHS:
            if year == "2026" and int(month) > 3:
                continue
            merged = {"data": []}
            for metro in metro_codes:
                result = fetch(
                    f"{BASE_URL}/change/custNum/industryType.do",
                    {"year": year, "month": month, "metroCd": metro}
                )
                if result:
                    merged["data"].extend(result.get("data", []))
                time.sleep(0.15)
            save("industry_cust_change", f"{year}_{month}", merged)
            cnt = len(merged["data"])
            total += cnt
            if cnt > 0:
                print(f"  {year}-{month}: {cnt}건")
    print(f"  → 총 {total}건 저장")


def collect_ev_charge(metro_codes):
    """전기차 충전소 설치현황 (metroCd 필수, 1회)"""
    print("\n[3/3] 전기차 충전소 설치현황 (ev_charge)")
    merged = {"data": []}
    for metro in metro_codes:
        result = fetch(f"{BASE_URL}/EVcharge.do", {"metroCd": metro})
        if result:
            cnt = len(result.get("data", []))
            merged["data"].extend(result.get("data", []))
            print(f"  metroCd={metro}: {cnt}건")
        time.sleep(0.3)
    save("ev_charge", "all", merged)
    print(f"  → 총 {len(merged['data'])}건 저장")


# ─── DB 적재 함수 ─────────────────────────────────────────────────────────────

def load_house_avg(conn):
    files = sorted(glob.glob(str(RAW_DIR / "house_avg" / "*.json")))
    rows = []
    for path in files:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for item in data.get("data", []):
            rows.append((
                item["year"], item["month"],
                item["metro"], item["city"],
                item.get("houseCnt"),
                item.get("powerUsage"),
                item.get("bill"),
            ))
    conn.execute("DELETE FROM house_avg")
    conn.executemany(
        "INSERT INTO house_avg (year, month, metro, city, house_count, power_usage, bill) VALUES (?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_industry_cust_change(conn):
    files = sorted(glob.glob(str(RAW_DIR / "industry_cust_change" / "*.json")))
    rows = []
    for path in files:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for item in data.get("data", []):
            rows.append((
                item["year"], item["month"],
                item["metro"], item["city"],
                item["biz"],
                item.get("new"),
                item.get("expansion"),
                item.get("cancel"),
            ))
    conn.execute("DELETE FROM industry_cust_change")
    conn.executemany(
        "INSERT INTO industry_cust_change (year, month, metro, city, biz, new_count, expansion_count, cancel_count) VALUES (?,?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def load_ev_charge(conn):
    files = sorted(glob.glob(str(RAW_DIR / "ev_charge" / "*.json")))
    rows = []
    for path in files:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for item in data.get("data", []):
            rows.append((
                item.get("metro"), item.get("city"),
                item.get("stnPlace"), item.get("stnAddr"),
                item.get("rapidCnt"), item.get("slowCnt"),
                item.get("carType"),
            ))
    conn.execute("DELETE FROM ev_charge")
    conn.executemany(
        "INSERT INTO ev_charge (metro, city, station_place, station_addr, rapid_count, slow_count, car_type) VALUES (?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    return len(rows)


def drop_agg_tables(conn):
    """agg_* 테이블 전체 삭제"""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'agg_%'")
    agg_tables = [r[0] for r in cur.fetchall()]
    for t in agg_tables:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
        print(f"  삭제: {t}")
    conn.commit()
    return len(agg_tables)


# ─── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("빈 테이블 수집 + agg 테이블 삭제")
    print("=" * 60)

    metro_codes = load_metro_codes()
    print(f"시도코드 {len(metro_codes)}개 로드: {metro_codes}")

    # 1. 수집
    collect_house_avg(metro_codes)
    collect_industry_cust_change(metro_codes)
    collect_ev_charge(metro_codes)

    # 2. DB 적재 + agg 삭제
    print("\n" + "=" * 60)
    print("DB 적재 및 agg 테이블 삭제")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    cnt = load_house_avg(conn)
    print(f"  ✓ house_avg: {cnt:,}행 적재")

    cnt = load_industry_cust_change(conn)
    print(f"  ✓ industry_cust_change: {cnt:,}행 적재")

    cnt = load_ev_charge(conn)
    print(f"  ✓ ev_charge: {cnt:,}행 적재")

    print("\nagg 테이블 삭제:")
    n = drop_agg_tables(conn)
    print(f"  → {n}개 테이블 삭제 완료")

    # 3. 최종 현황
    print("\n최종 테이블 현황:")
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:<30} {cnt:>10,}행")

    conn.close()
    print("\n완료!")


if __name__ == "__main__":
    main()
