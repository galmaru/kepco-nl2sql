"""
한전 OPEN API 데이터 수집 스크립트 (확장 버전)
수집 기간: 2021년 1월 ~ 2026년 3월
저장 위치: data/raw/{api_name}/
"""

import os
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("KEPCO_API_KEY")
BASE_URL = "https://bigdata.kepco.co.kr/openapi/v1"
DATA_DIR = Path("data/raw")

# 수집 대상 연도 (2021 ~ 2026)
YEARS = ["2021", "2022", "2023", "2024", "2025", "2026"]
MONTHS = [f"{m:02d}" for m in range(1, 13)]


def parse_multi_json(text):
    """API가 JSON 여러 개를 붙여서 반환할 경우 모두 파싱 후 병합"""
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
        except:
            break
    return result


def fetch(url, params, retries=3):
    params["apiKey"] = API_KEY
    params["returnType"] = "json"
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 200:
                return parse_multi_json(res.text)
            if res.status_code == 404:
                return None
            print(f"  [HTTP {res.status_code}] {url} params={params}")
            return None
        except Exception as e:
            print(f"  [오류] {e} (시도 {attempt+1}/{retries})")
            time.sleep(2)
    return None


def save(name, key, data):
    path = DATA_DIR / name
    path.mkdir(parents=True, exist_ok=True)
    filepath = path / f"{key}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def collect_monthly(name, endpoint, extra_params=None):
    """year + month 필수 API 수집"""
    url = f"{BASE_URL}/{endpoint}"
    total = 0
    for year in YEARS:
        for month in MONTHS:
            # 2026년은 현재(4월) 전인 3월까지만 시도
            if year == "2026" and int(month) > 3:
                continue
            
            params = {"year": year, "month": month}
            if extra_params:
                params.update(extra_params)
            result = fetch(url, params)
            if result:
                save(name, f"{year}_{month}", result)
                data_list = result.get("data", result.get("totData", []))
                count = len(data_list)
                total += count
                if count > 0:
                    print(f"  {year}-{month}: {count}건")
            time.sleep(0.2)
    print(f"  → {name}: 총 {total}건 저장\n")


def collect_yearly(name, endpoint, extra_params=None):
    """year만 필수 API 수집"""
    url = f"{BASE_URL}/{endpoint}"
    total = 0
    for year in YEARS:
        params = {"year": year}
        if extra_params:
            params.update(extra_params)
        result = fetch(url, params)
        if result:
            save(name, year, result)
            count = len(result.get("data", []))
            total += count
            if count > 0:
                print(f"  {year}: {count}건")
        time.sleep(0.3)
    print(f"  → {name}: 총 {total}건 저장\n")


def collect_once(name, endpoint, params=None, key="all"):
    """파라미터 없이 전체 조회하는 API"""
    url = f"{BASE_URL}/{endpoint}"
    result = fetch(url, params or {})
    if result:
        save(name, key, result)
        count = len(result.get("data", []))
        print(f"  {key}: {count}건")
        print(f"  → {name}: 총 {count}건 저장\n")


def collect_common_codes():
    """공통코드 전체 수집"""
    url = f"{BASE_URL}/commonCode.do"
    code_types = ["metroCd", "cityCd", "lglDngMetroCd", "lglDngCityCd",
                  "cntrCd", "bizCd", "genSrcCd", "wfTypeCd"]
    total = 0
    for ct in code_types:
        result = fetch(url, {"codeTy": ct})
        if result:
            save("common_code", ct, result)
            count = len(result.get("data", []))
            total += count
            print(f"  {ct}: {count}건")
        time.sleep(0.3)
    print(f"  → 공통코드: 총 {total}건 저장\n")


def collect_ev_charge_manage():
    """전기차 충전소 운영정보 - 주요 지역별 수집"""
    url = f"{BASE_URL}/EVchargeManage.do"
    regions = [
        "서울특별시", "부산광역시", "대구광역시", "인천광역시",
        "광주광역시", "대전광역시", "울산광역시", "경기도",
        "강원도", "충청북도", "충청남도", "전라북도",
        "전라남도", "경상북도", "경상남도", "제주특별자치도"
    ]
    total = 0
    for region in regions:
        result = fetch(url, {"addr": region})
        if result:
            save("ev_charge_manage", region, result)
            count = len(result.get("data", []))
            total += count
            print(f"  {region}: {count}건")
        time.sleep(0.3)
    print(f"  → 전기차 운영정보: 총 {total}건 저장\n")


def collect_contract():
    """전자입찰계약 - 연도별/분기별 수집 (2021~2026)"""
    url = f"{BASE_URL}/electContract.do"
    periods = []
    for year in YEARS:
        if year == "2026":
            periods.append(("20260101", "20260331"))
        else:
            periods.extend([
                (f"{year}0101", f"{year}0331"), (f"{year}0401", f"{year}0630"),
                (f"{year}0701", f"{year}0930"), (f"{year}1001", f"{year}1231"),
            ])
    
    total = 0
    for start, end in periods:
        result = fetch(url, {"noticeBeginDate": start, "noticeEndDate": end})
        if result:
            key = f"{start}_{end}"
            save("contract", key, result)
            count = len(result.get("data", []))
            total += count
            if count > 0:
                print(f"  {start}~{end}: {count}건")
        time.sleep(0.3)
    print(f"  → 전자입찰계약: 총 {total}건 저장\n")


if __name__ == "__main__":
    print("=" * 60)
    print("한전 OPEN API 데이터 확장 수집 시작 (2021~2026)")
    print("=" * 60)

    # 1. 월별 수집 API들
    apis_monthly = [
        ("contract_type", "powerUsage/contractType.do"),
        ("industry_type", "powerUsage/industryType.do"),
        ("business_type", "powerUsage/businessType.do"),
        ("house_avg", "powerUsage/houseAve.do"),
        ("industry_cust_change", "change/custNum/industryType.do"),
        ("billing_type", "billingType.do"),
        ("welfare_discount", "welfareDiscount.do"),
    ]
    
    for i, (name, endpoint) in enumerate(apis_monthly, 1):
        print(f"[{i}/13] {name}")
        collect_monthly(name, endpoint)

    # 2. 연도별 수집 API
    print("[8/13] 신재생 에너지 현황 (renew_energy)")
    collect_yearly("renew_energy", "renewEnergy.do")

    # 3. 1회성/고정 파라미터 수집 API
    print("[9/13] 전기차 충전소 설치현황 (ev_charge)")
    collect_once("ev_charge", "EVcharge.do")

    print("[10/13] 분산전원연계 정보 (dispersed_gen)")
    collect_once("dispersed_gen", "dispersedGeneration.do")

    # 4. 특수 파라미터 수집 API
    print("[11/13] 전기차충전소 운영정보")
    collect_ev_charge_manage()

    print("[12/13] 전자입찰계약 정보")
    collect_contract()

    print("[13/13] 공통코드 정보")
    collect_common_codes()

    print("=" * 60)
    print("전체 데이터 재수집 완료!")
    print("=" * 60)
