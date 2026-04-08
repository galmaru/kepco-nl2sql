"""
한전 OPEN API 데이터 수집 스크립트
수집 기간: 2022년 1월 ~ 2023년 12월 (월별 데이터)
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

YEARS = ["2022", "2023"]
MONTHS = [f"{m:02d}" for m in range(1, 13)]


def parse_multi_json(text):
    """API가 JSON 여러 개를 붙여서 반환할 경우 모두 파싱 후 병합"""
    decoder = json.JSONDecoder()
    result = {}
    pos = 0
    text = text.strip()
    while pos < len(text):
        obj, pos = decoder.raw_decode(text, pos)
        for k, v in obj.items():
            if k in result and isinstance(result[k], list):
                result[k].extend(v)
            else:
                result[k] = v
        pos = pos + len(text[pos:]) - len(text[pos:].lstrip())
    return result


def fetch(url, params, retries=3):
    params["apiKey"] = API_KEY
    params["returnType"] = "json"
    for attempt in range(retries):
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 200:
                return parse_multi_json(res.text)
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
            params = {"year": year, "month": month}
            if extra_params:
                params.update(extra_params)
            result = fetch(url, params)
            if result:
                save(name, f"{year}_{month}", result)
                count = len(result.get("data", result.get("totData", [])))
                total += count
                print(f"  {year}-{month}: {count}건")
            time.sleep(0.3)
    print(f"  → 총 {total}건 저장\n")


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
            print(f"  {year}: {count}건")
        time.sleep(0.3)
    print(f"  → 총 {total}건 저장\n")


def collect_once(name, endpoint, params=None, key="all"):
    """파라미터 없이 전체 조회하는 API"""
    url = f"{BASE_URL}/{endpoint}"
    result = fetch(url, params or {})
    if result:
        save(name, key, result)
        count = len(result.get("data", []))
        print(f"  {key}: {count}건")
        print(f"  → 총 {count}건 저장\n")
    else:
        print(f"  → 데이터 없음\n")


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
    print(f"  → 총 {total}건 저장\n")


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
    print(f"  → 총 {total}건 저장\n")


def collect_contract():
    """전자입찰계약 - 연도별 수집"""
    url = f"{BASE_URL}/electContract.do"
    periods = [
        ("20220101", "20220331"), ("20220401", "20220630"),
        ("20220701", "20220930"), ("20221001", "20221231"),
        ("20230101", "20230331"), ("20230401", "20230630"),
        ("20230701", "20230930"), ("20231001", "20231231"),
    ]
    total = 0
    for start, end in periods:
        result = fetch(url, {"noticeBeginDate": start, "noticeEndDate": end})
        if result:
            key = f"{start}_{end}"
            save("contract", key, result)
            count = len(result.get("data", []))
            total += count
            print(f"  {start}~{end}: {count}건")
        time.sleep(0.3)
    print(f"  → 총 {total}건 저장\n")


if __name__ == "__main__":
    print("=" * 50)
    print("한전 OPEN API 데이터 수집 시작")
    print("=" * 50)

    print("\n[1/12] 계약종별 전력사용량")
    collect_monthly("contract_type", "powerUsage/contractType.do")

    print("[2/12] 산업분류별 전력사용량")
    collect_monthly("industry_type", "powerUsage/industryType.do")

    print("[3/12] 업종별 전력사용량")
    collect_monthly("business_type", "powerUsage/businessType.do")

    print("[4/12] 가구평균 전력사용량")
    collect_monthly("house_avg", "powerUsage/houseAve.do")

    print("[5/12] 산업분류별 전기사용고객 증감")
    collect_monthly("industry_cust_change", "change/custNum/industryType.do")

    print("[6/12] 요금청구방식 변동추이")
    collect_monthly("billing_type", "billingType.do")

    print("[7/12] 복지할인대상")
    collect_monthly("welfare_discount", "welfareDiscount.do")

    print("[8/12] 신재생 에너지 현황")
    collect_yearly("renew_energy", "renewEnergy.do")

    print("[9/12] 전기차 충전소 설치현황")
    collect_once("ev_charge", "EVcharge.do")

    print("[10/12] 전기차충전소 운영정보")
    collect_ev_charge_manage()

    print("[11/12] 분산전원연계 정보")
    collect_once("dispersed_gen", "dispersedGeneration.do")

    print("[12/12] 전자입찰계약 정보")
    collect_contract()

    print("[13/13] 공통코드 정보")
    collect_common_codes()

    print("=" * 50)
    print("수집 완료!")
    print("=" * 50)
