"""실패한 4개 API 재수집"""
import os, json, time, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("KEPCO_API_KEY")
BASE_URL = "https://bigdata.kepco.co.kr/openapi/v1"
DATA_DIR = Path("data/raw")

YEARS = ["2022", "2023"]
MONTHS = [f"{m:02d}" for m in range(1, 13)]

# 공통코드에서 시도코드 로드
with open(DATA_DIR / "common_code/metroCd.json") as f:
    metro_codes = [item["code"] for item in json.load(f)["data"]]
print("시도코드:", metro_codes)


def parse_multi_json(text):
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


def fetch(url, params, timeout=30):
    params["apiKey"] = API_KEY
    params["returnType"] = "json"
    for attempt in range(3):
        try:
            res = requests.get(url, params=params, timeout=timeout)
            if res.status_code == 200:
                return parse_multi_json(res.text)
            print(f"  [HTTP {res.status_code}]")
            return None
        except Exception as e:
            print(f"  [오류] {e} (시도 {attempt+1}/3)")
            time.sleep(3)
    return None


def save(name, key, data):
    path = DATA_DIR / name
    path.mkdir(parents=True, exist_ok=True)
    with open(path / f"{key}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# 1. 가구평균 전력사용량 (metroCd 필수)
print("\n[1/4] 가구평균 전력사용량")
total = 0
for year in YEARS:
    for month in MONTHS:
        merged = {"data": []}
        for metro in metro_codes:
            result = fetch(f"{BASE_URL}/powerUsage/houseAve.do",
                          {"year": year, "month": month, "metroCd": metro})
            if result:
                merged["data"].extend(result.get("data", []))
            time.sleep(0.2)
        save("house_avg", f"{year}_{month}", merged)
        total += len(merged["data"])
        print(f"  {year}-{month}: {len(merged['data'])}건")
print(f"  → 총 {total}건 저장\n")

# 2. 산업분류별 전기사용고객 증감 (metroCd 필수)
print("[2/4] 산업분류별 전기사용고객 증감")
total = 0
for year in YEARS:
    for month in MONTHS:
        merged = {"data": []}
        for metro in metro_codes:
            result = fetch(f"{BASE_URL}/change/custNum/industryType.do",
                          {"year": year, "month": month, "metroCd": metro})
            if result:
                merged["data"].extend(result.get("data", []))
            time.sleep(0.2)
        save("industry_cust_change", f"{year}_{month}", merged)
        total += len(merged["data"])
        print(f"  {year}-{month}: {len(merged['data'])}건")
print(f"  → 총 {total}건 저장\n")

# 3. 전기차 충전소 설치현황 (metroCd 필수)
print("[3/4] 전기차 충전소 설치현황")
merged = {"data": []}
for metro in metro_codes:
    result = fetch(f"{BASE_URL}/EVcharge.do", {"metroCd": metro})
    if result:
        merged["data"].extend(result.get("data", []))
    time.sleep(0.3)
save("ev_charge", "all", merged)
print(f"  → 총 {len(merged['data'])}건 저장\n")

# 4. 신재생 에너지 현황 (타임아웃 30초)
print("[4/4] 신재생 에너지 현황")
total = 0
for year in YEARS:
    result = fetch(f"{BASE_URL}/renewEnergy.do", {"year": year}, timeout=30)
    if result:
        save("renew_energy", year, result)
        total += len(result.get("data", []))
        print(f"  {year}: {len(result.get('data', []))}건")
    time.sleep(0.5)
print(f"  → 총 {total}건 저장\n")

print("재수집 완료!")
