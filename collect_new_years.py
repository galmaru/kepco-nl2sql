"""
2024~2026년 신규 데이터 수집 스크립트
- 2024년: 1~12월 전체
- 2025년: 1~12월 전체
- 2026년: 1월만
"""
import os, json, time, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("KEPCO_API_KEY")
BASE_URL = "https://bigdata.kepco.co.kr/openapi/v1"
DATA_DIR = Path("data/raw")

YEAR_MONTHS = (
    [(y, f"{m:02d}") for y in ["2024", "2025"] for m in range(1, 13)]
    + [("2026", "01")]
)

# 시도코드 로드
with open(DATA_DIR / "common_code/metroCd.json") as f:
    metro_codes = [item["code"] for item in json.load(f)["data"]]


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
            if res.status_code == 404:
                return None  # 데이터 없음 (조용히 skip)
            print(f"  [HTTP {res.status_code}] {params}")
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


def collect_monthly(name, endpoint):
    """year+month 필수 API (직접 전국 조회)"""
    url = f"{BASE_URL}/{endpoint}"
    total = 0
    for year, month in YEAR_MONTHS:
        result = fetch(url, {"year": year, "month": month})
        if result:
            count = len(result.get("data", result.get("totData", [])))
            save(name, f"{year}_{month}", result)
            total += count
            print(f"  {year}-{month}: {count}건")
        time.sleep(0.3)
    print(f"  → 총 {total}건 저장\n")


def collect_monthly_by_metro(name, endpoint):
    """metroCd 필수 API (시도별 병합)"""
    url = f"{BASE_URL}/{endpoint}"
    total = 0
    for year, month in YEAR_MONTHS:
        merged = {"data": []}
        for metro in metro_codes:
            result = fetch(url, {"year": year, "month": month, "metroCd": metro})
            if result:
                merged["data"].extend(result.get("data", []))
            time.sleep(0.15)
        save(name, f"{year}_{month}", merged)
        total += len(merged["data"])
        print(f"  {year}-{month}: {len(merged['data'])}건")
    print(f"  → 총 {total}건 저장\n")


def collect_yearly(name, endpoint):
    """year만 필수 API"""
    url = f"{BASE_URL}/{endpoint}"
    total = 0
    years_done = set()
    for year, _ in YEAR_MONTHS:
        if year in years_done:
            continue
        years_done.add(year)
        result = fetch(url, {"year": year}, timeout=30)
        if result:
            count = len(result.get("data", []))
            save(name, year, result)
            total += count
            print(f"  {year}: {count}건")
        time.sleep(0.5)
    print(f"  → 총 {total}건 저장\n")


def collect_once_by_metro(name, endpoint):
    """시도별 전체 조회 (날짜 무관)"""
    url = f"{BASE_URL}/{endpoint}"
    merged = {"data": []}
    for metro in metro_codes:
        result = fetch(url, {"metroCd": metro})
        if result:
            merged["data"].extend(result.get("data", []))
        time.sleep(0.3)
    save(name, "all", merged)
    print(f"  → 총 {len(merged['data'])}건 저장\n")


def collect_contract():
    """전자입찰계약 - 월별 (API 90일 제한으로 Q3·Q4 분기 조회 불가)"""
    url = f"{BASE_URL}/electContract.do"
    # 월별로 수집: 31일짜리 달도 90일 이하 → 전 구간 안전
    import calendar
    periods = []
    for year in ["2024", "2025"]:
        for m in range(1, 13):
            last_day = calendar.monthrange(int(year), m)[1]
            start = f"{year}{m:02d}01"
            end   = f"{year}{m:02d}{last_day:02d}"
            periods.append((start, end))
    # 2026년 1월
    periods.append(("20260101", "20260131"))

    total = 0
    for start, end in periods:
        result = fetch(url, {"noticeBeginDate": start, "noticeEndDate": end})
        if result:
            count = len(result.get("data", []))
            save("contract", f"{start}_{end}", result)
            total += count
            print(f"  {start}~{end}: {count}건")
        time.sleep(0.3)
    print(f"  → 총 {total}건 저장\n")


def collect_ev_charge_manage():
    """EV 충전소 운영정보 - 광역별"""
    url = f"{BASE_URL}/EVchargeManage.do"
    regions = [
        "서울특별시", "부산광역시", "대구광역시", "인천광역시",
        "광주광역시", "대전광역시", "울산광역시", "경기도",
        "강원도", "충청북도", "충청남도", "전라북도",
        "전라남도", "경상북도", "경상남도", "제주특별자치도",
        "세종특별자치시",
    ]
    merged = {"data": []}
    for region in regions:
        result = fetch(url, {"addr": region})
        if result:
            merged["data"].extend(result.get("data", []))
            print(f"  {region}: {len(result.get('data', []))}건")
        time.sleep(0.3)
    # 날짜 무관한 스냅샷이므로 latest로 저장
    save("ev_charge_manage", "latest", merged)
    print(f"  → 총 {len(merged['data'])}건 저장\n")


if __name__ == "__main__":
    print("=" * 55)
    print("한전 OPEN API 신규 데이터 수집 (2024~2026)")
    print("=" * 55)

    print("\n[1/9] 계약종별 전력사용량")
    collect_monthly("contract_type", "powerUsage/contractType.do")

    print("[2/9] 산업분류별 전력사용량")
    collect_monthly("industry_type", "powerUsage/industryType.do")

    print("[3/9] 업종별 전력사용량")
    collect_monthly("business_type", "powerUsage/businessType.do")

    print("[4/9] 가구평균 전력사용량 (시도별)")
    collect_monthly_by_metro("house_avg", "powerUsage/houseAve.do")

    print("[5/9] 산업분류별 전기사용고객 증감 (시도별)")
    collect_monthly_by_metro("industry_cust_change", "change/custNum/industryType.do")

    print("[6/9] 요금청구방식 변동추이")
    collect_monthly("billing_type", "billingType.do")

    print("[7/9] 복지할인대상")
    collect_monthly("welfare_discount", "welfareDiscount.do")

    print("[8/9] 신재생 에너지 현황")
    collect_yearly("renew_energy", "renewEnergy.do")

    print("[9/9] 전자입찰계약 정보")
    collect_contract()

    print("\n[추가] 전기차 충전소 설치현황 (시도별)")
    collect_once_by_metro("ev_charge", "EVcharge.do")

    print("[추가] 전기차 충전소 운영정보 (최신 스냅샷)")
    collect_ev_charge_manage()

    print("=" * 55)
    print("수집 완료!")
    print("=" * 55)
