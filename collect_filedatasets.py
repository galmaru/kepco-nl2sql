"""
data.go.kr 한국전력공사 파일데이터 목록 수집 스크립트
총 305건의 파일데이터 메타정보를 수집하고 각 데이터셋의 파일을 다운로드합니다.
저장 위치: data/filedatasets/
"""

import os
import json
import time
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin

BASE_URL = "https://www.data.go.kr"
LIST_URL = f"{BASE_URL}/tcs/dss/selectDataSetList.do"
DATA_DIR = Path("data/filedatasets")
META_FILE = DATA_DIR / "datasets_meta.json"

LIST_PARAMS = {
    "dType": "FILE",
    "sort": "updtDt",
    "orgFullName": "한국전력공사",
    "orgFilter": "한국전력공사",
    "org": "한국전력공사",
    "perPage": "50",
    "operator": "AND",
    "pblonsipScopeCode": "PBDE07",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": BASE_URL,
}

session = requests.Session()
session.headers.update(HEADERS)


def parse_info_data(item) -> dict:
    """info-data 영역에서 수정일, 조회수, 다운로드 수, 키워드 추출"""
    result = {"updated": "", "view_count": "", "download_count": "", "keywords": ""}
    info_ps = item.select(".info-data > p")
    for p in info_ps:
        tit = p.select_one(".tit")
        data = p.select_one(".data")
        if not tit:
            continue
        key = tit.get_text(strip=True)
        val = data.get_text(strip=True) if data else p.get_text(strip=True).replace(key, "").strip()
        if key == "수정일":
            result["updated"] = val
        elif key == "조회수":
            result["view_count"] = val
        elif key == "다운로드":
            result["download_count"] = val
        elif key == "키워드":
            result["keywords"] = val
    return result


def fetch_page(page_num: int) -> list[dict]:
    """한 페이지의 데이터셋 목록을 파싱해서 반환"""
    params = {**LIST_PARAMS, "currentPage": str(page_num)}
    try:
        res = session.get(LIST_URL, params=params, timeout=15)
        res.raise_for_status()
    except Exception as e:
        print(f"  [오류] 페이지 {page_num} 요청 실패: {e}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")

    # 총 건수 파싱 (첫 페이지에서만)
    if page_num == 1:
        count_el = soup.select_one(".result-total-count strong, .cnt strong")
        if count_el:
            print(f"  총 건수: {count_el.text.strip()}건")

    datasets = []
    items = soup.select(".result-list li")

    for item in items:
        # 제목 및 URL
        title_el = item.select_one("dl dt a .title")
        link_el = item.select_one("dl dt a")
        if not title_el or not link_el:
            continue

        title = title_el.get_text(strip=True)
        href = link_el.get("href", "")
        full_url = urljoin(BASE_URL, href) if href.startswith("/") else href

        # publicDataPk 추출
        pk = href.split("/data/")[1].split("/")[0] if "/data/" in href else ""

        # 파일 형식 (CSV, JSON, XML 등)
        formats = [t.get_text(strip=True) for t in item.select("dl dt a .tagset")]

        # 설명
        desc_el = item.select_one("dd.publicDataDesc")
        desc = desc_el.get_text(separator=" ", strip=True) if desc_el else ""

        # 카테고리 레이블
        labels = [lb.get_text(strip=True) for lb in item.select(".tag-area .labelset")]

        # 수정일, 조회수, 다운로드, 키워드
        info = parse_info_data(item)

        datasets.append({
            "title": title,
            "publicDataPk": pk,
            "url": full_url,
            "formats": formats,
            "categories": labels,
            "description": desc,
            **info,
        })

    return datasets


def collect_all_datasets() -> list[dict]:
    """전체 페이지를 순회하며 모든 데이터셋 목록 수집"""
    print("=== 한국전력공사 파일데이터 목록 수집 시작 ===")
    all_datasets = []
    page = 1

    while True:
        print(f"[페이지 {page}] 수집 중...")
        items = fetch_page(page)

        if not items:
            print(f"  페이지 {page}에서 데이터 없음 → 수집 완료")
            break

        all_datasets.extend(items)
        print(f"  {len(items)}건 수집 (누계: {len(all_datasets)}건)")

        # 페이지당 50건 기준으로 305건이면 7페이지
        if len(items) < 50:
            print("  마지막 페이지 도달")
            break

        page += 1
        time.sleep(1)  # 서버 부하 방지

    return all_datasets


def save_meta(datasets: list[dict]):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(META_FILE, "w", encoding="utf-8") as f:
        json.dump(datasets, f, ensure_ascii=False, indent=2)
    print(f"\n메타데이터 저장 완료: {META_FILE} ({len(datasets)}건)")


def get_download_info(pk: str):
    """데이터셋 상세 페이지에서 atchFileId, fileDetailSn, 파일명 추출"""
    import re
    url = f"{BASE_URL}/data/{pk}/fileData.do"
    try:
        res = session.get(url, timeout=15)
        res.raise_for_status()
    except Exception as e:
        print(f"  [오류] 상세 페이지 요청 실패 (pk={pk}): {e}")
        return None

    # atchFileId 추출 (예: FILE_000000003585936)
    atch_match = re.search(r"(FILE_\d+)", res.text)
    if not atch_match:
        # 리다이렉트 케이스: 버튼의 실제 pk로 재시도
        # fn_fileDataDown('실제pk', 'uddi:...') 형태에서 실제 pk 추출
        redirect_match = re.search(
            r"fn_fileDataDown\('(\d+)',\s*'(uddi:[^']+)'", res.text
        )
        if redirect_match and redirect_match.group(1) != pk:
            real_pk = redirect_match.group(1)
            return get_download_info(real_pk)
        return None

    # fileDetailSn 추출
    sn_match = re.search(r"fileDetailSn[\"':\s=]+(\d+)", res.text)
    file_detail_sn = sn_match.group(1) if sn_match else "1"

    # 실제 파일명 (Content-Disposition에서 나중에 추출)
    soup = BeautifulSoup(res.text, "html.parser")
    title_el = soup.select_one("h1, h2, .data-title, .publicDataSj")
    title = title_el.get_text(strip=True) if title_el else pk

    return {
        "atchFileId": atch_match.group(1),
        "fileDetailSn": file_detail_sn,
        "title": title,
    }


def download_file(pk: str, atch_file_id: str, file_detail_sn: str,
                  title: str, out_dir: Path) -> bool:
    """파일 다운로드 후 저장"""
    import urllib.parse
    download_url = f"{BASE_URL}/cmm/cmm/fileDownload.do"
    params = {
        "atchFileId": atch_file_id,
        "fileDetailSn": file_detail_sn,
        "dataNm": title,
    }
    headers = {**session.headers, "Referer": f"{BASE_URL}/data/{pk}/fileData.do"}
    try:
        res = requests.get(download_url, params=params, headers=headers,
                           timeout=60, stream=True)
        res.raise_for_status()
    except Exception as e:
        print(f"  [오류] 다운로드 실패 (pk={pk}): {e}")
        return False

    content_type = res.headers.get("Content-Type", "")
    if "text/html" in content_type:
        print(f"  [오류] HTML 응답 - 로그인 필요 가능성 (pk={pk})")
        return False

    # 파일 확장자 추출 (Content-Disposition 또는 Content-Type으로)
    disposition = res.headers.get("Content-Disposition", "")
    ext = ".dat"
    if "." in disposition.split("filename=")[-1]:
        raw_name = disposition.split("filename=")[-1].strip('"')
        try:
            ext = "." + raw_name.rsplit(".", 1)[-1].lower()
        except Exception:
            pass
    elif "csv" in content_type:
        ext = ".csv"
    elif "excel" in content_type or "spreadsheet" in content_type:
        ext = ".xlsx"

    # 안전한 파일명 생성
    safe_title = "".join(c if c.isalnum() or c in "._- " else "_" for c in title)[:100]
    filename = out_dir / f"{pk}_{safe_title}{ext}"

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(filename, "wb") as f:
        for chunk in res.iter_content(chunk_size=8192):
            f.write(chunk)

    size_kb = filename.stat().st_size / 1024
    print(f"  저장: {filename.name} ({size_kb:.0f} KB)")
    return True


def download_all_files(datasets: list[dict], out_dir: Path = DATA_DIR / "files",
                       skip_existing: bool = True):
    """305건 전체 파일 다운로드"""
    print(f"\n=== 파일 다운로드 시작 ({len(datasets)}건) ===")
    out_dir.mkdir(parents=True, exist_ok=True)

    success, failed = 0, []

    for i, ds in enumerate(datasets, 1):
        pk = ds["publicDataPk"]
        title = ds["title"]
        print(f"[{i}/{len(datasets)}] {title[:50]} (pk={pk})")

        # 이미 다운로드된 파일 확인
        if skip_existing:
            existing = list(out_dir.glob(f"{pk}_*"))
            if existing:
                print(f"  → 건너뜀 (이미 존재: {existing[0].name})")
                success += 1
                continue

        info = get_download_info(pk)
        if not info:
            print(f"  [경고] 다운로드 정보 추출 실패 (pk={pk})")
            failed.append(pk)
            time.sleep(0.5)
            continue

        ok = download_file(pk, info["atchFileId"], info["fileDetailSn"],
                           title, out_dir)
        if ok:
            success += 1
        else:
            failed.append(pk)

        time.sleep(1)  # 서버 부하 방지

    print(f"\n완료: 성공 {success}건 / 실패 {len(failed)}건")
    if failed:
        print(f"실패 목록: {failed}")
    return failed


if __name__ == "__main__":
    import sys

    cmd = sys.argv[1] if len(sys.argv) > 1 else "meta"

    if cmd == "meta":
        # 메타데이터 수집
        datasets = collect_all_datasets()
        if datasets:
            save_meta(datasets)
            print(f"\n수집 완료: 총 {len(datasets)}건")
        else:
            print("\n⚠ 데이터를 수집하지 못했습니다.")

    elif cmd == "download":
        # 파일 다운로드 (메타데이터가 이미 있어야 함)
        if not META_FILE.exists():
            print("먼저 'python collect_filedatasets.py meta' 로 메타데이터를 수집하세요.")
            sys.exit(1)
        with open(META_FILE, encoding="utf-8") as f:
            datasets = json.load(f)

        # 특정 인덱스 범위만 다운로드 가능 (예: 0~9)
        start = int(sys.argv[2]) if len(sys.argv) > 2 else 0
        end = int(sys.argv[3]) if len(sys.argv) > 3 else len(datasets)
        target = datasets[start:end]
        print(f"대상: {start}~{end-1}번 ({len(target)}건)")
        download_all_files(target)

    elif cmd == "all":
        # 메타 수집 + 파일 다운로드 한번에
        datasets = collect_all_datasets()
        if datasets:
            save_meta(datasets)
            download_all_files(datasets)
        else:
            print("\n⚠ 데이터를 수집하지 못했습니다.")
