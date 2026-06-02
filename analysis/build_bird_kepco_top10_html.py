#!/usr/bin/env python3
"""Build an HTML report for the 10 BIRD DBs most similar to KEPCO NL2SQL data."""

from __future__ import annotations

import html
import json
import zipfile
from pathlib import Path


OUT_HTML = Path("analysis/output/bird_kepco_similarity_top10.html")

CHOICES = [
    ("train", "sales_in_weather", "전력 사용량/계량값 + 기상/지점 조인 패턴", "전력 사용량"),
    ("dev", "debit_card_specializing", "고객별 월별 사용량, 거래 금액, 단가 패턴", "고객/요금"),
    ("train", "regional_sales", "지역/고객/상품/주문/매출 집계 패턴", "지역/요금"),
    ("train", "superstore", "지역별 고객·상품·판매 데이터 마트 패턴", "지역/고객"),
    ("train", "retails", "대규모 고객·주문·청구/라인아이템 조인 패턴", "대규모 조인"),
    ("train", "bike_share_1", "설비/스테이션 상태 이력과 시간대 운영 패턴", "설비 상태"),
    ("train", "works_cycles", "실무 ERP형 복잡 조인, 주문·생산·재고·거래 이력", "복잡 실무 DB"),
    ("dev", "financial", "고객 계좌, 거래, 대출, 납부 금액 패턴", "거래/납부"),
    ("train", "car_retails", "고객, 주문, 결제, 상품, 사업소/직원 패턴", "고객/결제"),
    ("train", "synthea", "고객/환자 단위 이벤트, 관측값, 청구/처치 이력 패턴", "이벤트/청구"),
]

DB_DETAILS = {
    "train.sales_in_weather": {
        "summary": "매장-측정소 매핑을 통해 날짜별 판매량과 날씨를 함께 분석하는 DB입니다. KEPCO의 지역/사업소/계량 지점별 전력 사용량, 기온 영향, 기간별 사용량 추이 질문을 대체하기 가장 좋습니다.",
        "kepco_questions": [
            "월별/일별 전력 사용량 합계와 증감률",
            "지점 또는 지역별 사용량 순위",
            "기온/날씨와 사용량의 관계",
            "측정 지점과 관측소 매핑 기반 조인",
        ],
    },
    "dev.debit_card_specializing": {
        "summary": "고객, 주유소, 상품, 거래, 월별 소비량이 연결된 DB입니다. 고객번호 단위 월별 전력 사용량, 사용요금, 단가, 고객군별 사용 패턴을 흉내 내기에 좋습니다.",
        "kepco_questions": [
            "고객별 월별 사용량 조회",
            "고객군/계약군별 평균 사용량",
            "거래 금액과 사용량 기반 단가 계산",
            "지점별 또는 상품/요금 항목별 소비 비교",
        ],
    },
    "train.regional_sales": {
        "summary": "고객, 상품, 판매팀, 매장 위치, 지역, 주문이 정규화된 영업 데이터입니다. 지역별 요금, 고객군별 청구액, 사업소별 판매/수납 실적 같은 집계형 질문에 잘 맞습니다.",
        "kepco_questions": [
            "지역별 총액/평균액 집계",
            "고객별 사용·요금 순위",
            "사업소/영업팀/지역 코드 조인",
            "상품 또는 요금 항목별 매출 분석",
        ],
    },
    "train.superstore": {
        "summary": "지역별로 분리된 판매 테이블과 고객·상품 차원이 있는 데이터 마트형 DB입니다. 지역본부별 전력 판매, 고객 세그먼트별 사용량, 상품/계약종별 비교 질문을 만들기 쉽습니다.",
        "kepco_questions": [
            "권역별 실적 비교",
            "고객 세그먼트별 매출/사용량 분석",
            "상품군 또는 계약종별 집계",
            "여러 지역 테이블 통합 질의",
        ],
    },
    "train.retails": {
        "summary": "TPC-H 스타일의 대규모 주문, 주문상세, 고객, 공급자, 지역 데이터입니다. 대용량 고객·청구·상세내역 테이블을 조인하는 성능/정확도 평가에 유용합니다.",
        "kepco_questions": [
            "대용량 상세 사용내역 집계",
            "고객-지역-주문상세 다중 조인",
            "기간별 청구 또는 사용 금액 집계",
            "지역/국가 계층 기반 비교",
        ],
    },
    "train.bike_share_1": {
        "summary": "스테이션, 시간대별 상태, 이용 이력, 날씨가 있는 운영 데이터입니다. 송변전/배전 설비의 상태 이력, 가동 현황, 시간대별 이벤트 질의를 가장 잘 대체합니다.",
        "kepco_questions": [
            "설비별 시간대 상태 조회",
            "설비 가동/여유/부하 현황 집계",
            "설비 위치와 운영 이벤트 조인",
            "날씨 또는 시간대별 운영 패턴 분석",
        ],
    },
    "train.works_cycles": {
        "summary": "AdventureWorks 계열의 ERP형 DB로 주문, 생산, 작업지시, 재고, 거래, 고객, 주소, 조직 정보가 매우 복잡하게 연결되어 있습니다. 실제 사내 업무 DB 수준의 복잡한 조인 평가에 적합합니다.",
        "kepco_questions": [
            "작업지시/설비/자재/거래 이력 조인",
            "고객·주소·영업조직·주문 연결 질의",
            "복잡한 PK/FK 경로 탐색",
            "실무형 다중 CTE 및 집계",
        ],
    },
    "dev.financial": {
        "summary": "은행 고객, 계좌, 거래, 카드, 대출, 지구 정보가 있는 금융 DB입니다. 고객별 납부/수납, 거래 금액, 미납 또는 대출 상태 같은 요금·수납 도메인 질문과 유사합니다.",
        "kepco_questions": [
            "고객/계좌별 납부 금액 집계",
            "기간별 거래 내역 조회",
            "지역별 고객/거래 분포",
            "금액, 상태, 빈도 기반 필터링",
        ],
    },
    "train.car_retails": {
        "summary": "고객, 주문, 주문상세, 결제, 상품, 사무소, 직원이 연결된 영업관리 DB입니다. 고객 청구, 납부, 사업소, 담당자, 상품/요금 항목 관계를 평가하기 좋습니다.",
        "kepco_questions": [
            "고객별 주문/결제 내역",
            "사업소 또는 담당자별 실적",
            "상품/요금 항목별 금액 집계",
            "주문 상태와 결제일 기준 조회",
        ],
    },
    "train.synthea": {
        "summary": "환자, 방문, 관측값, 청구, 처방, 절차 이벤트가 있는 의료 시뮬레이션 DB입니다. 전력 도메인은 아니지만 고객 단위 이벤트 로그, 관측값, 청구 이력 구조가 계량/검침/요금 이력과 닮았습니다.",
        "kepco_questions": [
            "고객 단위 이벤트 타임라인",
            "관측값/계량값 코드별 조회",
            "청구 기간과 이벤트 조인",
            "상태·처치·이력 기반 필터링",
        ],
    },
}

TABLE_DETAILS = {
    "train.sales_in_weather": {
        "sales_in_weather": "날짜, 매장, 품목별 판매 수량 테이블입니다. KEPCO의 일별/월별 전력 사용량 fact 테이블처럼 사용할 수 있습니다.",
        "weather": "측정소와 날짜별 기상 관측 테이블입니다. 기온, 강수, 적설 등 외부 요인과 사용량을 연결하는 역할입니다.",
        "relation": "매장과 기상 측정소를 연결하는 매핑 테이블입니다. KEPCO의 사업소-관측소 또는 계량지점-지역 매핑과 비슷합니다.",
    },
    "dev.debit_card_specializing": {
        "yearmonth": "고객별 월별 소비량 테이블입니다. 고객번호 단위 월별 전력 사용량 테스트에 가장 직접적으로 대응됩니다.",
        "transactions_1k": "거래 일시, 고객, 카드, 주유소, 상품, 금액, 가격이 있는 거래 fact 테이블입니다.",
        "customers": "고객 ID, 세그먼트, 통화 정보를 담은 고객 차원 테이블입니다.",
        "gasstations": "주유소 ID, 체인, 국가, 세그먼트가 있는 지점/사업소 차원 테이블입니다.",
        "products": "상품 ID와 설명을 담은 요금 항목 또는 서비스 항목 차원으로 볼 수 있습니다.",
    },
    "train.regional_sales": {
        "Sales Orders": "주문번호, 고객, 매장, 상품, 판매채널, 주문일, 수량/금액 필드를 가진 중심 fact 테이블입니다.",
        "Store Locations": "매장 ID와 도시, 주, 카운티, 위도/경도 등 위치 정보를 담은 지역 차원입니다.",
        "Customers": "고객 ID와 고객명을 담은 고객 차원입니다.",
        "Regions": "주 코드와 권역을 연결하는 지역 매핑 테이블입니다.",
        "Products": "상품 ID와 상품명을 담은 상품/요금 항목 차원입니다.",
        "Sales Team": "영업팀과 권역 정보를 담은 조직 차원입니다.",
    },
    "train.superstore": {
        "west_superstore": "서부 권역 판매 fact 테이블입니다. 주문일, 고객, 상품, 매출, 수량, 할인 등이 있습니다.",
        "east_superstore": "동부 권역 판매 fact 테이블입니다. 지역별 분리 테이블을 통합하는 평가에 좋습니다.",
        "central_superstore": "중부 권역 판매 fact 테이블입니다.",
        "south_superstore": "남부 권역 판매 fact 테이블입니다.",
        "people": "고객 ID, 고객명, 세그먼트, 도시, 주, 우편번호를 담은 고객/지역 차원입니다.",
        "product": "상품 ID, 상품명, 카테고리, 하위 카테고리를 담은 상품 차원입니다.",
    },
    "train.retails": {
        "lineitem": "주문 상세 라인 테이블입니다. 수량, 할인, 세금, 배송일 등이 있어 상세 사용내역 fact로 볼 수 있습니다.",
        "orders": "주문 헤더 테이블입니다. 고객, 주문일, 우선순위, 상태성 필드를 담습니다.",
        "customer": "고객, 국가, 시장 세그먼트, 주소, 전화번호를 담은 고객 차원입니다.",
        "supplier": "공급자와 국가, 주소 정보를 담은 공급/거래처 차원입니다.",
        "part": "상품/부품 마스터입니다.",
        "partsupp": "부품-공급자 관계와 공급 비용, 재고량을 담은 연결 테이블입니다.",
        "nation": "국가와 지역 키를 담은 지리 차원입니다.",
        "region": "상위 지역 차원입니다.",
    },
    "train.bike_share_1": {
        "status": "스테이션별 특정 시각의 사용 가능 자전거/도크 수 상태 이력입니다. 설비별 부하/상태 이력에 가장 가깝습니다.",
        "station": "스테이션 ID, 이름, 좌표, 도크 수, 도시를 담은 설비 마스터입니다.",
        "trip": "이용 이벤트 이력입니다. 시작/종료 스테이션과 시각, 지속시간을 담습니다.",
        "weather": "날짜별 기상 정보입니다. 설비 운영 상태와 외부 요인을 연결할 수 있습니다.",
    },
    "train.works_cycles": {
        "SalesOrderDetail": "판매 주문 상세입니다. 주문, 상품, 수량, 단가, 할인 등이 연결됩니다.",
        "SalesOrderHeader": "판매 주문 헤더입니다. 고객, 영업사원, 권역, 청구/배송 주소와 주문 상태를 담습니다.",
        "TransactionHistory": "상품별 거래 이력입니다. 자재 이동이나 설비 작업 이력과 유사하게 쓸 수 있습니다.",
        "TransactionHistoryArchive": "거래 이력 아카이브입니다.",
        "WorkOrder": "생산 작업지시 테이블입니다. 작업량, 재고량, 폐기량 등을 담아 배전 작업 지시와 유사한 패턴을 만들 수 있습니다.",
        "WorkOrderRouting": "작업지시별 공정 순서와 작업 위치, 예정/실제 시각을 담은 운영 이력입니다.",
        "BusinessEntity": "조직/사람/거래처의 공통 엔티티 테이블입니다.",
    },
    "dev.financial": {
        "trans": "계좌 거래 내역 fact 테이블입니다. 날짜, 유형, 금액, 잔액이 있어 납부/수납 내역과 유사합니다.",
        "account": "계좌 마스터입니다. 계좌, 지역, 거래 빈도, 개설일을 담습니다.",
        "client": "고객 마스터입니다. 고객, 지역, 성별, 생년월일을 담습니다.",
        "disp": "고객과 계좌의 관계를 나타내는 연결 테이블입니다.",
        "card": "카드 발급 정보입니다.",
        "loan": "대출 금액, 기간, 상환액, 상태를 담은 금액성 계약 테이블입니다.",
        "order": "정기 이체 주문 정보입니다.",
        "district": "지역별 인구·경제 통계를 담은 지역 차원입니다.",
    },
    "train.car_retails": {
        "orders": "고객 주문 헤더입니다. 주문일, 필요일, 배송일, 상태를 담습니다.",
        "orderdetails": "주문 상세입니다. 상품, 수량, 개별 가격, 라인 번호를 담습니다.",
        "payments": "고객 결제 내역입니다. 결제일과 금액이 있어 요금 납부와 유사합니다.",
        "customers": "고객 마스터입니다. 고객명, 연락처, 담당 직원, 신용한도 등을 담습니다.",
        "products": "상품 마스터입니다. 상품 라인, 이름, 설명, 재고, 권장 가격을 담습니다.",
        "offices": "사업소/지점 차원입니다. 도시, 전화, 주소, 지역 정보를 담습니다.",
        "employees": "직원 및 보고 라인 테이블입니다.",
    },
    "train.synthea": {
        "observations": "환자별 관측값 이력입니다. 검침값/계량값 코드별 이력 테이블처럼 볼 수 있습니다.",
        "encounters": "방문/접점 이벤트입니다. 고객 서비스 접점 또는 현장 방문 이벤트와 유사합니다.",
        "claims": "청구 이력입니다. 청구 기간, 조직, 진단/사유 코드가 연결됩니다.",
        "procedures": "처치/작업 이벤트입니다. 배전 작업 또는 현장 조치 이력 패턴을 만들 수 있습니다.",
        "conditions": "상태/진단 이력입니다. 설비 상태 또는 고객 상태 코드 이력과 유사하게 활용할 수 있습니다.",
        "medications": "처방 이력입니다. 서비스/조치 항목 이력의 대체재입니다.",
        "careplans": "계획/관리 이력입니다.",
        "immunizations": "이벤트성 기록 테이블입니다.",
    },
}


def load_catalogs() -> list[dict]:
    catalogs = []
    for split, path in [
        ("dev", Path("data/bird/bird_dev_schema_samples.json")),
        ("train", Path("data/bird/bird_train_schema_samples.json")),
    ]:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
        for db in data["databases"]:
            db["_split"] = split
            catalogs.append(db)
    return catalogs


def db_file_sizes() -> dict[str, int]:
    sizes: dict[str, int] = {}

    dev_root = Path("data/bird/dev_20240627/dev_databases")
    for sqlite_path in dev_root.glob("*/*.sqlite"):
        db_id = sqlite_path.stem
        sizes[f"dev.{db_id}"] = sqlite_path.stat().st_size

    train_zip = Path("data/bird/train/train_databases.zip")
    if train_zip.exists():
        with zipfile.ZipFile(train_zip) as zf:
            for info in zf.infolist():
                if info.filename.startswith("train_databases/") and info.filename.endswith(".sqlite"):
                    parts = info.filename.split("/")
                    if len(parts) >= 3:
                        sizes[f"train.{parts[1]}"] = info.file_size

    return sizes


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"


def fmt(value) -> str:
    if value is None:
        return "NULL"
    text = str(value).replace("\n", " ")
    return text if len(text) <= 78 else text[:75] + "..."


def key_columns(table: dict, limit: int = 6) -> list[str]:
    pk = [c["name"] for c in table["columns"] if c["is_primary_key"]]
    fk = [c["name"] for c in table["columns"] if c["foreign_keys"] and c["name"] not in pk]
    rest = [c["name"] for c in table["columns"] if c["name"] not in pk and c["name"] not in fk]
    return (pk + fk + rest)[:limit]


def sample_preview(table: dict, limit: int = 4) -> str:
    if not table["sample_rows"]:
        return "-"
    row = table["sample_rows"][0]
    parts = []
    for col in key_columns(table, limit):
        if col in row:
            parts.append(f"<b>{html.escape(col)}</b>={html.escape(fmt(row[col]))}")
    return "; ".join(parts) if parts else "-"


def table_rows(db: dict, split_db: str) -> str:
    rows = []
    for table in sorted(db["tables"], key=lambda item: item["row_count"], reverse=True)[:8]:
        columns = ", ".join(f"<code>{html.escape(col)}</code>" for col in key_columns(table))
        rows.append(
            f"""
            <tr>
              <td><code>{html.escape(table["name"])}</code></td>
              <td>{html.escape(TABLE_DETAILS.get(split_db, {}).get(table["name"], "테이블명과 컬럼 기준으로 도메인 역할을 해석해야 하는 테이블입니다."))}</td>
              <td class="num">{table["row_count"]:,}</td>
              <td class="num">{len(table["columns"])}</td>
              <td>{columns}</td>
              <td>{sample_preview(table)}</td>
            </tr>
            """
        )
    return "\n".join(rows)


def main() -> None:
    catalogs = load_catalogs()
    sizes = db_file_sizes()
    selected = []
    for rank, (split, db_id, reason, tag) in enumerate(CHOICES, 1):
        db = next(item for item in catalogs if item["_split"] == split and item["db_id"] == db_id)
        selected.append((rank, db, reason, tag))

    max_rows = max(sum(t["row_count"] for t in db["tables"]) for _, db, _, _ in selected)

    overview_rows = []
    sections = []
    for rank, db, reason, tag in selected:
        row_count = sum(t["row_count"] for t in db["tables"])
        split_db = f"{db['_split']}.{db['db_id']}"
        detail = DB_DETAILS[split_db]
        db_size = sizes.get(split_db, 0)
        overview_rows.append(
            f"""
            <tr>
              <td class="rank">{rank}</td>
              <td><code>{html.escape(split_db)}</code></td>
              <td><span class="tag">{html.escape(tag)}</span></td>
              <td>{html.escape(reason)}</td>
              <td class="num">{db["table_count"]}</td>
              <td class="num">{db["column_count"]}</td>
              <td class="num">{row_count:,}</td>
              <td class="num">{html.escape(human_size(db_size))}</td>
              <td class="num">{db["question_count"]}</td>
            </tr>
            """
        )
        width = row_count / max_rows * 100
        sections.append(
            f"""
            <section class="db-card" id="{html.escape(split_db)}">
              <div class="card-title">
                <div>
                  <span class="rank-badge">{rank}</span>
                  <h2>{html.escape(split_db)}</h2>
                </div>
                <span class="tag">{html.escape(tag)}</span>
              </div>
              <p>{html.escape(detail["summary"])}</p>
              <div class="question-box">
                <strong>KEPCO 질문 유형</strong>
                <ul>
                  {''.join(f"<li>{html.escape(item)}</li>" for item in detail["kepco_questions"])}
                </ul>
              </div>
              <div class="metrics">
                <div><strong>{db["table_count"]}</strong><small>tables</small></div>
                <div><strong>{db["column_count"]}</strong><small>columns</small></div>
                <div><strong>{row_count:,}</strong><small>rows</small></div>
                <div><strong>{html.escape(human_size(db_size))}</strong><small>sqlite size</small></div>
                <div><strong>{db["question_count"]}</strong><small>questions</small></div>
              </div>
              <div class="rowbar-label">
                <strong>DB 전체 row 수 상대 규모</strong>
                <span>Top 10 중 가장 큰 DB를 100%로 둔 비교입니다.</span>
              </div>
              <div class="rowbar"><div style="width:{width:.2f}%"></div></div>
              <table>
                <thead>
                  <tr><th>Table</th><th>테이블 역할</th><th>Rows</th><th>Cols</th><th>주요 컬럼</th><th>실제 샘플 값</th></tr>
                </thead>
                <tbody>{table_rows(db, split_db)}</tbody>
              </table>
            </section>
            """
        )

    html_text = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BIRD DB Similarity Top 10 for KEPCO NL2SQL</title>
  <style>
    :root {{
      --bg: #f6f6f3;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #667085;
      --line: #d9d9d2;
      --blue: #1d4ed8;
      --teal: #0f766e;
      --gold: #b45309;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    header {{
      padding: 30px 36px 22px;
      background: #fff;
      border-bottom: 1px solid var(--line);
    }}
    h1, h2 {{ letter-spacing: 0; margin: 0; }}
    h1 {{ font-size: 30px; }}
    h2 {{ font-size: 20px; display: inline; }}
    p {{ color: var(--muted); line-height: 1.5; margin: 8px 0 0; }}
    main {{ padding: 24px 36px 42px; }}
    .intro {{
      display: grid;
      grid-template-columns: 1.1fr .9fr;
      gap: 16px;
      margin-bottom: 18px;
    }}
    .panel, .db-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    .panel {{ padding: 16px; }}
    .panel h3 {{ margin: 0 0 10px; font-size: 16px; }}
    .panel ul {{ margin: 0; padding-left: 18px; color: var(--muted); line-height: 1.6; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: #fff;
      overflow: hidden;
      border-radius: 8px;
      border: 1px solid var(--line);
    }}
    th, td {{
      padding: 9px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      font-size: 13px;
    }}
    th {{ background: #ededE8; color: #3d4654; font-size: 12px; }}
    code {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
      background: #f0f2f4;
      padding: 2px 4px;
      border-radius: 4px;
    }}
    .num, .rank {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .rank {{ font-weight: 700; color: var(--blue); }}
    .tag {{
      display: inline-block;
      border: 1px solid #a7d4ce;
      background: #e7f5f2;
      color: #0f5c56;
      border-radius: 999px;
      padding: 3px 8px;
      font-size: 12px;
      white-space: nowrap;
    }}
    .db-card {{ padding: 18px; margin-top: 16px; }}
    .card-title {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
    }}
    .rank-badge {{
      display: inline-grid;
      place-items: center;
      width: 28px;
      height: 28px;
      border-radius: 50%;
      background: var(--blue);
      color: white;
      font-weight: 700;
      margin-right: 8px;
      font-size: 13px;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
      margin: 14px 0;
    }}
    .metrics div {{
      background: #f4f4f1;
      border-radius: 6px;
      padding: 10px;
    }}
    .metrics strong {{ display: block; font-size: 18px; }}
    .metrics small {{ color: var(--muted); }}
    .rowbar-label {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin: 2px 0 6px;
      color: var(--muted);
      font-size: 12px;
    }}
    .rowbar-label strong {{ color: var(--ink); font-size: 13px; }}
    .question-box {{
      margin-top: 12px;
      padding: 12px 14px;
      border: 1px solid #d8e7df;
      background: #f0f8f4;
      border-radius: 8px;
    }}
    .question-box strong {{ display: block; margin-bottom: 6px; }}
    .question-box ul {{ margin: 0; padding-left: 18px; color: #385344; line-height: 1.5; }}
    .rowbar {{
      height: 10px;
      background: #eeeeea;
      border-radius: 999px;
      overflow: hidden;
      margin-bottom: 14px;
    }}
    .rowbar div {{ height: 10px; background: var(--gold); border-radius: 999px; }}
    @media (max-width: 920px) {{
      header, main {{ padding-left: 18px; padding-right: 18px; }}
      .intro, .metrics {{ grid-template-columns: 1fr; }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>BIRD DB Similarity Top 10 for KEPCO NL2SQL</h1>
    <p>전력 사용량, 요금, 고객, 지역, 송변전·배전 설비 운영 데이터와 유사한 BIRD dev/train DB를 우선순위로 정리했습니다.</p>
  </header>
  <main>
    <section class="intro">
      <div class="panel">
        <h3>추천 기준</h3>
        <ul>
          <li>사용량·금액·단가·청구 같은 수치 집계가 있는가</li>
          <li>고객, 지역, 기간, 지점/설비를 조인해 물을 수 있는가</li>
          <li>실제 업무 DB처럼 여러 테이블 조인과 상태 이력을 포함하는가</li>
        </ul>
      </div>
      <div class="panel">
        <h3>활용 방향</h3>
        <ul>
          <li><b>기본 평가</b>: sales_in_weather, debit_card_specializing</li>
          <li><b>요금/고객</b>: regional_sales, superstore, financial</li>
          <li><b>설비/운영</b>: bike_share_1, works_cycles</li>
        </ul>
      </div>
    </section>
    <section>
      <table>
        <thead>
          <tr><th>Rank</th><th>DB</th><th>축</th><th>KEPCO 유사 포인트</th><th>Tables</th><th>Cols</th><th>Rows</th><th>DB Size</th><th>Questions</th></tr>
        </thead>
        <tbody>{''.join(overview_rows)}</tbody>
      </table>
    </section>
    {''.join(sections)}
  </main>
</body>
</html>
"""

    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.write_text(html_text, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
