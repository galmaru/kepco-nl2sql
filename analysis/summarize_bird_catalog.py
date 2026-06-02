#!/usr/bin/env python3
"""
Create a compact human-readable summary and HTML visualization for BIRD dev metadata.
"""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path


DB_DESCRIPTIONS = {
    "debit_card_specializing": "주유소 카드 결제 고객, 상품, 거래, 월별 소비 데이터",
    "financial": "은행 계좌, 고객, 카드, 대출, 거래, 지역 데이터",
    "formula_1": "F1 경기, 드라이버, 팀, 서킷, 결과, 순위 데이터",
    "california_schools": "캘리포니아 학교, 급식 지원, SAT 성적 데이터",
    "card_games": "카드 게임 카드, 세트, 룰링, 언어별 카드 정보 데이터",
    "european_football_2": "유럽 축구 리그, 경기, 팀, 선수, 속성 데이터",
    "thrombosis_prediction": "혈전증 환자, 검사, 실험실 수치 데이터",
    "toxicology": "분자 독성, 원자, 결합, 연결 관계 데이터",
    "student_club": "학생 동아리 회원, 행사, 예산, 수입, 지출 데이터",
    "superhero": "슈퍼히어로 인물, 능력, 속성, 출판사, 정렬 데이터",
    "codebase_community": "개발자 커뮤니티 게시글, 댓글, 투표, 태그, 사용자 데이터",
}

TABLE_DESCRIPTIONS = {
    "customers": "고객 ID, 세그먼트, 통화",
    "gasstations": "주유소와 체인, 국가, 세그먼트",
    "products": "상품 ID와 설명",
    "transactions_1k": "카드 거래 일시, 고객, 주유소, 상품, 금액",
    "yearmonth": "고객별 월별 소비량",
    "account": "은행 계좌, 개설 지점, 빈도, 날짜",
    "card": "카드 발급 정보",
    "client": "은행 고객과 지역 정보",
    "disp": "계좌-고객 관계와 권한 유형",
    "district": "지역 인구·경제 통계",
    "loan": "대출 금액, 기간, 상환, 상태",
    "order": "계좌 이체 주문 정보",
    "trans": "계좌 거래 내역",
    "circuits": "F1 서킷 위치와 속성",
    "constructorResults": "팀별 경기 결과",
    "constructorStandings": "팀별 시즌 순위",
    "constructors": "F1 팀/제조사",
    "driverStandings": "드라이버 시즌 순위",
    "drivers": "드라이버 개인 정보",
    "lapTimes": "랩별 시간 기록",
    "pitStops": "피트스톱 기록",
    "qualifying": "예선 결과",
    "races": "경기 일정과 서킷",
    "results": "레이스 최종 결과",
    "seasons": "시즌 정보",
    "status": "결과 상태 코드",
    "frpm": "무료/감면 급식 및 재학생 통계",
    "schools": "학교 기본 정보와 위치/연락처",
    "satscores": "학교별 SAT 응시 및 점수",
    "cards": "카드 속성, 색상, 타입, 가격 등",
    "foreign_data": "언어별 카드명/텍스트",
    "legalities": "포맷별 카드 사용 가능 여부",
    "rulings": "카드 룰 판정 기록",
    "set_translations": "카드 세트 번역명",
    "sets": "카드 세트 메타데이터",
    "Country": "국가",
    "League": "리그",
    "Match": "축구 경기 상세 통계",
    "Player": "선수 기본 정보",
    "Player_Attributes": "선수 능력치 시계열",
    "Team": "팀 기본 정보",
    "Team_Attributes": "팀 전술/능력치 시계열",
    "Examination": "환자 진찰/진단 정보",
    "Laboratory": "실험실 검사 수치",
    "Patient": "환자 기본 정보",
    "atom": "분자 내 원자 정보",
    "bond": "원자 간 결합 정보",
    "connected": "분자 연결 관계",
    "molecule": "분자와 독성 라벨",
    "attendance": "행사 참석 기록",
    "budget": "동아리 예산",
    "event": "동아리 행사",
    "expense": "지출 내역",
    "income": "수입 내역",
    "major": "전공 코드",
    "member": "회원 정보",
    "zip_code": "우편번호와 위치 정보",
    "alignment": "히어로 성향",
    "attribute": "속성 종류",
    "colour": "색상 코드",
    "gender": "성별 코드",
    "hero_attribute": "히어로별 속성 값",
    "hero_power": "히어로별 능력 매핑",
    "publisher": "출판사",
    "race": "종족",
    "superhero": "슈퍼히어로 기본 정보",
    "superpower": "능력 종류",
    "badges": "사용자 배지",
    "comments": "게시글 댓글",
    "postHistory": "게시글 변경 이력",
    "postLinks": "게시글 간 링크",
    "posts": "질문/답변 게시글",
    "tags": "태그 통계",
    "users": "커뮤니티 사용자",
    "votes": "게시글 투표",
}


def load_catalog(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def fmt(value) -> str:
    if value is None:
        return "NULL"
    text = str(value).replace("\n", " ")
    return text if len(text) <= 48 else text[:45] + "..."


def key_columns(table: dict, limit: int = 7) -> list[str]:
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
            parts.append(f"{col}={fmt(row[col])}")
    return "; ".join(parts) if parts else "-"


def db_description(db: dict) -> str:
    explicit = DB_DESCRIPTIONS.get(db["db_id"])
    if explicit:
        return explicit
    names = [t["name"] for t in db["tables"][:5]]
    return "주요 테이블: " + ", ".join(names)


def write_markdown(catalog: dict, out_md: Path) -> None:
    out_md.parent.mkdir(parents=True, exist_ok=True)
    meta = catalog["metadata"]
    lines = [
        "# BIRD Schema Summary",
        "",
        f"- Source: `{meta['source']}`",
        f"- Databases: {meta['database_count']}",
        f"- Tables: {meta['table_count']}",
        f"- Columns: {meta['column_count']}",
        f"- Questions: {meta['question_count']}",
        f"- Table samples: {meta['sample_rows_per_table']} rows per table",
        "",
        "## Database Overview",
        "",
        "| DB | 내용 | Tables | Columns | Rows | Questions |",
        "|---|---|---:|---:|---:|---:|",
    ]

    for db in catalog["databases"]:
        row_count = sum(t["row_count"] for t in db["tables"])
        lines.append(
            f"| `{db['db_id']}` | {db_description(db)} | "
            f"{db['table_count']} | {db['column_count']} | {row_count:,} | {db['question_count']} |"
        )

    for db in catalog["databases"]:
        lines.extend(
            [
                "",
                f"## {db['db_id']}",
                "",
                db_description(db),
                "",
                "| Table | 내용 | Rows | Columns | 주요 컬럼 | 샘플 |",
                "|---|---|---:|---:|---|---|",
            ]
        )
        for table in db["tables"]:
            columns = ", ".join(f"`{c}`" for c in key_columns(table))
            sample = sample_preview(table)
            lines.append(
                f"| `{table['name']}` | {TABLE_DESCRIPTIONS.get(table['name'], table['bird_name'])} | "
                f"{table['row_count']:,} | {len(table['columns'])} | {columns} | {sample} |"
            )

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def bar(width_pct: float, label: str, color: str = "#2563eb") -> str:
    return (
        f'<div class="bar-row"><div class="bar-label">{html.escape(label)}</div>'
        f'<div class="bar-track"><div class="bar" style="width:{width_pct:.2f}%;background:{color}"></div></div></div>'
    )


def write_html(catalog: dict, out_html: Path) -> None:
    out_html.parent.mkdir(parents=True, exist_ok=True)
    dbs = catalog["databases"]
    max_tables = max(db["table_count"] for db in dbs)
    max_columns = max(db["column_count"] for db in dbs)
    max_rows = max(sum(t["row_count"] for t in db["tables"]) for db in dbs)

    cards = []
    for db in dbs:
        row_count = sum(t["row_count"] for t in db["tables"])
        top_tables = sorted(db["tables"], key=lambda t: t["row_count"], reverse=True)[:4]
        chips = "".join(
            f'<span class="chip">{html.escape(t["name"])} · {t["row_count"]:,}</span>'
            for t in top_tables
        )
        cards.append(
            f"""
            <section class="db-card">
              <div class="card-head">
                <h2>{html.escape(db["db_id"])}</h2>
                <span>{db["table_count"]} tables · {db["column_count"]} columns</span>
              </div>
                <p>{html.escape(db_description(db))}</p>
              <div class="metric-grid">
                <div><strong>{row_count:,}</strong><small>rows</small></div>
                <div><strong>{db["question_count"]}</strong><small>questions</small></div>
                <div><strong>{db["difficulty_counts"].get("challenging", 0)}</strong><small>challenging</small></div>
              </div>
              <div class="chips">{chips}</div>
            </section>
            """
        )

    table_rows = []
    for db in dbs:
        for table in db["tables"]:
            table_rows.append(
                {
                    "db": db["db_id"],
                    "table": table["name"],
                    "rows": table["row_count"],
                    "columns": len(table["columns"]),
                    "desc": TABLE_DESCRIPTIONS.get(table["name"], table["bird_name"]),
                    "sample": sample_preview(table, 3),
                }
            )
    table_rows.sort(key=lambda r: r["rows"], reverse=True)

    top_table_html = "\n".join(
        f"""
        <tr>
          <td>{html.escape(r["db"])}</td>
          <td>{html.escape(r["table"])}</td>
          <td>{html.escape(r["desc"])}</td>
          <td class="num">{r["rows"]:,}</td>
          <td class="num">{r["columns"]}</td>
          <td>{html.escape(r["sample"])}</td>
        </tr>
        """
        for r in table_rows[:30]
    )

    chart_tables = "\n".join(
        bar(db["table_count"] / max_tables * 100, f'{db["db_id"]} · {db["table_count"]}')
        for db in dbs
    )
    chart_columns = "\n".join(
        bar(db["column_count"] / max_columns * 100, f'{db["db_id"]} · {db["column_count"]}', "#0f766e")
        for db in dbs
    )
    chart_rows = "\n".join(
        bar(
            sum(t["row_count"] for t in db["tables"]) / max_rows * 100,
            f'{db["db_id"]} · {sum(t["row_count"] for t in db["tables"]):,}',
            "#b45309",
        )
        for db in dbs
    )

    html_text = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BIRD Dev Schema Overview</title>
  <style>
    :root {{
      --bg: #f7f7f5;
      --panel: #ffffff;
      --ink: #18202a;
      --muted: #667085;
      --line: #d7d7d0;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: var(--bg);
    }}
    header {{
      padding: 28px 36px 18px;
      border-bottom: 1px solid var(--line);
      background: #fff;
    }}
    h1 {{ margin: 0 0 8px; font-size: 30px; letter-spacing: 0; }}
    h2 {{ margin: 0; font-size: 18px; letter-spacing: 0; }}
    h3 {{ margin: 0 0 14px; font-size: 16px; letter-spacing: 0; }}
    p {{ margin: 8px 0 0; color: var(--muted); line-height: 1.45; }}
    main {{ padding: 24px 36px 40px; }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .summary div, .panel, .db-card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    .summary div {{ padding: 16px; }}
    strong {{ display: block; font-size: 24px; }}
    small {{ color: var(--muted); }}
    .charts {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }}
    .panel {{ padding: 16px; min-width: 0; }}
    .bar-row {{
      display: grid;
      grid-template-columns: minmax(130px, 1fr) 1.2fr;
      gap: 10px;
      align-items: center;
      margin: 8px 0;
      font-size: 12px;
    }}
    .bar-label {{ overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .bar-track {{ height: 10px; background: #ececea; border-radius: 999px; overflow: hidden; }}
    .bar {{ height: 10px; border-radius: 999px; }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
      gap: 14px;
      margin-bottom: 22px;
    }}
    .db-card {{ padding: 16px; min-height: 188px; }}
    .card-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: baseline; }}
    .card-head span {{ color: var(--muted); font-size: 12px; white-space: nowrap; }}
    .metric-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin: 14px 0; }}
    .metric-grid div {{ background: #f4f4f1; border-radius: 6px; padding: 10px; }}
    .metric-grid strong {{ font-size: 17px; }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 6px; }}
    .chip {{ border: 1px solid var(--line); border-radius: 999px; padding: 4px 8px; font-size: 12px; background: #fff; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; font-size: 13px; }}
    th {{ background: #eeeeea; font-size: 12px; color: #3f4652; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    @media (max-width: 900px) {{
      header, main {{ padding-left: 18px; padding-right: 18px; }}
      .summary, .charts {{ grid-template-columns: 1fr; }}
      .bar-row {{ grid-template-columns: 1fr; }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>BIRD Dev Schema Overview</h1>
    <p>DB와 테이블의 성격, 규모, 실제 샘플 값을 한 화면에서 훑기 위한 요약입니다.</p>
  </header>
  <main>
    <section class="summary">
      <div><strong>{catalog["metadata"]["database_count"]}</strong><small>databases</small></div>
      <div><strong>{catalog["metadata"]["table_count"]}</strong><small>tables</small></div>
      <div><strong>{catalog["metadata"]["column_count"]}</strong><small>columns</small></div>
      <div><strong>{catalog["metadata"]["question_count"]}</strong><small>dev questions</small></div>
    </section>
    <section class="charts">
      <div class="panel"><h3>Tables by DB</h3>{chart_tables}</div>
      <div class="panel"><h3>Columns by DB</h3>{chart_columns}</div>
      <div class="panel"><h3>Rows by DB</h3>{chart_rows}</div>
    </section>
    <section class="cards">
      {''.join(cards)}
    </section>
    <section>
      <h3>Largest Tables and Samples</h3>
      <table>
        <thead><tr><th>DB</th><th>Table</th><th>내용</th><th>Rows</th><th>Cols</th><th>Sample</th></tr></thead>
        <tbody>{top_table_html}</tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""
    out_html.write_text(html_text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=Path, default=Path("data/bird/bird_dev_schema_samples.json"))
    parser.add_argument("--out-md", type=Path, default=Path("analysis/output/bird_dev_schema_summary.md"))
    parser.add_argument("--out-html", type=Path, default=Path("analysis/output/bird_dev_schema_overview.html"))
    args = parser.parse_args()

    catalog = load_catalog(args.catalog)
    write_markdown(catalog, args.out_md)
    write_html(catalog, args.out_html)
    print(f"Wrote {args.out_md}")
    print(f"Wrote {args.out_html}")


if __name__ == "__main__":
    main()
