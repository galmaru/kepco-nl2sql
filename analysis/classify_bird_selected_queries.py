#!/usr/bin/env python3
"""Classify BIRD selected DB questions by SQL type and heuristic difficulty."""

from __future__ import annotations

import html
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


TRAIN_JSON = Path("data/bird/train/train.json")
OUT_JSON = Path("analysis/output/bird_selected_query_classification.json")
OUT_MD = Path("analysis/output/bird_selected_query_classification.md")
OUT_HTML = Path("analysis/output/bird_selected_query_classification.html")
TARGET_DBS = ("bike_share_1", "works_cycles")


TYPE_RULES = [
    ("조인/관계 조회", re.compile(r"\bJOIN\b", re.I)),
    ("집계/카운트", re.compile(r"\b(COUNT|AVG|SUM|MIN|MAX)\s*\(", re.I)),
    ("그룹별 집계", re.compile(r"\bGROUP\s+BY\b", re.I)),
    ("최대/최소/순위", re.compile(r"\bORDER\s+BY\b[\s\S]*\bLIMIT\b|\bMAX\s*\(|\bMIN\s*\(", re.I)),
    ("기간/시간 조건", re.compile(r"\b(STRFTIME|DATE|YEAR|MONTH|DAY|SUBSTR|INSTR)\s*\(|\b(StartDate|EndDate|ModifiedDate|OrderDate|DueDate|ShipDate|start_date|end_date|time|date)\b", re.I)),
    ("상태/조건 필터", re.compile(r"\bWHERE\b|\bHAVING\b", re.I)),
    ("서브쿼리", re.compile(r"\(\s*SELECT\b", re.I)),
    ("조건부 계산", re.compile(r"\bCASE\s+WHEN\b", re.I)),
    ("계산/비율", re.compile(r"(\+|\-|\*|/)|\bCAST\s*\(", re.I)),
    ("고유값/중복 제거", re.compile(r"\bDISTINCT\b", re.I)),
    ("문자열 검색", re.compile(r"\bLIKE\b|\bSUBSTR\s*\(|\bINSTR\s*\(|\bLENGTH\s*\(", re.I)),
    ("존재/불리언 판정", re.compile(r"\bCASE\s+WHEN\b|>=\s*1\s+THEN\s+'Yes'|\bEXISTS\b", re.I)),
]


def load_rows() -> list[dict]:
    with TRAIN_JSON.open(encoding="utf-8") as f:
        rows = json.load(f)
    return [row for row in rows if row["db_id"] in TARGET_DBS]


def normalized_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip())


def count_tables(sql: str) -> int:
    names = re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*|\"[^\"]+\"|\[[^\]]+\])", sql, re.I)
    return len(set(name.strip('"[]') for name in names))


def count_predicates(sql: str) -> int:
    where_match = re.search(r"\bWHERE\b([\s\S]*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)", sql, re.I)
    having_match = re.search(r"\bHAVING\b([\s\S]*?)(?:\bORDER\s+BY\b|\bLIMIT\b|$)", sql, re.I)
    text = " ".join(m.group(1) for m in [where_match, having_match] if m)
    if not text:
        return 0
    return len(re.findall(r"\bAND\b|\bOR\b|=|<>|!=|>=|<=|>|<|\bLIKE\b|\bIN\b|\bIS\b", text, re.I))


def metrics(sql: str) -> dict:
    compact = normalized_sql(sql)
    return {
        "tables": count_tables(compact),
        "joins": len(re.findall(r"\bJOIN\b", compact, re.I)),
        "subqueries": len(re.findall(r"\(\s*SELECT\b", compact, re.I)),
        "aggregates": len(re.findall(r"\b(COUNT|AVG|SUM|MIN|MAX)\s*\(", compact, re.I)),
        "group_by": bool(re.search(r"\bGROUP\s+BY\b", compact, re.I)),
        "order_by": bool(re.search(r"\bORDER\s+BY\b", compact, re.I)),
        "limit": bool(re.search(r"\bLIMIT\b", compact, re.I)),
        "case_when": bool(re.search(r"\bCASE\s+WHEN\b", compact, re.I)),
        "distinct": bool(re.search(r"\bDISTINCT\b", compact, re.I)),
        "set_op": bool(re.search(r"\b(UNION|INTERSECT|EXCEPT)\b", compact, re.I)),
        "string_or_date_fn": bool(re.search(r"\b(STRFTIME|SUBSTR|INSTR|LENGTH|DATE)\s*\(", compact, re.I)),
        "predicates": count_predicates(compact),
        "sql_length": len(compact),
    }


def classify_types(sql: str) -> list[str]:
    labels = [label for label, pattern in TYPE_RULES if pattern.search(sql)]
    return labels or ["단순 조회"]


def difficulty(m: dict) -> str:
    score = 0
    score += min(m["joins"], 4)
    score += min(m["subqueries"] * 2, 6)
    score += 1 if m["aggregates"] else 0
    score += 1 if m["group_by"] else 0
    score += 1 if m["order_by"] and m["limit"] else 0
    score += 1 if m["case_when"] else 0
    score += 1 if m["distinct"] else 0
    score += 2 if m["set_op"] else 0
    score += 1 if m["string_or_date_fn"] else 0
    score += 1 if m["predicates"] >= 3 else 0
    score += 1 if m["tables"] >= 4 else 0
    score += 1 if m["sql_length"] >= 350 else 0

    if score <= 1 and m["tables"] <= 1 and not m["subqueries"]:
        return "Easy"
    if score <= 3 and m["joins"] <= 1 and m["subqueries"] == 0:
        return "Medium"
    if score <= 6 and m["joins"] <= 3 and m["subqueries"] <= 1:
        return "Hard"
    return "Extra Hard"


def difficulty_reason(m: dict) -> str:
    parts = []
    if m["tables"]:
        parts.append(f"{m['tables']} tables")
    if m["joins"]:
        parts.append(f"{m['joins']} joins")
    if m["subqueries"]:
        parts.append(f"{m['subqueries']} subqueries")
    if m["aggregates"]:
        parts.append(f"{m['aggregates']} aggregates")
    if m["group_by"]:
        parts.append("GROUP BY")
    if m["order_by"] and m["limit"]:
        parts.append("ORDER BY + LIMIT")
    if m["case_when"]:
        parts.append("CASE")
    if m["string_or_date_fn"]:
        parts.append("date/string fn")
    if m["predicates"]:
        parts.append(f"{m['predicates']} predicates")
    return ", ".join(parts) or "single-table projection"


def classify() -> list[dict]:
    classified = []
    per_db_index: defaultdict[str, int] = defaultdict(int)
    for original_index, row in enumerate(load_rows(), 1):
        db_id = row["db_id"]
        per_db_index[db_id] += 1
        sql = normalized_sql(row["SQL"])
        m = metrics(sql)
        classified.append(
            {
                "global_index": original_index,
                "db_id": db_id,
                "db_query_id": per_db_index[db_id],
                "difficulty": difficulty(m),
                "types": classify_types(sql),
                "difficulty_reason": difficulty_reason(m),
                "metrics": m,
                "question": row["question"],
                "evidence": row.get("evidence", ""),
                "SQL": sql,
            }
        )
    return classified


def summarize(rows: list[dict]) -> dict:
    by_db: dict[str, dict] = {}
    for db_id in TARGET_DBS:
        db_rows = [row for row in rows if row["db_id"] == db_id]
        type_counts = Counter(label for row in db_rows for label in row["types"])
        by_db[db_id] = {
            "query_count": len(db_rows),
            "difficulty_counts": dict(Counter(row["difficulty"] for row in db_rows)),
            "type_counts": dict(type_counts.most_common()),
            "avg_tables": round(sum(row["metrics"]["tables"] for row in db_rows) / len(db_rows), 2),
            "avg_joins": round(sum(row["metrics"]["joins"] for row in db_rows) / len(db_rows), 2),
            "avg_subqueries": round(sum(row["metrics"]["subqueries"] for row in db_rows) / len(db_rows), 2),
        }
    return {"target_dbs": TARGET_DBS, "summary_by_db": by_db}


def md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def write_json(rows: list[dict], summary: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps({"summary": summary, "queries": rows}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_markdown(rows: list[dict], summary: dict) -> None:
    lines = [
        "# BIRD Selected Query Classification",
        "",
        "대상 DB: `bike_share_1`, `works_cycles`",
        "",
        "주의: BIRD train JSON에는 공식 난이도/유형 라벨이 없으므로, 아래 분류는 SQL 패턴 기반 휴리스틱이다.",
        "",
        "## 분류 기준",
        "",
        "| difficulty | 기준 요약 |",
        "|---|---|",
        "| Easy | 단일 테이블 단순 조회 또는 아주 단순한 조건/정렬 |",
        "| Medium | 단일 조인, 기본 집계, 기본 필터/정렬이 있는 질의 |",
        "| Hard | 다중 조인, 서브쿼리, 그룹 집계, 날짜/문자열 처리 등 복합 질의 |",
        "| Extra Hard | 복잡한 다중 조인과 서브쿼리/계산/조건이 결합된 질의 |",
        "",
        "## 요약",
        "",
        "| DB | queries | Easy | Medium | Hard | Extra Hard | avg_tables | avg_joins | avg_subqueries |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for db_id in TARGET_DBS:
        item = summary["summary_by_db"][db_id]
        counts = item["difficulty_counts"]
        lines.append(
            f"| `{db_id}` | {item['query_count']} | {counts.get('Easy', 0)} | {counts.get('Medium', 0)} | "
            f"{counts.get('Hard', 0)} | {counts.get('Extra Hard', 0)} | {item['avg_tables']} | "
            f"{item['avg_joins']} | {item['avg_subqueries']} |"
        )

    for db_id in TARGET_DBS:
        lines.extend(["", f"## {db_id} 유형 분포", "", "| type | count |", "|---|---:|"])
        for label, count in summary["summary_by_db"][db_id]["type_counts"].items():
            lines.append(f"| {label} | {count} |")

    for db_id in TARGET_DBS:
        lines.extend(
            [
                "",
                f"## {db_id} query list",
                "",
                "| id | difficulty | types | reason | question | evidence | SQL |",
                "|---:|---|---|---|---|---|---|",
            ]
        )
        for row in [r for r in rows if r["db_id"] == db_id]:
            lines.append(
                f"| {row['db_query_id']} | {row['difficulty']} | {', '.join(row['types'])} | "
                f"{md_escape(row['difficulty_reason'])} | {md_escape(row['question'])} | "
                f"{md_escape(row['evidence'])} | `{md_escape(row['SQL'])}` |"
            )

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def chip(text: str) -> str:
    return f'<span class="chip">{html.escape(text)}</span>'


def write_html(rows: list[dict], summary: dict) -> None:
    css = """
    :root { color-scheme: light; --ink:#1f2937; --muted:#6b7280; --line:#d7dde7; --bg:#f7f8fb; --blue:#1f6feb; --green:#248a5a; --orange:#b76e00; --red:#b42318; }
    body { margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; color:var(--ink); background:var(--bg); }
    header { padding:32px 40px 18px; background:#fff; border-bottom:1px solid var(--line); }
    h1 { margin:0 0 8px; font-size:28px; }
    h2 { margin:34px 0 12px; font-size:20px; }
    h3 { margin:26px 0 10px; font-size:16px; }
    main { padding:0 40px 40px; }
    .note { color:var(--muted); max-width:960px; line-height:1.55; }
    .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; margin-top:18px; }
    .card { background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; }
    .metric { display:grid; grid-template-columns:1fr auto; gap:8px; font-size:14px; padding:4px 0; }
    .metric strong { font-size:15px; }
    table { width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line); }
    th, td { border-bottom:1px solid var(--line); padding:8px 10px; text-align:left; vertical-align:top; font-size:13px; }
    th { background:#eef2f7; font-weight:650; position:sticky; top:0; z-index:1; }
    code { white-space:pre-wrap; word-break:break-word; font-size:12px; }
    .chip { display:inline-block; padding:2px 7px; border:1px solid #c7d2fe; color:#273a78; background:#eef2ff; border-radius:999px; margin:0 4px 4px 0; font-size:12px; }
    .difficulty { font-weight:700; white-space:nowrap; }
    .Easy { color:var(--green); }
    .Medium { color:var(--blue); }
    .Hard { color:var(--orange); }
    .ExtraHard { color:var(--red); }
    .scroll { max-height:760px; overflow:auto; border:1px solid var(--line); }
    .scroll table { border:0; }
    .type-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px; }
    """

    summary_cards = []
    for db_id in TARGET_DBS:
        item = summary["summary_by_db"][db_id]
        counts = item["difficulty_counts"]
        summary_cards.append(
            f"""
            <section class="card">
              <h3>{html.escape(db_id)}</h3>
              <div class="metric"><span>queries</span><strong>{item['query_count']}</strong></div>
              <div class="metric"><span>Easy</span><strong class="Easy">{counts.get('Easy', 0)}</strong></div>
              <div class="metric"><span>Medium</span><strong class="Medium">{counts.get('Medium', 0)}</strong></div>
              <div class="metric"><span>Hard</span><strong class="Hard">{counts.get('Hard', 0)}</strong></div>
              <div class="metric"><span>Extra Hard</span><strong class="ExtraHard">{counts.get('Extra Hard', 0)}</strong></div>
              <div class="metric"><span>avg tables / joins / subqueries</span><strong>{item['avg_tables']} / {item['avg_joins']} / {item['avg_subqueries']}</strong></div>
            </section>
            """
        )

    type_sections = []
    for db_id in TARGET_DBS:
        rows_html = "\n".join(
            f"<tr><td>{html.escape(label)}</td><td>{count}</td></tr>"
            for label, count in summary["summary_by_db"][db_id]["type_counts"].items()
        )
        type_sections.append(
            f"""
            <section>
              <h3>{html.escape(db_id)}</h3>
              <table><thead><tr><th>type</th><th>count</th></tr></thead><tbody>{rows_html}</tbody></table>
            </section>
            """
        )

    query_sections = []
    for db_id in TARGET_DBS:
        db_rows = [r for r in rows if r["db_id"] == db_id]
        q_rows = []
        for row in db_rows:
            cls = row["difficulty"].replace(" ", "")
            q_rows.append(
                "<tr>"
                f"<td>{row['db_query_id']}</td>"
                f'<td class="difficulty {cls}">{html.escape(row["difficulty"])}</td>'
                f"<td>{''.join(chip(t) for t in row['types'])}</td>"
                f"<td>{html.escape(row['difficulty_reason'])}</td>"
                f"<td>{html.escape(row['question'])}</td>"
                f"<td>{html.escape(row['evidence'])}</td>"
                f"<td><code>{html.escape(row['SQL'])}</code></td>"
                "</tr>"
            )
        query_sections.append(
            f"""
            <section>
              <h2>{html.escape(db_id)} Queries</h2>
              <div class="scroll">
                <table>
                  <thead><tr><th>ID</th><th>Difficulty</th><th>Types</th><th>Reason</th><th>Question</th><th>Evidence</th><th>SQL</th></tr></thead>
                  <tbody>{''.join(q_rows)}</tbody>
                </table>
              </div>
            </section>
            """
        )

    html_text = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BIRD Selected Query Classification</title>
  <style>{css}</style>
</head>
<body>
  <header>
    <h1>BIRD Selected Query Classification</h1>
    <p class="note">대상 DB는 <code>bike_share_1</code>, <code>works_cycles</code>입니다. BIRD train JSON에는 공식 난이도/유형 라벨이 없으므로, 이 보고서는 SQL 패턴 기반 휴리스틱으로 난이도와 유형을 분류합니다.</p>
  </header>
  <main>
    <h2>Difficulty Summary</h2>
    <div class="cards">{''.join(summary_cards)}</div>

    <h2>Classification Rules</h2>
    <table>
      <thead><tr><th>Difficulty</th><th>Rule summary</th></tr></thead>
      <tbody>
        <tr><td class="difficulty Easy">Easy</td><td>단일 테이블 단순 조회 또는 아주 단순한 조건/정렬</td></tr>
        <tr><td class="difficulty Medium">Medium</td><td>단일 조인, 기본 집계, 기본 필터/정렬이 있는 질의</td></tr>
        <tr><td class="difficulty Hard">Hard</td><td>다중 조인, 서브쿼리, 그룹 집계, 날짜/문자열 처리 등 복합 질의</td></tr>
        <tr><td class="difficulty ExtraHard">Extra Hard</td><td>복잡한 다중 조인과 서브쿼리/계산/조건이 결합된 질의</td></tr>
      </tbody>
    </table>

    <h2>Type Distribution</h2>
    <div class="type-grid">{''.join(type_sections)}</div>

    {''.join(query_sections)}
  </main>
</body>
</html>
"""
    OUT_HTML.write_text(html_text, encoding="utf-8")


def main() -> None:
    rows = classify()
    summary = summarize(rows)
    write_json(rows, summary)
    write_markdown(rows, summary)
    write_html(rows, summary)
    print(f"wrote {OUT_JSON}")
    print(f"wrote {OUT_MD}")
    print(f"wrote {OUT_HTML}")
    for db_id in TARGET_DBS:
        print(db_id, summary["summary_by_db"][db_id])


if __name__ == "__main__":
    main()

