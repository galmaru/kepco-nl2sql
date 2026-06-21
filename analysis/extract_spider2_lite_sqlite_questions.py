#!/usr/bin/env python3
"""Extract Spider 2.0-Lite local SQLite questions into readable reports."""

from __future__ import annotations

import html
import json
from collections import Counter, defaultdict
from pathlib import Path


IN_JSONL = Path("data/spider2/spider2-lite.jsonl")
GOLD_SQL_DIR = Path("data/spider2/gold_sql")
QUESTION_KO_CACHE = Path("data/spider2/spider2_lite_sqlite_question_ko.json")
OUT_JSON = Path("analysis/output/spider2_lite_sqlite_questions.json")
OUT_MD = Path("analysis/output/spider2_lite_sqlite_questions.md")
OUT_HTML = Path("analysis/output/spider2_lite_sqlite_questions.html")


def load_local_rows() -> list[dict]:
    question_ko = {}
    if QUESTION_KO_CACHE.exists():
        question_ko = json.loads(QUESTION_KO_CACHE.read_text(encoding="utf-8"))

    rows = []
    for line in IN_JSONL.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if str(row.get("instance_id", "")).startswith("local"):
            gold_path = GOLD_SQL_DIR / f"{row['instance_id']}.sql"
            row["gold_sql"] = gold_path.read_text(encoding="utf-8").strip() if gold_path.exists() else None
            row["question_ko"] = question_ko.get(row["instance_id"])
            rows.append(row)
    return rows


def write_json(rows: list[dict]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def md_escape(text: object) -> str:
    if text is None:
        return ""
    return str(text).replace("|", "\\|").replace("\n", " ")


def write_markdown(rows: list[dict]) -> None:
    counts = Counter(row["db"] for row in rows)
    by_db = defaultdict(list)
    for row in rows:
        by_db[row["db"]].append(row)

    lines = [
        "# Spider 2.0-Lite SQLite Questions",
        "",
        f"- Total SQLite/local questions: {len(rows)}",
        f"- SQLite/local DBs: {len(counts)}",
        "- Filter: `instance_id` starts with `local`",
        f"- Released gold SQL: {sum(1 for row in rows if row.get('gold_sql'))}",
        f"- Not released gold SQL: {sum(1 for row in rows if not row.get('gold_sql'))}",
        "",
        "## DB별 질문 수",
        "",
        "| DB | Questions |",
        "|---|---:|",
    ]

    for db, count in sorted(counts.items(), key=lambda item: (-item[1], item[0].lower())):
        lines.append(f"| `{db}` | {count} |")

    for db, count in sorted(counts.items(), key=lambda item: (-item[1], item[0].lower())):
        lines.extend(
            [
                "",
                f"## {db}",
                "",
                "| instance_id | external_knowledge | question_ko | question_en | gold_sql |",
                "|---|---|---|---|---|",
            ]
        )
        for row in by_db[db]:
            gold_sql = row.get("gold_sql") or "not released"
            lines.append(
                f"| `{row['instance_id']}` | {md_escape(row.get('external_knowledge'))} | "
                f"{md_escape(row.get('question_ko'))} | {md_escape(row['question'])} | `{md_escape(gold_sql)}` |"
            )

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_html(rows: list[dict]) -> None:
    counts = Counter(row["db"] for row in rows)
    by_db = defaultdict(list)
    for row in rows:
        by_db[row["db"]].append(row)

    css = """
    :root { color-scheme: light; --ink:#1f2937; --muted:#687385; --line:#d7dde7; --bg:#f7f8fb; --head:#eef2f7; --accent:#1f6feb; }
    body { margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; color:var(--ink); background:var(--bg); }
    header { background:#fff; border-bottom:1px solid var(--line); padding:30px 40px 18px; }
    main { padding:24px 40px 48px; }
    h1 { margin:0 0 8px; font-size:28px; }
    h2 { margin:34px 0 12px; font-size:20px; }
    .note { color:var(--muted); line-height:1.55; }
    .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px; margin:18px 0 24px; }
    .card { background:#fff; border:1px solid var(--line); border-radius:8px; padding:12px 14px; display:flex; justify-content:space-between; gap:12px; }
    .card a { color:var(--accent); text-decoration:none; font-weight:650; }
    .card strong { white-space:nowrap; }
    .table-wrap { overflow-x:auto; border:1px solid var(--line); background:#fff; }
    table { width:100%; min-width:2050px; table-layout:fixed; border-collapse:collapse; }
    th, td { padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; font-size:13px; }
    th { background:var(--head); font-weight:650; }
    th:nth-child(1), td:nth-child(1) { width:110px; }
    th:nth-child(2), td:nth-child(2) { width:180px; }
    th:nth-child(3), td:nth-child(3) { width:430px; }
    th:nth-child(4), td:nth-child(4) { width:430px; }
    th:nth-child(5), td:nth-child(5) { width:750px; }
    .qid { font-weight:700; color:var(--accent); }
    .question-text { line-height:1.5; }
    .sql-box { background:#0f172a; color:#e5edf7; border-radius:6px; padding:10px; overflow:auto; max-width:100%; max-height:420px; margin:0; }
    .sql-box code { white-space:pre; word-break:normal; font-size:12px; line-height:1.45; }
    .db-section { scroll-margin-top:18px; }
    .missing { color:var(--muted); font-style:italic; }
    """

    card_html = []
    for db, count in sorted(counts.items(), key=lambda item: (-item[1], item[0].lower())):
        anchor = "db-" + "".join(ch if ch.isalnum() else "-" for ch in db.lower())
        card_html.append(
            f'<div class="card"><a href="#{anchor}">{html.escape(db)}</a><strong>{count}</strong></div>'
        )

    sections = []
    for db, count in sorted(counts.items(), key=lambda item: (-item[1], item[0].lower())):
        anchor = "db-" + "".join(ch if ch.isalnum() else "-" for ch in db.lower())
        rows_html = []
        for row in by_db[db]:
            ext = row.get("external_knowledge") or ""
            ext_html = html.escape(ext) if ext else '<span class="missing">no external knowledge</span>'
            if row.get("gold_sql"):
                gold = f'<pre class="sql-box"><code>{html.escape(row["gold_sql"])}</code></pre>'
            else:
                gold = '<span class="missing">not released</span>'
            rows_html.append(
                "<tr>"
                f'<td class="qid">{html.escape(row["instance_id"])}</td>'
                f"<td>{ext_html}</td>"
                f'<td class="question-text">{html.escape(row.get("question_ko") or "")}</td>'
                f'<td class="question-text">{html.escape(row["question"])}</td>'
                f"<td>{gold}</td>"
                "</tr>"
            )
        sections.append(
            f"""
            <section id="{anchor}" class="db-section">
              <h2>{html.escape(db)} <span class="note">({count})</span></h2>
              <div class="table-wrap">
                <table>
                  <thead><tr><th>instance_id</th><th>external_knowledge</th><th>question_ko</th><th>question_en</th><th>gold_sql</th></tr></thead>
                  <tbody>{''.join(rows_html)}</tbody>
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
  <title>Spider 2.0-Lite SQLite Questions</title>
  <style>{css}</style>
</head>
<body>
  <header>
    <h1>Spider 2.0-Lite SQLite Questions</h1>
    <p class="note">총 {len(rows)}개 질문, {len(counts)}개 SQLite/local DB입니다. 기준은 <code>instance_id</code>가 <code>local</code>로 시작하는 항목입니다. 공식 공개 gold SQL은 {sum(1 for row in rows if row.get('gold_sql'))}개이며, 나머지는 공개되어 있지 않습니다.</p>
  </header>
  <main>
    <div class="cards">{''.join(card_html)}</div>
    {''.join(sections)}
  </main>
</body>
</html>
"""
    OUT_HTML.write_text(html_text, encoding="utf-8")


def main() -> None:
    rows = load_local_rows()
    write_json(rows)
    write_markdown(rows)
    write_html(rows)
    counts = Counter(row["db"] for row in rows)
    print(f"wrote {OUT_JSON}")
    print(f"wrote {OUT_MD}")
    print(f"wrote {OUT_HTML}")
    print(f"questions={len(rows)} dbs={len(counts)}")


if __name__ == "__main__":
    main()
