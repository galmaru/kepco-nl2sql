"""comparison_results.json → 마크다운 평가 리포트 생성.

사용법:
  python tests/generate_report.py
  python tests/generate_report.py --out tests/my_report.md
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IN  = ROOT / "tests" / "comparison_results.json"
DEFAULT_OUT = ROOT / "tests" / "nl2sql_eval_report.md"

ICON = {"정답": "✅", "거절 성공": "✅"}

SCENARIO_ORDER = ["I-01","I-02","I-03","I-04","I-05","I-06","I-07","I-08",
                  "IJ-01","IJ-02","IJ-03","IJ-04"]


def cell(r: dict) -> str:
    """시나리오 결과 셀 문자열."""
    et = r["error_type"]
    icon = "✅" if r["correct"] else "❌"
    if et in ("정답", "거절 성공"):
        return icon
    return f"{icon} {et}"


def short_model(key: str) -> str:
    """'openai/gpt-oss-120b[full]' → 'gpt-oss-120b[full]'"""
    name = key.split("/")[-1]
    return name


def build_report(data: dict) -> str:
    lines: list[str] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ------------------------------------------------------------------
    # 헤더
    # ------------------------------------------------------------------
    lines += [
        "# NL2SQL 평가 리포트",
        f"",
        f"**생성일시**: {now}  ",
        f"**데이터**: `tests/comparison_results.json`",
        "",
    ]

    # 키를 semantic 유무로 분리
    semantic_keys = [k for k in data if "[" in k]
    legacy_keys   = [k for k in data if "[" not in k]

    # ------------------------------------------------------------------
    # 1. 전체 성능 요약
    # ------------------------------------------------------------------
    lines.append("---\n")
    lines.append("## 1. 전체 성능 요약\n")

    # 1-A. 최신 (전체 스키마)
    if semantic_keys:
        lines.append("### 전체 스키마 투입 방식\n")
        lines.append("| 모델 | semantic | EX | Valid% | IMPOSSIBLE 거절 |")
        lines.append("|------|----------|----|--------|-----------------|")
        for k in semantic_keys:
            s = data[k]
            ex   = f"{s['ex_correct']}/{s['ex_total']} ({s['ex_rate']}%)"
            vld  = f"{s['valid_exec_rate']}%"
            imp  = f"{s['impossible_refused']}/{s['impossible_total']} ({s['impossible_rate']}%)"
            # 모델명과 semantic 분리
            bracket = k.index("[")
            model = short_model(k[:bracket])
            sem   = k[bracket:]
            lines.append(f"| {model} | {sem} | {ex} | {vld} | {imp} |")
        lines.append("")

    # 1-B. 레거시 (Classifier)
    if legacy_keys:
        lines.append("### Classifier 기반 (레거시)\n")
        lines.append("| 모델 | EX | Valid% | IMPOSSIBLE 거절 |")
        lines.append("|------|----|--------|-----------------|")
        for k in legacy_keys:
            s = data[k]
            ex   = f"{s['ex_correct']}/{s['ex_total']} ({s['ex_rate']}%)"
            vld  = f"{s['valid_exec_rate']}%"
            imp  = f"{s['impossible_refused']}/{s['impossible_total']} ({s['impossible_rate']}%)"
            lines.append(f"| {short_model(k)} | {ex} | {vld} | {imp} |")
        lines.append("")

    # ------------------------------------------------------------------
    # 2. 시나리오별 결과 매트릭스 (semantic=full 우선, 없으면 전체)
    # ------------------------------------------------------------------
    lines.append("---\n")
    lines.append("## 2. 시나리오별 결과 매트릭스\n")

    full_keys = [k for k in semantic_keys if k.endswith("[full]")]
    matrix_keys = full_keys if full_keys else semantic_keys if semantic_keys else legacy_keys

    if matrix_keys:
        sem_label = " (semantic=full)" if full_keys else ""
        lines.append(f"> {', '.join(short_model(k) for k in matrix_keys)}{sem_label}\n")

        # 시나리오별 결과 인덱싱
        results_by_key: dict[str, dict[str, dict]] = {}
        for k in matrix_keys:
            results_by_key[k] = {r["id"]: r for r in data[k]["scenario_results"]}

        # 헤더
        header_cols = " | ".join(short_model(k) for k in matrix_keys)
        lines.append(f"| ID | 질문 요약 | {header_cols} |")
        sep_cols = " | ".join("---" for _ in matrix_keys)
        lines.append(f"|----|-----------| {sep_cols} |")

        # 행
        for sid in SCENARIO_ORDER:
            # 질문 요약 (첫 번째 키에서 가져옴)
            first_r = results_by_key[matrix_keys[0]].get(sid, {})
            question = first_r.get("question", sid)
            q_short  = question[:22] + "…" if len(question) > 22 else question
            impossible_mark = " *(IMPOSSIBLE)*" if first_r.get("impossible") else ""

            cells = []
            for k in matrix_keys:
                r = results_by_key[k].get(sid)
                cells.append(cell(r) if r else "—")

            cells_str = " | ".join(cells)
            lines.append(f"| {sid} | {q_short}{impossible_mark} | {cells_str} |")

        lines.append("")

    # ------------------------------------------------------------------
    # 3. 실패 상세 분석 (semantic=full)
    # ------------------------------------------------------------------
    lines.append("---\n")
    lines.append("## 3. 실패 상세 분석\n")

    if not full_keys:
        lines.append("_full semantic 결과 없음_\n")
    else:
        for sid in SCENARIO_ORDER:
            # 하나라도 실패한 시나리오만
            failures = []
            for k in full_keys:
                r = results_by_key[k].get(sid)
                if r and not r["correct"]:
                    failures.append((k, r))

            if not failures:
                continue

            # 시나리오 헤더
            first_r = results_by_key[full_keys[0]].get(sid, {})
            question = first_r.get("question", sid)
            impossible_tag = " `[IMPOSSIBLE]`" if first_r.get("impossible") else ""
            lines.append(f"### {sid}{impossible_tag} — {question}\n")

            # gold SQL
            gold_sql = first_r.get("gold_sql", "")
            if gold_sql and not first_r.get("impossible"):
                lines.append("<details><summary>Gold SQL</summary>\n")
                lines.append(f"```sql\n{gold_sql}\n```\n</details>\n")

            # 모델별 실패 내용
            for k, r in failures:
                model_label = short_model(k)
                et = r["error_type"]
                gen_sql  = r.get("generated_sql", "")
                gen_err  = r.get("gen_error")
                gen_rows = r.get("gen_rows", 0)
                gold_rows= r.get("gold_rows", 0)

                lines.append(f"**{model_label}** — `{et}`")

                if gen_err:
                    lines.append(f"\n> 실행 오류: `{gen_err}`\n")
                elif et == "거절 실패":
                    lines.append("")
                else:
                    lines.append(f"  (생성 {gen_rows}행 / gold {gold_rows}행)\n")

                if gen_sql and et != "거절 실패":
                    lines.append(f"```sql\n{gen_sql}\n```\n")
                elif gen_sql and et == "거절 실패":
                    lines.append(f"```sql\n{gen_sql}\n```\n")

            lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in",  dest="inp", default=str(DEFAULT_IN))
    parser.add_argument("--out", dest="out", default=str(DEFAULT_OUT))
    args = parser.parse_args()

    data = json.loads(Path(args.inp).read_text(encoding="utf-8"))
    report = build_report(data)
    Path(args.out).write_text(report, encoding="utf-8")
    print(f"리포트 저장: {args.out}")


if __name__ == "__main__":
    main()
