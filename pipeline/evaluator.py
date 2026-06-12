"""[3-6] 자동 평가기: gold SQL vs 생성 SQL 결과 비교 및 오류 분류."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from . import executor

SCENARIOS_PATH = Path(__file__).resolve().parents[1] / "tests" / "nl2sql_scenarios.md"

# 오류 유형 분류 키워드
_ERROR_PATTERNS = [
    ("스키마 오류",   [r"no such column", r"no such table"]),
    ("문법 오류",     [r"syntax error", r"near"]),
    ("타입 오류",     [r"datatype mismatch"]),
    ("타임아웃",      [r"타임아웃"]),
    ("API 오류",      [r"OPENROUTER_API_KEY", r"Connection", r"timeout"]),
]


def load_scenarios(path: Path = SCENARIOS_PATH) -> list[dict]:
    """nl2sql_scenarios.md 파싱 → 시나리오 목록 반환."""
    text = path.read_text(encoding="utf-8")
    scenarios: list[dict] = []

    # ### S-01. 제목 패턴으로 섹션 분리
    blocks = re.split(r'(?=^### [A-Z]+-\d+\.)', text, flags=re.MULTILINE)

    for block in blocks:
        header = re.match(r'^### ([A-Z]+-\d+)\.\s*(.+)', block)
        if not header:
            continue

        sid   = header.group(1)
        title = header.group(2).strip()

        # 자연어 추출
        nl_m = re.search(r'자연어:\s*(.+)', block)
        nl   = nl_m.group(1).strip() if nl_m else ""

        # 테이블 추출
        tbl_m = re.search(r'테이블:\s*(.+)', block)
        tables_raw = tbl_m.group(1).strip() if tbl_m else ""
        tables = [t.strip() for t in re.split(r'[+,]', tables_raw) if t.strip()]

        # gold SQL 추출 (마지막 ```sql 블록)
        sql_blocks = re.findall(r'```sql\s*(.*?)```', block, re.DOTALL)
        gold_sql   = sql_blocks[-1].strip() if sql_blocks else ""

        if nl and gold_sql:
            scenarios.append({
                "id":       sid,
                "title":    title,
                "question": nl,
                "tables":   tables,
                "gold_sql": gold_sql,
            })

    return scenarios


def _normalize_rows(rows: list[dict]) -> list[tuple]:
    """결과 행을 순서 무관 비교용 집합으로 변환."""
    result = []
    for row in rows:
        vals = tuple(
            round(v, 2) if isinstance(v, float) else v
            for v in row.values()
        )
        result.append(vals)
    return sorted(result, key=lambda x: [str(i) for i in x])


def _classify_error(gen_result: dict, gold_result: dict) -> str:
    """오류 유형 분류."""
    if not gold_result["success"]:
        return "평가불가"

    if not gen_result["success"]:
        err = (gen_result.get("error") or "").lower()
        for label, patterns in _ERROR_PATTERNS:
            for p in patterns:
                if re.search(p, err, re.IGNORECASE):
                    return label
        return "실행 오류"

    # 실행은 됐지만 결과 불일치
    gen_rows = _normalize_rows(gen_result["rows"])
    gold_rows = _normalize_rows(gold_result["rows"])

    if gen_rows == gold_rows:
        return "정답"

    # 컬럼 수 비교
    if gen_result["columns"] and gold_result["columns"]:
        if len(gen_result["columns"]) != len(gold_result["columns"]):
            return "컬럼 불일치"

    # 행 수 비교
    if len(gen_rows) == 0 and len(gold_rows) > 0:
        return "결과 없음"

    return "결과 불일치"


def evaluate_one(scenario: dict, pipeline_fn) -> dict[str, Any]:
    """시나리오 1개 평가."""
    gold_result = executor.execute(scenario["gold_sql"])

    pipeline_out = pipeline_fn(scenario)
    gen_sql     = pipeline_out.get("sql", "")
    gen_result  = pipeline_out.get("execution", {"success": False, "rows": [], "error": pipeline_out.get("error")})

    error_type = _classify_error(gen_result, gold_result)
    correct    = error_type == "정답"

    return {
        "id":            scenario["id"],
        "question":      scenario["question"],
        "correct":       correct,
        "error_type":    error_type,
        "generated_sql": gen_sql,
        "gold_sql":      scenario["gold_sql"],
        "gen_rows":      gen_result.get("row_count", 0),
        "gold_rows":     gold_result.get("row_count", 0),
        "gen_error":     gen_result.get("error"),
        "pipeline_error": pipeline_out.get("error"),
    }


def evaluate_all(pipeline_fn, scenario_ids: list[str] | None = None) -> dict[str, Any]:
    """전체 시나리오 평가 후 요약 반환."""
    scenarios = load_scenarios()
    if scenario_ids:
        scenarios = [s for s in scenarios if s["id"] in scenario_ids]

    results = []
    for s in scenarios:
        print(f"  평가 중: {s['id']} — {s['question'][:40]}...", flush=True)
        r = evaluate_one(s, pipeline_fn)
        results.append(r)
        status = "✅" if r["correct"] else f"❌ {r['error_type']}"
        print(f"    {status}", flush=True)

    total     = len(results)
    executed  = sum(1 for r in results if r["gen_error"] is None and r["pipeline_error"] is None)
    correct   = sum(1 for r in results if r["correct"])

    error_counts: dict[str, int] = {}
    for r in results:
        error_counts[r["error_type"]] = error_counts.get(r["error_type"], 0) + 1

    summary = {
        "total":          total,
        "executed":       executed,
        "correct":        correct,
        "exec_rate":      round(executed / total * 100, 1) if total else 0,
        "accuracy":       round(correct / total * 100, 1) if total else 0,
        "error_breakdown": error_counts,
        "results":        results,
    }
    return summary


def print_summary(summary: dict) -> None:
    print("\n" + "=" * 50)
    print(f"총 시나리오: {summary['total']}")
    print(f"실행 가능률: {summary['executed']}/{summary['total']} ({summary['exec_rate']}%)")
    print(f"정답률:      {summary['correct']}/{summary['total']} ({summary['accuracy']}%)")
    print("\n오류 분류:")
    for err_type, cnt in sorted(summary["error_breakdown"].items(), key=lambda x: -x[1]):
        print(f"  {err_type}: {cnt}건")
    print("=" * 50)


def save_results(summary: dict, out_path: Path | None = None) -> Path:
    if out_path is None:
        out_path = Path(__file__).resolve().parents[1] / "tests" / "eval_results.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path
