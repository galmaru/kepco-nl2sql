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

        # IMPOSSIBLE 태그 감지
        is_impossible = "[IMPOSSIBLE]" in title

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

        # IMPOSSIBLE: gold_sql 없어도 nl만 있으면 포함
        if nl and (gold_sql or is_impossible):
            scenarios.append({
                "id":         sid,
                "title":      title,
                "question":   nl,
                "tables":     tables,
                "gold_sql":   gold_sql,
                "impossible": is_impossible,
            })

    return scenarios


def _normalize_rows(rows: list[dict]) -> list[tuple]:
    """결과 행을 순서 무관 비교용 집합으로 변환."""
    result = []
    for row in rows:
        vals = tuple(
            round(v, 6) if isinstance(v, float) else v
            for v in row.values()
        )
        result.append(vals)
    return sorted(result, key=lambda x: [str(i) for i in x])


def _superset_match(gen_result: dict, gold_result: dict) -> bool:
    """gen이 gold보다 컬럼이 많아도 gold의 모든 값을 포함하면 True.
    gold 행 수 == gen 행 수이고, gold 각 행의 값이 대응 gen 행의 값 집합에 포함되면 성공."""
    gen_rows = gen_result.get("rows", [])
    gold_rows = gold_result.get("rows", [])

    if len(gen_rows) != len(gold_rows):
        return False

    def norm(v):
        # NULL과 0을 동일 취급: COALESCE 미적용 여부를 오답으로 보지 않음
        if v is None:
            return 0
        return round(v, 6) if isinstance(v, float) else v

    def to_set(row: dict) -> frozenset:
        return frozenset(norm(v) for v in row.values())

    gold_sets = [to_set(r) for r in gold_rows]
    gen_sets  = [to_set(r) for r in gen_rows]

    # gold 각 행에 대해 포함 관계를 만족하는 gen 행을 greedy 매칭
    used: set[int] = set()
    for g_set in gold_sets:
        matched = False
        for i, gen_set in enumerate(gen_sets):
            if i not in used and g_set.issubset(gen_set):
                used.add(i)
                matched = True
                break
        if not matched:
            return False
    return True


def _subset_match(gen_result: dict, gold_result: dict) -> bool:
    """gen이 gold보다 컬럼이 적어도 gen의 모든 값이 gold에 포함되면 True.
    gen이 핵심 수치만 반환하고 gold가 중간 계산값을 추가로 포함하는 경우를 정답 처리."""
    gen_rows = gen_result.get("rows", [])
    gold_rows = gold_result.get("rows", [])

    if len(gen_rows) != len(gold_rows):
        return False

    def norm(v):
        if v is None:
            return 0
        return round(v, 6) if isinstance(v, float) else v

    def to_set(row: dict) -> frozenset:
        return frozenset(norm(v) for v in row.values())

    gold_sets = [to_set(r) for r in gold_rows]
    gen_sets  = [to_set(r) for r in gen_rows]

    # gen 각 행에 대해 gen_set ⊆ gold_set 인 gold 행을 greedy 매칭
    used: set[int] = set()
    for gen_set in gen_sets:
        matched = False
        for i, gold_set in enumerate(gold_sets):
            if i not in used and gen_set.issubset(gold_set):
                used.add(i)
                matched = True
                break
        if not matched:
            return False
    return True


def _is_refusal(raw: str) -> bool:
    """LLM 응답이 SQL 생성 거절인지 판단.
    ```sql 블록 없음 AND SELECT/WITH 토큰 없음 → 거절로 판정."""
    has_sql_block = bool(re.search(r'```(?:sql)?', raw, re.IGNORECASE))
    has_select = bool(re.search(r'\b(?:SELECT|WITH)\b', raw, re.IGNORECASE))
    return not has_sql_block and not has_select


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

    # 컬럼이 더 많아도 gold 값을 모두 포함하면 정답 허용
    if _superset_match(gen_result, gold_result):
        return "정답"

    # gen이 핵심 수치만 반환하고 gold가 중간 계산값 추가 포함해도 정답 허용
    if _subset_match(gen_result, gold_result):
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
    is_impossible = scenario.get("impossible", False)

    pipeline_out  = pipeline_fn(scenario)
    gen_sql       = pipeline_out.get("sql", "")
    raw_response  = pipeline_out.get("steps", {}).get("raw", "")
    infra_error   = pipeline_out.get("infra_error", False)
    gen_result    = pipeline_out.get("execution", {"success": False, "rows": [], "error": pipeline_out.get("error")})

    if is_impossible:
        # 거절 판정: raw에 SQL 없으면 거절로 판단
        refused = _is_refusal(raw_response) or (not gen_sql)
        return {
            "id":            scenario["id"],
            "question":      scenario["question"],
            "impossible":    True,
            "refused":       refused,
            "correct":       refused,
            "error_type":    "거절 성공" if refused else "거절 실패",
            "generated_sql": gen_sql,
            "gold_sql":      scenario["gold_sql"],
            "raw_response":  raw_response,
            "gen_rows":      0,
            "gold_rows":     0,
            "gen_error":     gen_result.get("error"),
            "pipeline_error": pipeline_out.get("error"),
            "infra_error":   infra_error,
        }

    gold_result = executor.execute(scenario["gold_sql"])
    error_type  = _classify_error(gen_result, gold_result)
    correct     = error_type == "정답"

    return {
        "id":            scenario["id"],
        "question":      scenario["question"],
        "impossible":    False,
        "correct":       correct,
        "error_type":    error_type,
        "generated_sql": gen_sql,
        "gold_sql":      scenario["gold_sql"],
        "gen_rows":      gen_result.get("row_count", 0),
        "gold_rows":     gold_result.get("row_count", 0),
        "gen_error":     gen_result.get("error"),
        "pipeline_error": pipeline_out.get("error"),
        "infra_error":   infra_error,
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

    # 시나리오 분류
    normal_results     = [r for r in results if not r.get("impossible")]
    impossible_results = [r for r in results if r.get("impossible")]

    # EX / Valid Execution Rate: IMPOSSIBLE 제외한 시나리오 대상
    ex_total    = len(normal_results)
    ex_correct  = sum(1 for r in normal_results if r["correct"])
    infra_fails = sum(1 for r in normal_results if r.get("infra_error"))
    # 인프라 오류 제외하고 실행 성공 여부 판단
    valid_executed = sum(
        1 for r in normal_results
        if not r.get("infra_error")
        and r.get("gen_error") is None
        and r.get("pipeline_error") is None
    )

    # IMPOSSIBLE 거절률
    impossible_total    = len(impossible_results)
    impossible_refused  = sum(1 for r in impossible_results if r.get("refused"))

    error_counts: dict[str, int] = {}
    for r in results:
        error_counts[r["error_type"]] = error_counts.get(r["error_type"], 0) + 1

    summary = {
        "total":              len(results),
        # EX: IMPOSSIBLE 제외
        "ex_total":           ex_total,
        "ex_correct":         ex_correct,
        "ex_rate":            round(ex_correct / ex_total * 100, 1) if ex_total else 0,
        # Valid Execution Rate: 인프라 오류 제외
        "valid_executed":     valid_executed,
        "valid_exec_rate":    round(valid_executed / (ex_total - infra_fails) * 100, 1)
                              if (ex_total - infra_fails) > 0 else 0,
        "infra_fails":        infra_fails,
        # IMPOSSIBLE 거절률
        "impossible_total":   impossible_total,
        "impossible_refused": impossible_refused,
        "impossible_rate":    round(impossible_refused / impossible_total * 100, 1)
                              if impossible_total else 0,
        "error_breakdown":    error_counts,
        "results":            results,
    }
    return summary


def print_summary(summary: dict, model: str = "") -> None:
    label = f" [{model}]" if model else ""
    print("\n" + "=" * 55)
    print(f"모델{label}  총 시나리오: {summary['total']}개")
    print(f"  EX (Execution Accuracy):   {summary['ex_correct']}/{summary['ex_total']} ({summary['ex_rate']}%)")
    print(f"  Valid Execution Rate:       {summary['valid_executed']}/{summary['ex_total'] - summary['infra_fails']} ({summary['valid_exec_rate']}%)")
    print(f"  IMPOSSIBLE 거절률:          {summary['impossible_refused']}/{summary['impossible_total']} ({summary['impossible_rate']}%)")
    if summary["infra_fails"]:
        print(f"  인프라 오류(제외):          {summary['infra_fails']}건")
    print("\n  오류 분류:")
    for err_type, cnt in sorted(summary["error_breakdown"].items(), key=lambda x: -x[1]):
        print(f"    {err_type}: {cnt}건")
    print("=" * 55)


def save_results(summary: dict, out_path: Path | None = None) -> Path:
    if out_path is None:
        out_path = Path(__file__).resolve().parents[1] / "tests" / "eval_results.json"
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path
