"""3개 모델 NL2SQL 비교 평가 스크립트.

사용법:
  # 전체 12개 시나리오 3개 모델 비교
  python -m tests.run_model_comparison

  # smoke test: 모델당 1개 시나리오로 슬러그/연결 검증
  python -m tests.run_model_comparison --smoke

  # 특정 모델만 실행
  python -m tests.run_model_comparison --models openai/gpt-oss-120b

결과: tests/comparison_results.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import evaluator, pipeline

MODELS = [
    "openai/gpt-oss-120b",
    "qwen/qwen3-32b",
    "openai/gpt-4o",
]

# smoke test용 시나리오 (간단한 단일 테이블 쿼리)
SMOKE_SCENARIO_ID = "I-01"

OUT_PATH = Path(__file__).resolve().parent / "comparison_results.json"


def make_pipeline_fn(model: str, semantic: str = "full"):
    """특정 모델·시맨틱 레벨로 고정된 pipeline_fn 반환."""
    def fn(scenario: dict) -> dict:
        return pipeline.run_scenario(scenario, model=model, semantic=semantic)
    return fn


def run_smoke(models: list[str], semantic: str = "full") -> None:
    """각 모델로 I-01 1회 실행해서 슬러그·연결·SQL 추출 검증."""
    scenarios = evaluator.load_scenarios()
    smoke = next((s for s in scenarios if s["id"] == SMOKE_SCENARIO_ID), None)
    if not smoke:
        print(f"[오류] smoke 시나리오 {SMOKE_SCENARIO_ID} 없음")
        return

    print(f"\n=== Smoke Test (시나리오: {SMOKE_SCENARIO_ID}, semantic={semantic}) ===")
    for model in models:
        print(f"\n모델: {model}")
        out = pipeline.run_scenario(smoke, model=model, semantic=semantic)
        if out.get("infra_error"):
            print(f"  [인프라 오류] {out.get('error')}")
        elif out.get("error"):
            print(f"  [오류] {out.get('error')}")
        else:
            sql_preview = (out.get("sql") or "")[:120].replace("\n", " ")
            print(f"  [성공] SQL: {sql_preview}...")
            rows = out.get("execution", {}).get("rows", [])
            print(f"  결과 행 수: {len(rows)}")


def run_comparison(models: list[str], semantic: str = "full") -> dict:
    """전체 평가 후 비교 결과 반환."""
    all_summaries: dict[str, dict] = {}

    for model in models:
        key = f"{model}[{semantic}]"
        print(f"\n\n{'='*60}")
        print(f"모델 평가: {model}  (semantic={semantic})")
        print('='*60)

        summary = evaluator.evaluate_all(make_pipeline_fn(model, semantic=semantic))
        evaluator.print_summary(summary, model=key)
        all_summaries[key] = summary

    return all_summaries


def print_comparison_table(summaries: dict) -> None:
    """모델 비교 테이블 출력."""
    print("\n\n" + "="*72)
    print("모델 비교 요약")
    print("="*72)
    header = f"{'모델[semantic]':<35} {'EX':>16} {'Valid%':>8} {'거절률':>10}"
    print(header)
    print("-"*72)
    for key, s in summaries.items():
        ex    = f"{s['ex_correct']}/{s['ex_total']} ({s['ex_rate']}%)"
        valid = f"{s['valid_exec_rate']}%"
        imp   = f"{s['impossible_refused']}/{s['impossible_total']} ({s['impossible_rate']}%)"
        print(f"{key:<35} {ex:>16} {valid:>8} {imp:>10}")
    print("="*72)


def save_results(summaries: dict) -> None:
    """결과를 JSON으로 저장. 기존 파일에 모델별로 병합(append)."""
    # 기존 결과 로드
    existing: dict = {}
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text(encoding="utf-8"))
        except Exception:
            existing = {}

    for model, s in summaries.items():
        entry = {k: v for k, v in s.items() if k != "results"}
        entry["scenario_results"] = [
            {
                "id":            r["id"],
                "question":      r.get("question", ""),
                "correct":       r["correct"],
                "error_type":    r["error_type"],
                "impossible":    r.get("impossible", False),
                "refused":       r.get("refused"),
                "generated_sql": r.get("generated_sql", ""),
                "gold_sql":      r.get("gold_sql", ""),
                "gen_rows":      r.get("gen_rows", 0),
                "gold_rows":     r.get("gold_rows", 0),
                "gen_error":     r.get("gen_error"),
                "infra_error":   r.get("infra_error", False),
            }
            for r in s["results"]
        ]
        existing[model] = entry

    OUT_PATH.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n결과 저장: {OUT_PATH}")


def main() -> None:
    parser = argparse.ArgumentParser(description="NL2SQL 모델 비교 평가")
    parser.add_argument("--smoke",    action="store_true", help="smoke test만 실행")
    parser.add_argument("--models",   nargs="+", default=MODELS, help="평가할 모델 목록")
    parser.add_argument("--semantic", nargs="+", default=["full"],
                        choices=["none", "examples", "full"],
                        help="시맨틱 레이어 수준 (복수 지정 가능)")
    args = parser.parse_args()

    if args.smoke:
        for sem in args.semantic:
            run_smoke(args.models, semantic=sem)
        return

    all_summaries: dict[str, dict] = {}
    for sem in args.semantic:
        summaries = run_comparison(args.models, semantic=sem)
        all_summaries.update(summaries)

    print_comparison_table(all_summaries)
    save_results(all_summaries)


if __name__ == "__main__":
    main()
