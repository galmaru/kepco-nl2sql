"""Phase 3 NL2SQL 파이프라인 통합 실행."""
from __future__ import annotations

from typing import Any

from . import classifier, normalizer, schema, generator, safety, executor


def run(question: str) -> dict[str, Any]:
    """
    자연어 질문 → SQL 생성 → 실행까지 전체 파이프라인.

    Returns:
        {
            "question":  str,
            "tables":    list[str],
            "sql":       str,
            "execution": dict,
            "error":     str | None,
            "steps":     dict,   # 각 단계 중간 결과
        }
    """
    result: dict[str, Any] = {
        "question":  question,
        "tables":    [],
        "sql":       "",
        "execution": {},
        "error":     None,
        "steps":     {},
    }

    # [3-1] 테이블 선택
    tables = classifier.classify(question)
    result["tables"] = tables
    result["steps"]["classifier"] = tables

    # [3-2] 코드 정규화
    norm = normalizer.normalize(question)
    result["steps"]["normalizer"] = norm

    # 스키마 컨텍스트 생성
    schema_ctx = schema.build_context(tables)
    result["steps"]["schema_length"] = len(schema_ctx)

    # [3-3] LLM SQL 생성
    gen = generator.generate(question, schema_ctx, norm["hint_text"])
    result["steps"]["generator"] = {"raw_length": len(gen["raw"]), "error": gen["error"]}

    if gen["error"]:
        result["error"] = f"SQL 생성 실패: {gen['error']}"
        return result

    sql = gen["sql"]
    result["sql"] = sql

    # [3-4] 안전성 검사
    ok, reason = safety.check(sql)
    result["steps"]["safety"] = {"ok": ok, "reason": reason}

    if not ok:
        result["error"] = f"안전성 검사 실패: {reason}"
        return result

    # [3-5] 실행
    exec_result = executor.execute(sql)
    result["execution"] = exec_result

    if not exec_result["success"]:
        result["error"] = f"실행 오류: {exec_result['error']}"

    return result


def run_scenario(scenario: dict) -> dict[str, Any]:
    """evaluator에서 호출하는 래퍼."""
    return run(scenario["question"])
