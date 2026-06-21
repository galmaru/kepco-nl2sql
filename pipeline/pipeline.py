"""Phase 3 NL2SQL 파이프라인 통합 실행."""
from __future__ import annotations

from typing import Any

from . import normalizer, semantic_builder as schema, generator, safety, executor


def run(question: str, model: str | None = None, semantic: str = "full",
        verify: bool = False) -> dict[str, Any]:
    """
    자연어 질문 → SQL 생성 → 실행까지 전체 파이프라인.

    Args:
        semantic: "none" | "examples" | "full" — 시맨틱 레이어 수준
        verify: True면 SQL 생성 후 실행 전에 LLM 자기검증 단계를 거침

    Returns:
        {
            "question":  str,
            "sql":       str,
            "execution": dict,
            "error":     str | None,
            "infra_error": bool,
            "steps":     dict,
        }
    """
    result: dict[str, Any] = {
        "question":    question,
        "sql":         "",
        "execution":   {"success": False, "rows": [], "columns": [], "error": None},
        "error":       None,
        "infra_error": False,
        "steps":       {},
    }

    # [3-1] 코드 정규화
    norm = normalizer.normalize(question)
    result["steps"]["normalizer"] = norm

    # 전체 스키마 컨텍스트 생성 (semantic 레벨 적용)
    schema_ctx = schema.build_full_context(semantic=semantic)
    result["steps"]["schema_length"] = len(schema_ctx)
    result["steps"]["semantic"] = semantic
    result["steps"]["schema_ctx"] = schema_ctx

    # [3-2] LLM SQL 생성
    gen_kwargs: dict[str, Any] = {}
    if model:
        gen_kwargs["model"] = model
    gen = generator.generate(question, schema_ctx, norm["hint_text"], **gen_kwargs)
    result["steps"]["generator"] = {"raw_length": len(gen["raw"]), "error": gen["error"]}
    result["steps"]["prompt_system"] = gen.get("prompt_system", "")
    result["steps"]["prompt_user"]   = gen.get("prompt_user", "")
    result["infra_error"] = gen.get("infra_error", False)

    if gen["error"]:
        result["error"] = f"SQL 생성 실패: {gen['error']}"
        return result

    sql = gen["sql"]
    result["steps"]["raw"] = gen["raw"]

    # [3-2.5] 자기검증 (선택): 질문 조건 반영 여부 점검 후 1회 수정
    if verify and sql:
        ver = generator.verify_and_fix(question, schema_ctx, sql, norm["hint_text"], **gen_kwargs)
        result["steps"]["verify"] = {
            "changed": ver["changed"],
            "error":   ver["error"],
            "raw":     ver.get("raw", ""),
        }
        if not ver["error"]:
            sql = ver["sql"]

    result["sql"] = sql

    # [3-3] 안전성 검사
    ok, reason = safety.check(sql)
    result["steps"]["safety"] = {"ok": ok, "reason": reason}

    if not ok:
        result["error"] = f"안전성 검사 실패: {reason}"
        return result

    # [3-4] 실행
    exec_result = executor.execute(sql)
    result["execution"] = exec_result

    if not exec_result["success"]:
        result["error"] = f"실행 오류: {exec_result['error']}"

    return result


def run_scenario(scenario: dict, model: str | None = None, semantic: str = "full",
                 verify: bool = False) -> dict[str, Any]:
    """evaluator에서 호출하는 래퍼."""
    return run(scenario["question"], model=model, semantic=semantic, verify=verify)
