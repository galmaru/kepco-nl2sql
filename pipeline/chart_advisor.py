"""차트 유형 자동 추천: 규칙 기반 + LLM 기반."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# 시간 축 후보 컬럼명 패턴
_TIME_RE = re.compile(r"(year|month|date|period|시간|날짜|년도|연도|년|월|일)", re.I)
# 숫자 값 컬럼명 패턴
_NUM_RE  = re.compile(r"(count|usage|kwh|amount|sum|bill|capacity|power|수|량|금|건|율|비|개|가구|용량|실적)", re.I)


def _find_axes(df: pd.DataFrame) -> tuple[str | None, list[str], str | None, bool]:
    """x축, y축, color 컬럼을 추론합니다."""
    cols     = df.columns.tolist()
    num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    str_cols = [c for c in cols if not pd.api.types.is_numeric_dtype(df[c])]

    # x축: 시간 패턴 컬럼 우선 → 첫 번째 문자열 컬럼 → 첫 번째 컬럼
    x = next((c for c in cols if _TIME_RE.search(c)), None)
    if x is None:
        x = str_cols[0] if str_cols else (cols[0] if cols else None)

    # y축: 숫자 컬럼 중 x축 제외
    y = [c for c in num_cols if c != x]

    # year+month 모두 있으면 period로 합치기 → month를 color 후보에서 제외
    combine_period = "year" in df.columns and "month" in df.columns
    exclude = {x, "year", "month"} if combine_period else {x}

    # color: x/시간축 제외한 첫 번째 문자열 컬럼 (카테고리 분류)
    color = next((c for c in str_cols if c not in exclude), None)

    return x, y, color, combine_period


def suggest_rule(df: pd.DataFrame) -> dict:
    """규칙 기반 차트 추천."""
    if df.empty:
        return {"type": "none", "reason": "데이터 없음"}

    x, y, color, combine_period = _find_axes(df)
    if not y:
        return {"type": "none", "reason": "숫자 컬럼 없음"}

    # x축이 시간 패턴이면 line, 그 외 bar
    chart_type = "line" if (x and _TIME_RE.search(x)) else "bar"
    return {
        "type": chart_type,
        "x": x,
        "y": y,
        "color": color,
        "combine_period": combine_period,
        "reason": f"규칙: {x} → {chart_type}" + (f", 분류: {color}" if color else ""),
    }


def suggest_llm(df: pd.DataFrame) -> dict:
    """LLM 기반 차트 추천. 실패 시 규칙 기반으로 폴백."""
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key or df.empty:
        return suggest_rule(df)

    from openai import OpenAI
    cols   = df.columns.tolist()
    sample = df.head(3).to_dict(orient="records")

    # year+month 모두 있으면 period 합치기 안내
    period_hint = ""
    if "year" in cols and "month" in cols:
        period_hint = '\n- year와 month 컬럼이 모두 있으면 x는 "year", combine_period는 true로 설정'

    prompt = f"""다음 쿼리 결과에 가장 적합한 차트 유형을 JSON으로만 답하세요.

컬럼: {cols}
샘플 (3행): {json.dumps(sample, ensure_ascii=False)}

규칙:
- 월별/연도별 시계열 추이 → "line"
- 지역별/업종별 카테고리 비교 → "bar"
- y는 반드시 숫자 컬럼만 포함
- color: 카테고리 분류 컬럼(지역명·업종명·복지유형 등 문자열). 없으면 null{period_hint}

응답 형식 (JSON만, 설명 없이):
{{"type":"bar","x":"컬럼명","y":["숫자컬럼1"],"color":"분류컬럼 또는 null","combine_period":false,"reason":"한 줄 이유"}}"""

    try:
        client = OpenAI(api_key=api_key, base_url="https://openrouter.ai/api/v1")
        resp = client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            timeout=15,
        )
        raw = resp.choices[0].message.content or ""
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group())
            # y가 문자열로 올 경우 리스트로 변환
            if isinstance(parsed.get("y"), str):
                parsed["y"] = [parsed["y"]]
            # 숫자 컬럼만 y에 남김
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            parsed["y"] = [c for c in parsed.get("y", []) if c in num_cols] or num_cols[:1]
            return parsed
    except Exception:
        pass

    return suggest_rule(df)
