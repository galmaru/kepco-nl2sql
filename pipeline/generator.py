"""[3-3] LLM SQL 생성: OpenRouter를 통해 지정 모델로 SQL 생성."""
from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

MODEL = "openai/gpt-oss-120b"

_SYSTEM = """당신은 PostgreSQL 전문가입니다.
주어진 스키마와 힌트를 사용해 자연어 질문에 대한 PostgreSQL SELECT 쿼리를 작성하세요.

규칙:
1. SELECT 또는 WITH로만 시작하세요.
2. SQL 코드 블록(```sql ... ```)만 반환하고, 설명은 포함하지 마세요.
3. [코드 정규화 힌트]가 있으면 metro_code·biz_code 필터에 반드시 해당 서브쿼리를 사용하세요. 코드값 직접 하드코딩 금지.
4. city JOIN에는 항상 `AND c.upper_code = t.metro_code` 조건을 추가하세요. CTE 내부 서브쿼리에서도 예외 없이 적용.
5. 지역명·업종명을 SELECT에 표시할 때는 common_code를 JOIN하여 code_name을 출력하세요.
6. 집계 쿼리는 GROUP BY를 반드시 포함하세요. SELECT에 포함된 모든 비집계 컬럼을 GROUP BY에 명시하세요. (예: GROUP BY t.biz_code, b.code_name)
7. 전국 집계는 metro 조건 없이 SUM/AVG를 사용하세요 (전체 집계행 없음).
8. 결과 행이 많을 수 있으므로 LIMIT 100 이하를 권장합니다.
9. month 컬럼은 두 자리 문자열('01'~'12')입니다. 한 자리('1','2') 형태 사용 금지.
10. 두 테이블을 결합할 때 한쪽에 데이터가 없을 수 있으면 LEFT JOIN을 사용하세요.
11. 질문에 필요한 테이블 또는 컬럼이 위 스키마에 존재하지 않는 경우에만 SQL을 생성하지 말고, ```sql 블록 없이 "해당 데이터는 스키마에 없어 조회할 수 없습니다."라고만 답하세요. year/month 비교·증감률 계산 등 기간 조회는 항상 가능합니다.
12. ROUND 함수는 반드시 `ROUND((전체수식)::numeric, 자릿수)` 형식으로 사용하세요. PostgreSQL에서 ROUND(double precision, integer)는 지원하지 않습니다.
    - 올바른 예: ROUND((SUM(bill)/NULLIF(SUM(power_usage),0))::numeric, 4)
    - 올바른 예: ROUND(((2024값 - 2023값) * 100.0 / 2023값)::numeric, 2)
    - 잘못된 예: ROUND(SUM(...) * 100\n    ::numeric, 2)  ← ::numeric이 100에만 붙어 전체가 double precision으로 남음
    - 반드시 전체 수식을 괄호로 감싼 후 ::numeric을 붙이세요: ROUND((A / B * 100)::numeric, 2)
13. ROUND 자릿수는 반드시 0 이상의 정수를 사용하세요. 음수 자릿수(-1, -2 등)는 절대 사용 금지입니다. 정수로 반올림할 때는 ROUND((값)::numeric, 0)을 사용하세요.
14. 시군구별 집계(TOP N 포함)는 반드시 GROUP BY metro_code, city_code + SUM/COUNT를 사용하세요. 집계 없이 ORDER BY + LIMIT만 사용하면 같은 시군구가 월별로 중복 선정됩니다.
15. house_avg.power_usage는 이미 가구당 평균값입니다. AVG(power_usage) 재집계 금지. 광역시도 가중평균은 SUM(power_usage * house_count) / NULLIF(SUM(house_count), 0)으로 계산하세요."""


_VERIFY_SYSTEM = """당신은 PostgreSQL SQL 검수자입니다.
자연어 질문과 그에 대해 생성된 SQL을 비교하여, SQL이 질문의 모든 조건을 빠짐없이 반영했는지 점검하세요.

점검 항목:
1. 질문에 명시된 시간 조건(연도, 월, 기간)이 WHERE 절에 모두 반영됐는가
2. 질문에 명시된 지역·업종·유형 등 필터 조건이 반영됐는가
3. 질문이 요구한 집계·계산(점유율·증감률·평균 등)·정렬·개수 제한이 올바른가
4. 질문이 요구한 지표를, 질문이 요구한 분류 기준으로 실제 조회할 수 있는가 (가장 중요)
   - 특정 지표가 한 분류 축에만 존재하고, 질문이 요구하는 다른 축에는 없을 수 있다.
     예: 계약전력(contract_power)은 계약종별(contract_type)에만 있고 KSIC 업종별(industry_type)에는 없다.
         → "제조업(업종)의 계약전력"은 조회 불가.
   - 컬럼이 다른 테이블에 존재한다는 이유로, 분류 기준이 다른 두 테이블을 억지로 JOIN해
     값을 만들어내면 안 된다. 그렇게 만든 값은 질문과 무관한 숫자다.
   - 스키마 힌트에 "~ 컬럼 없음", "~별 ~ 조회 불가" 같은 주의가 있으면 그 조합은 조회 불가로 간주한다.

규칙:
- SQL이 질문을 올바르게 반영했으면 원본 SQL을 그대로 ```sql 블록으로 반환하세요.
- 조건 누락·계산 오류가 있으면 수정한 SQL을 ```sql 블록으로 반환하세요.
- 질문이 요구한 지표를 요구한 분류 기준으로 조회할 수 없으면, 억지로 JOIN하지 말고
  ```sql 블록 없이 "해당 데이터는 스키마에 없어 조회할 수 없습니다."라고만 답하세요.
- 단, 정상적으로 조회 가능한 질문은 거절하지 마세요. 조회 불가가 분명할 때만 거절합니다.
- 설명은 포함하지 말고 SQL 코드 블록만 반환하세요."""


def verify_and_fix(question: str, schema_context: str, sql: str,
                   hint_text: str = "", model: str = MODEL) -> dict:
    """
    [3-2.5] 자기검증: 생성된 SQL이 질문 조건을 모두 반영했는지 LLM으로 점검·수정.

    실행 전 단계로, 질문에 명시된 시간·지역·계산 조건의 누락을 잡는다.

    Returns:
        {"sql": str, "raw": str, "changed": bool, "error": str | None, "infra_error": bool}
    """
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        return {"sql": sql, "raw": "", "changed": False,
                "error": "OPENROUTER_API_KEY 미설정", "infra_error": True}

    client = OpenAI(api_key=api_key, base_url="https://openrouter.ai/api/v1")

    parts = [f"[스키마]\n{schema_context}"]
    if hint_text:
        parts.append(hint_text)
    parts.append(f"[질문]\n{question}")
    parts.append(f"[생성된 SQL]\n{sql}")
    user_message = "\n\n".join(parts)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _VERIFY_SYSTEM},
                {"role": "user",   "content": user_message},
            ],
            temperature=0,
            timeout=60,
        )
        raw = response.choices[0].message.content or ""
        fixed = _extract_sql(raw)
        changed = _normalize_sql(fixed) != _normalize_sql(sql)
        return {"sql": fixed, "raw": raw, "changed": changed,
                "error": None, "infra_error": False}
    except Exception as e:
        err_str = str(e)
        is_infra = any(k in err_str.lower() for k in ("429", "rate limit", "timeout", "timed out", "connection"))
        # 검증 실패 시 원본 SQL 유지 (회귀 방지)
        return {"sql": sql, "raw": "", "changed": False, "error": err_str, "infra_error": is_infra}


def _normalize_sql(sql: str) -> str:
    """공백·줄바꿈 차이를 무시한 SQL 비교용 정규화."""
    return re.sub(r"\s+", " ", (sql or "").strip()).lower()


def _extract_sql(text: str) -> str:
    """응답에서 SQL 코드 블록 추출."""
    m = re.search(r'```(?:sql)?\s*(.*?)```', text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # 코드 블록 없으면 SELECT/WITH로 시작하는 부분 추출
    m2 = re.search(r'((?:SELECT|WITH)\b.*)', text, re.DOTALL | re.IGNORECASE)
    if m2:
        return m2.group(1).strip()
    return text.strip()


def generate(question: str, schema_context: str, hint_text: str = "", model: str = MODEL) -> dict:
    """
    OpenRouter를 통해 SQL 생성.

    Returns:
        {"sql": str, "raw": str, "error": str | None, "infra_error": bool}
        infra_error=True: API 인프라 문제(429/timeout) — 모델 능력과 무관
    """
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        return {"sql": "", "raw": "", "error": "OPENROUTER_API_KEY 미설정", "infra_error": True}

    client = OpenAI(
        api_key=api_key,
        base_url="https://openrouter.ai/api/v1",
    )

    user_parts = [f"[스키마]\n{schema_context}"]
    if hint_text:
        user_parts.append(hint_text)
    user_parts.append(f"[질문]\n{question}")

    user_message = "\n\n".join(user_parts)

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user",   "content": user_message},
            ],
            temperature=0,
            timeout=60,
        )
        raw = response.choices[0].message.content or ""
        sql = _extract_sql(raw)
        return {"sql": sql, "raw": raw, "error": None, "infra_error": False,
                "prompt_system": _SYSTEM, "prompt_user": user_message}
    except Exception as e:
        err_str = str(e)
        is_infra = any(k in err_str.lower() for k in ("429", "rate limit", "timeout", "timed out", "connection"))
        return {"sql": "", "raw": "", "error": err_str, "infra_error": is_infra}
