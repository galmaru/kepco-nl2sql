"""[3-3] LLM SQL 생성: OpenRouter를 통해 지정 모델로 SQL 생성."""
from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

MODEL = "openai/gpt-oss-120b"

_SYSTEM = """당신은 SQLite 전문가입니다.
주어진 스키마와 힌트를 사용해 자연어 질문에 대한 SQLite SELECT 쿼리를 작성하세요.

규칙:
1. SELECT 또는 WITH로만 시작하세요.
2. SQL 코드 블록(```sql ... ```)만 반환하고, 설명은 포함하지 마세요.
3. [코드 정규화 힌트]가 있으면 metro_code·biz_code 필터에 반드시 해당 서브쿼리를 사용하세요. 코드값 직접 하드코딩 금지.
4. city JOIN에는 항상 `AND c.upper_code = t.metro_code` 조건을 추가하세요. CTE 내부 서브쿼리에서도 예외 없이 적용.
5. 지역명·업종명을 SELECT에 표시할 때는 common_code를 JOIN하여 code_name을 출력하세요.
6. 집계 쿼리는 GROUP BY를 반드시 포함하세요.
7. SQLite는 LEFT() 미지원 → SUBSTR(col,1,N) 사용.
8. 전국 집계는 metro 조건 없이 SUM/AVG를 사용하세요 (전체 집계행 없음).
9. 결과 행이 많을 수 있으므로 LIMIT 100 이하를 권장합니다.
10. month 컬럼은 두 자리 문자열('01'~'12')입니다. 한 자리('1','2') 형태 사용 금지.
11. 두 테이블을 결합할 때 한쪽에 데이터가 없을 수 있으면 LEFT JOIN을 사용하세요.
12. 질문에 필요한 테이블 또는 컬럼이 위 스키마에 존재하지 않는 경우에만 SQL을 생성하지 말고, ```sql 블록 없이 "해당 데이터는 스키마에 없어 조회할 수 없습니다."라고만 답하세요. year/month 비교·증감률 계산 등 기간 조회는 항상 가능합니다."""


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
        return {"sql": sql, "raw": raw, "error": None, "infra_error": False}
    except Exception as e:
        err_str = str(e)
        is_infra = any(k in err_str.lower() for k in ("429", "rate limit", "timeout", "timed out", "connection"))
        return {"sql": "", "raw": "", "error": err_str, "infra_error": is_infra}
