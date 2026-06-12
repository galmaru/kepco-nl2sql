"""[3-3] LLM SQL 생성: OpenRouter openai/gpt-oss-120b."""
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
4. contract_type 컬럼 값(주택용/일반용/산업용 등)은 텍스트 그대로 사용하세요. 서브쿼리 불필요.
5. city JOIN에는 항상 `AND c.upper_code = t.metro_code` 조건을 추가하세요.
6. 지역명·업종명을 SELECT에 표시할 때는 common_code를 JOIN하여 code_name을 출력하세요.
7. 집계 쿼리는 GROUP BY를 반드시 포함하세요.
8. SQLite는 LEFT() 미지원 → SUBSTR(col,1,N) 사용.
9. 전국 집계는 metro 조건 없이 SUM/AVG를 사용하세요 (전체 집계행 없음).
10. 결과 행이 많을 수 있으므로 LIMIT 100 이하를 권장합니다.
11. month 컬럼은 두 자리 문자열('01'~'12')입니다. 한 자리('1','2') 형태 사용 금지.
12. 평균 판매단가: AVG(unit_cost) 금지 → ROUND(SUM(bill)*1.0/NULLIF(SUM(power_usage),0),4) 사용.
13. 두 테이블을 결합할 때 한쪽에 데이터가 없을 수 있으면 LEFT JOIN을 사용하세요.
14. 급속/완속 합계 조회 시 개별 합계와 전체 합계(total) 컬럼을 모두 포함하세요."""


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


def generate(question: str, schema_context: str, hint_text: str = "") -> dict:
    """
    OpenRouter를 통해 SQL 생성.

    Returns:
        {"sql": str, "raw": str, "error": str | None}
    """
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        return {"sql": "", "raw": "", "error": "OPENROUTER_API_KEY 미설정"}

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
            model=MODEL,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user",   "content": user_message},
            ],
            temperature=0,
            timeout=30,
        )
        raw = response.choices[0].message.content or ""
        sql = _extract_sql(raw)
        return {"sql": sql, "raw": raw, "error": None}
    except Exception as e:
        return {"sql": "", "raw": "", "error": str(e)}
