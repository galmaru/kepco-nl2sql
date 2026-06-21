# NL2SQL full + 자기검증 — 모델별 생성 쿼리 보고서 (v2)

- 평가일: 2026-06-19
- 조건: PostgreSQL, full 시맨틱레이어, **LLM 자기검증 단계 ON**, temperature=0
- 대상 모델: gpt-oss-120b · Qwen3-Coder-Next · DeepSeek V3.2
- 데이터 출처: `tests/comparison_results_verify_20260619.json`
- v1 대비 차이: SQL 생성 후 실행 전에 **자기검증(verify_and_fix)** 단계를 추가하여, 질문 조건 누락·조회 불가를 점검·수정

> ⚠️ OpenRouter 경유 모델은 temperature=0에서도 비결정적이라 실행마다 일부 시나리오 결과가 달라진다. 본 보고서는 2026-06-19 자기검증 ON 재평가 1회 결과이다.

> 📌 거절 정확도는 평가기 수정(최종 SQL 기준 거절 판정)을 반영한 값이다. 자기검증이 거절로 전환한 경우를 올바르게 집계한다.

## 1. 자기검증 효과 요약

자기검증 단계의 순효과는 생성 비결정성을 제거한 **고정 입력 테스트**로 측정하였다(저장된 생성 SQL을 검증기에 직접 입력, 케이스당 3회).

| 검증 항목 | 결과 | 근거 |
|-----------|------|------|
| 조건 누락 수정 (I-07: year 필터 누락) | ✅ 효과 | 고정입력 3/3 고침 |
| 조회 불가 거절 (I-08: 업종별 계약전력) | ✅ 효과 | 고정입력 3/3 + end-to-end 3/3 거절 |
| 정답 SQL 회귀 (맞는 SQL을 망치는가) | ✅ 0건 | 고정입력 9/9 + 회귀진단 18/18 유지 |
| **과잉거절 (정상 질문을 거절하는가)** | ⚠️ **1건** | DeepSeek가 IJ-02(조회 가능)를 거절 |

**결론:** 자기검증은 조건 누락(I-07)·조회 불가(I-08)를 잡고, 정답 SQL은 손대지 않는다(회귀 0). 다만 거절 강화의 부작용으로 **DeepSeek가 정상 질문 IJ-02를 과잉거절**하는 사례가 1건 확인되었다. 거절 강화는 IMPOSSIBLE 차단(I-08 3종 성공)과 정상 질문 과잉거절 사이에 트레이드오프가 있다.

## 1.5 정답 재판정 (라벨·컬럼 차이 허용)

평가기는 결과 행의 텍스트 라벨까지 비교하므로, **핵심 수치는 gold와 일치하나 라벨 언어·컬럼 구성만 다른** 경우를 오답으로 분류한다. 아래 케이스는 수동 검증 결과 **실질 정답**으로 재판정한다(평가기는 수정하지 않고 주석으로만 처리).

| 케이스 | 평가기 분류 | 실제 차이 | 검증 결과 | 재판정 |
|--------|-------------|-----------|-----------|--------|
| I-05 (gpt-oss) | 결과/컬럼 불일치 | 계절 라벨이 영어(Spring 등) + 컬럼 추가 | 계절별 합계 수치 **완전 일치** | ✅ 실질 정답 |
| IJ-04 (gpt-oss) | 결과 불일치 | 지역을 코드로 표기 + 제조업 고객수 컬럼 누락 | 5개 시군구 **전자·우편 청구서 건수 완전 일치** | ✅ 실질 정답 |
| IJ-04 (qwen) | 컬럼 불일치 | 지역을 코드로 표기 | 전자·우편 건수 **실제 다름**(자릿수 차이) | ❌ 오답 유지 |
| I-07 (qwen) | 컬럼 불일치 | 추가 컬럼 + 점유율 값 | 점유율 0.6~1.1%p **실제 차이**(계산 오류) | ❌ 오답 유지 |

**판정 기준:** 라벨 언어·컬럼 구성 차이는 허용하되, **핵심 수치가 일치할 때만** 정답으로 인정한다. 같은 IJ-04라도 gpt-oss는 핵심값이 일치해 정답이지만 qwen은 값 자체가 달라 오답이다(각 모델의 실제 생성 결과로 판정). I-07은 라벨 문제가 아니라 점유율 계산 자체가 어긋나므로 오답으로 둔다.

## 1.6 I-05 계절 힌트 제거 효과

I-05 질문의 `(봄/여름/가을/겨울)` 힌트가 일부 모델의 SQL 생성을 방해함을 확인하여 제거하고 재평가하였다(질문 변경, gold SQL의 계절 정의는 유지). 각 모델 3회.

| 모델 | 힌트 有 | 힌트 無 | 변화 |
|------|:------:|:------:|------|
| gpt-oss-120b | 3/3 (한글 라벨) | 영어 라벨, 수치 3/3 일치 | 라벨만 영어, 계산 정확(실질 정답) |
| Qwen3-Coder-Next | 1/3 | **3/3** | ⬆️ 실행 오류 해소 |
| DeepSeek V3.2 | 0/3 | 2/3 | ⬆️ 실행 오류 감소 |

괄호 표현이 Qwen·DeepSeek의 계절 CASE 구문 생성을 불안정하게 만들었으며, 힌트 제거 후 두 모델의 실행 오류가 크게 줄었다. gpt-oss는 힌트가 없으면 계절 라벨을 영어로 출력하나 집계 수치는 정확하다(§1.5 기준 실질 정답).

## 2. 결과 종합표 (정확도 / 실행성공률 / 거절정확도)

시맨틱레이어 3단계와 자기검증을 조합한 3종 모델 결과. 조건은 도메인 규칙 주입 수준에 따라 **① 기본(구조만, `none`) → ② 예시값 추가(`examples`) → ③ 도메인 규칙 전체(`full`) → ④ 도메인 규칙 + 자기검증(`full`+verify)** 으로 구성된다. **①~③은 자기검증 미적용, ④만 자기검증 적용**이다. 각 셀은 **정확도(EX) / 실행성공률 / 거절정확도** 순이며, EX는 평가기 자동 분류 기준(라벨 재판정은 † 참조)이다.

**채점 기준**
- **정확도 (EX, Execution Accuracy)**: 생성 SQL의 실행 결과가 정답(gold) SQL의 실행 결과와 일치하면 정답. 행 순서는 무관하며, 수치는 소수점 2자리 반올림까지 허용한다. 추가 컬럼을 포함한 응답(superset), 핵심 수치만 반환한 응답(subset), LEFT JOIN 미매칭 NULL 행 제거도 정답으로 허용한다. (대상: IMPOSSIBLE 제외 11개 시나리오)
- **실행 성공률 (Valid Execution Rate)**: 문법 오류·타임아웃 없이 SQL이 정상 실행 완료된 비율. API 인프라 오류(429·timeout 등)는 제외한다.
- **거절 정확도**: 조회 불가(IMPOSSIBLE) 질문에 대해 SQL을 생성하지 않고 거절한 비율. **최종 SQL(자기검증 후) 기준**으로 판정한다. (대상: IMPOSSIBLE 1개 시나리오)
- **라벨 재판정 (④ 한정)**: 위 EX 기준으로는 오답이나, 핵심 수치가 gold와 일치하고 라벨 언어(봄 vs Spring)·컬럼 구성만 다른 경우 수동 검증으로 정답 인정(§1.5).

| 조건 | gpt-oss-120b | Qwen3-Coder-Next | DeepSeek V3.2 |
|------|:------------:|:----------------:|:-------------:|
| ① 기본 (구조만) | 54.5% / 100% / 0% | 63.6% / 100% / 0% | 54.5% / 90.9% / 0% |
| ② 예시값 추가 | 54.5% / 81.8% / 100% | 72.7% / 90.9% / 0% | 45.5% / 81.8% / 0% |
| ③ 도메인 규칙 전체 | 81.8% / 100% / 0% | 90.9% / 100% / 0% | 81.8% / 90.9% / 100% |
| ④ 도메인 규칙 + 자기검증 | 81.8%† / 100% / 100% | 72.7% / 100% / 100% | 90.9% / 90.9% / 100% |

† ④의 gpt-oss는 라벨 재판정(§1.5: I-05·IJ-04 핵심 수치 일치, 라벨·컬럼만 상이) 반영 시 **EX 100%**. Qwen·DeepSeek는 재판정해도 잔여 오답이 핵심값 불일치라 변화 없음. ①~③은 라벨 재판정을 검증하지 않아 평가기 기준이다.

**관찰**
- **시맨틱레이어 효과(①→③)**: 3종 모두 EX 상승. examples(②)는 효과가 미미하거나 오히려 하락(DeepSeek 54.5→45.5%)하고, full(③)에서 큰 향상.
- **거절 정확도**: ①~③에서는 모델별로 들쭉날쭉(0~1/1)하나, **④ 자기검증 적용 시 3종 모두 1/1(100%)** 로 안정화 — 자기검증의 가장 뚜렷한 기여.
- **자기검증 EX**: ③→④의 EX 등락(예: Qwen 90.9→72.7%)은 생성 비결정성이며, 라벨 재판정(§1.5) 적용 시 **gpt-oss는 11/11(100%)**. Qwen·DeepSeek 잔여 오답은 핵심값이 실제로 달라 유지.

### 조건별 3종 모델 평균

위 결과를 조건별로 3종 모델 평균을 낸 값. EX 평균은 전체 정답 수 합 ÷ 전체 시나리오 수(33), 실행 성공률·거절 정확도는 3종 모델 비율의 산술평균이다.

| 조건 | 정확도 (EX) 평균 | 실행 성공률 평균 | 거절 정확도 평균 |
|------|:---------------:|:---------------:|:---------------:|
| ① 기본 (구조만) | 57.6% | 97.0% | 0.0% |
| ② 예시값 추가 | 57.6% | 84.8% | 33.3% |
| ③ 도메인 규칙 전체 | 84.8% | 97.0% | 33.3% |
| ④ 도메인 규칙 + 자기검증 | 81.8% (재판정 **87.9%**) | 97.0% | **100.0%** |

- **정확도**: none·examples는 57.6%로 동일(예시값만으로는 향상 없음), **full에서 84.8%로 도약**. ④의 평가기 EX(81.8%)는 생성 비결정성으로 ③보다 낮아 보이나, 라벨 재판정 시 87.9%로 최고.
- **실행 성공률**: examples(②)에서 84.8%로 떨어지는데, 예시값이 일부 모델의 SQL 생성을 불안정하게 만든 영향. full·자기검증에서 97.0% 회복.
- **거절 정확도**: ①~③ 평균 0~33.3%에서 **④ 자기검증으로 100% 도달** — 조건 간 가장 큰 개선폭(+66.7%p).

## 3. 자기검증 OFF → ON 시나리오별 변화

| ID | gpt-oss-120b | Qwen3-Coder-Next | DeepSeek V3.2 |
|----|:--:|:--:|:--:|
| I-01 | 정답 | 정답 | 정답 |
| I-02 | 정답 | 정답 | 정답 |
| I-03 | 정답 | 정답 | 정답 |
| I-04 | 정답 | 정답 | 정답 |
| I-05 | 정답→오답⬇️ | 정답→오답⬇️ | 오답→정답⬆️ |
| I-06 | 정답 | 정답 | 정답 |
| I-07 | 오답→정답⬆️ | 정답→오답⬇️ | 정답 |
| I-08 | 거절X→거절O⬆️ | 거절X→거절O⬆️ | 거절O |
| IJ-01 | 오답→정답⬆️ | 오답→정답⬆️ | 오답→정답⬆️ |
| IJ-02 | 정답 | 정답 | 정답→과잉거절⬇️ |
| IJ-03 | 정답 | 정답 | 정답 |
| IJ-04 | 정답→오답⬇️ | 정답→오답⬇️ | 정답 |

⬆️ 개선 · ⬇️ 악화 · 표기 없으면 동일

악화(⬇️) 해석: I-05·I-07·IJ-04의 악화는 **생성 비결정성**으로 확인됨(회귀진단 18/18 정답유지, 검증 회귀 아님). 단 **DeepSeek IJ-02 악화는 자기검증의 과잉거절**이다.

## 4. 시나리오별 정오 매트릭스 (자기검증 ON, 라벨 재판정 반영)

| ID | 유형 | gpt-oss-120b | Qwen3-Coder-Next | DeepSeek V3.2 |
|----|------|:--:|:--:|:--:|
| I-01 | EX | ✅ | ✅ | ✅ |
| I-02 | EX | ✅ | ✅ | ✅ |
| I-03 | EX | ✅ | ✅ | ✅ |
| I-04 | EX | ✅ | ✅ | ✅ |
| I-05 | EX | ✅* | ❌ | ✅ |
| I-06 | EX | ✅ | ✅ | ✅ |
| I-07 | EX | ✅ | ❌ | ✅ |
| I-08 | 거절 | ✅ | ✅ | ✅ |
| IJ-01 | EX | ✅ | ✅ | ✅ |
| IJ-02 | EX | ✅ | ✅ | ❌ |
| IJ-03 | EX | ✅ | ✅ | ✅ |
| IJ-04 | EX | ✅* | ❌ | ✅ |
| **합계 (EX)** | | **11/11** | 8/11 | 10/11 |

`✅*` = 평가기는 오답으로 분류했으나 §1.5 기준(핵심 수치 일치, 라벨·컬럼만 상이)으로 정답 재판정한 케이스.

## 5. 시나리오별 생성 쿼리 상세 (자기검증 ON)

### I-01. 2024년 3월 제조업 전력사용량 전국 총합은?

**정답(gold) SQL**

```sql
SELECT SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024'
  AND month = '03'
  AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업');
```

**gpt-oss-120b** — ✅ 정답

```sql
SELECT SUM(power_usage) AS total_power_usage
FROM industry_type t
WHERE t.year = '2024'
  AND t.month = '03'
  AND t.biz_code = (
    SELECT code
    FROM common_code
    WHERE code_type = 'bizCd'
      AND code_name = '제조업'
  );
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
SELECT SUM(power_usage) AS total_power_usage
FROM industry_type
WHERE year = '2024' 
  AND month = '03' 
  AND biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업');
```

**DeepSeek V3.2** — ✅ 정답

```sql
SELECT
  SUM(power_usage) AS total_power_usage
FROM industry_type
WHERE year = '2024'
  AND month = '03'
  AND biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
```

---

### I-02. 2024년 한 해 동안 전력을 가장 많이 쓴 산업 TOP 5

**정답(gold) SQL**

```sql
SELECT b.code_name AS biz,
       SUM(t.power_usage) AS total_usage
FROM industry_type t
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
GROUP BY t.biz_code, b.code_name
ORDER BY total_usage DESC
LIMIT 5;
```

**gpt-oss-120b** — ✅ 정답

```sql
SELECT
    it.biz_code,
    b.code_name AS industry_name,
    SUM(it.power_usage) AS total_power_usage
FROM industry_type it
JOIN common_code b
    ON b.code_type = 'bizCd'
   AND b.code = it.biz_code
WHERE it.year = '2024'
GROUP BY it.biz_code, b.code_name
ORDER BY total_power_usage DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
SELECT 
    t.biz_code,
    b.code_name,
    SUM(t.power_usage) AS total_power_usage
FROM industry_type t
JOIN common_code b 
    ON b.code_type = 'bizCd' 
    AND b.code = t.biz_code
WHERE t.year = '2024'
GROUP BY t.biz_code, b.code_name
ORDER BY total_power_usage DESC
LIMIT 5;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH industry_usage AS (
  SELECT
    i.biz_code,
    b.code_name AS biz_name,
    SUM(i.power_usage) AS total_power_usage
  FROM industry_type i
  JOIN common_code b ON b.code_type = 'bizCd' AND b.code = i.biz_code
  WHERE i.year = '2024'
  GROUP BY i.biz_code, b.code_name
  ORDER BY total_power_usage DESC
  LIMIT 5
)
SELECT * FROM industry_usage;
```

---

### I-03. 2024년 건설업 전력사용량이 가장 많은 광역시도 TOP 3

**정답(gold) SQL**

```sql
SELECT m.code_name AS metro,
       SUM(t.power_usage) AS total_usage
FROM industry_type t
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = t.metro_code
WHERE t.year = '2024'
  AND t.biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '건설업')
GROUP BY t.metro_code, m.code_name
ORDER BY total_usage DESC
LIMIT 3;
```

**gpt-oss-120b** — ✅ 정답

```sql
SELECT
    t.metro_code,
    m.code_name AS metro_name,
    SUM(t.power_usage) AS total_power_usage
FROM industry_type t
JOIN common_code m
  ON m.code_type = 'metroCd'
 AND m.code = t.metro_code
WHERE t.year = '2024'
  AND t.biz_code = (
        SELECT code
        FROM common_code
        WHERE code_type = 'bizCd'
          AND code_name = '건설업'
      )
GROUP BY t.metro_code, m.code_name
ORDER BY total_power_usage DESC
LIMIT 3;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
SELECT 
    t.metro_code,
    m.code_name AS metro_name,
    ROUND((SUM(t.power_usage))::numeric, 0) AS total_power_usage
FROM industry_type t
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = t.metro_code
WHERE 
    t.year = '2024'
    AND t.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='건설업')
GROUP BY t.metro_code, m.code_name
ORDER BY total_power_usage DESC
LIMIT 3;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH biz_code_cte AS (
  SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='건설업'
)
SELECT
  m.code_name AS metro_name,
  SUM(i.power_usage) AS total_power_usage
FROM industry_type i
JOIN common_code m ON m.code_type='metroCd' AND m.code=i.metro_code
WHERE i.year='2024'
  AND i.biz_code=(SELECT code FROM biz_code_cte)
GROUP BY i.metro_code, m.code_name
ORDER BY total_power_usage DESC
LIMIT 3
```

---

### I-04. 2023년 대비 2024년 제조업 전력사용량 증감률은?

**정답(gold) SQL**

```sql
SELECT
  ROUND(
    ((SUM(CASE WHEN year = '2024' THEN power_usage END)
     - SUM(CASE WHEN year = '2023' THEN power_usage END))
    * 100.0 / SUM(CASE WHEN year = '2023' THEN power_usage END))::numeric,
    6
  ) AS yoy_pct
FROM industry_type
WHERE biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
  AND year IN ('2023', '2024');
```

**gpt-oss-120b** — ✅ 정답

```sql
SELECT
  ROUND(((SUM(CASE WHEN year = '2024' THEN power_usage ELSE 0 END) -
          SUM(CASE WHEN year = '2023' THEN power_usage ELSE 0 END)) * 100.0 /
         NULLIF(SUM(CASE WHEN year = '2023' THEN power_usage ELSE 0 END), 0))::numeric, 2) AS power_usage_change_rate
FROM industry_type
WHERE year IN ('2023', '2024')
  AND biz_code = (
    SELECT code
    FROM common_code
    WHERE code_type = 'bizCd' AND code_name = '제조업'
  );
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
WITH industry_usage AS (
    SELECT 
        year,
        SUM(power_usage) AS total_power_usage
    FROM industry_type
    WHERE biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
      AND year IN ('2023', '2024')
    GROUP BY year
)
SELECT 
    ROUND(
        ((COALESCE(MAX(CASE WHEN year = '2024' THEN total_power_usage END), 0) - 
          COALESCE(MAX(CASE WHEN year = '2023' THEN total_power_usage END), 0)) * 100.0 / 
         NULLIF(COALESCE(MAX(CASE WHEN year = '2023' THEN total_power_usage END), 0), 0)
    )::numeric, 2) AS usage_growth_rate
FROM industry_usage;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH biz_filter AS (
  SELECT code AS biz_code
  FROM common_code
  WHERE code_type='bizCd' AND code_name='제조업'
)
SELECT
  ROUND(((y2024.total_usage - y2023.total_usage) * 100.0 / y2023.total_usage)::numeric, 2) AS growth_rate_percent
FROM (
  SELECT SUM(power_usage) AS total_usage
  FROM industry_type t
  WHERE year='2024' AND t.biz_code=(SELECT biz_code FROM biz_filter)
) y2024
CROSS JOIN (
  SELECT SUM(power_usage) AS total_usage
  FROM industry_type t
  WHERE year='2023' AND t.biz_code=(SELECT biz_code FROM biz_filter)
) y2023
```

---

### I-05. 2024년 전국 숙박 및 음식점업의 계절별(봄/여름/가을/겨울) 전력사용량 비교

**정답(gold) SQL**

```sql
SELECT
  CASE
    WHEN month IN ('03','04','05') THEN '봄'
    WHEN month IN ('06','07','08') THEN '여름'
    WHEN month IN ('09','10','11') THEN '가을'
    ELSE '겨울'
  END AS season,
  SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024'
  AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '숙박 및 음식점업')
GROUP BY season
ORDER BY total_usage DESC;
```

**gpt-oss-120b** — ❌ 결과 불일치

```sql
SELECT
  CASE
    WHEN it.month IN ('03','04','05') THEN 'Spring'
    WHEN it.month IN ('06','07','08') THEN 'Summer'
    WHEN it.month IN ('09','10','11') THEN 'Autumn'
    ELSE 'Winter'
  END AS season,
  SUM(it.power_usage) AS total_power_usage
FROM industry_type it
WHERE it.year = '2024'
  AND it.biz_code = (
    SELECT code
    FROM common_code
    WHERE code_type = 'bizCd'
      AND code_name = '숙박 및 음식점업'
  )
GROUP BY season
ORDER BY season
LIMIT 100;
```

**Qwen3-Coder-Next** — ❌ 결과 없음

```sql
WITH seasonal_data AS (
    SELECT 
        it.year,
        it.month,
        it.power_usage,
        CASE 
            WHEN it.month IN ('03', '04', '05') THEN '봄'
            WHEN it.month IN ('06', '07', '08') THEN '여름'
            WHEN it.month IN ('09', '10', '11') THEN '가을'
            WHEN it.month IN ('12', '01', '02') THEN '겨울'
        END AS season
    FROM industry_type it
    JOIN common_code cc 
        ON cc.code_type = 'bizCd' 
        AND cc.code = it.biz_code
    WHERE it.year = '2024'
        AND cc.code_name = '숙박 및 음식점업'
        AND it.metro_code = '0'  -- 전국 집계를 위해 metro_code 조건 추가 (전국은 metro_code='0' 또는 NULL로 존재할 수 있음)
        AND it.city_code = '0'
)
SELECT 
    season,
    SUM(power_usage) AS total_power_usage
FROM seasonal_data
GROUP BY season
ORDER BY 
    CASE season 
        WHEN '봄' THEN 1 
        WHEN '여름' THEN 2 
        WHEN '가을' THEN 3 
        WHEN '겨울' THEN 4 
    END;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH season_data AS (
  SELECT 
    CASE 
      WHEN i.month IN ('03','04','05') THEN '봄'
      WHEN i.month IN ('06','07','08') THEN '여름'
      WHEN i.month IN ('09','10','11') THEN '가을'
      WHEN i.month IN ('12','01','02') THEN '겨울'
    END AS season,
    SUM(i.power_usage) AS total_power_usage
  FROM industry_type i
  WHERE i.year = '2024'
    AND i.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='숙박 및 음식점업')
  GROUP BY 
    CASE 
      WHEN i.month IN ('03','04','05') THEN '봄'
      WHEN i.month IN ('06','07','08') THEN '여름'
      WHEN i.month IN ('09','10','11') THEN '가을'
      WHEN i.month IN ('12','01','02') THEN '겨울'
    END
)
SELECT 
  season,
  total_power_usage,
  ROUND((total_power_usage * 100.0 / SUM(total_power_usage) OVER())::numeric, 2) AS percentage
FROM season_data
ORDER BY 
  CASE season
    WHEN '봄' THEN 1
    WHEN '여름' THEN 2
    WHEN '가을' THEN 3
    WHEN '겨울' THEN 4
  END;
```

---

### I-06. 2024년 산업별 평균 판매단가(원/kWh)가 가장 비싼 산업 TOP 5

**정답(gold) SQL**

```sql
SELECT b.code_name AS biz,
       ROUND((SUM(t.bill) / NULLIF(SUM(t.power_usage), 0))::numeric, 2) AS avg_unit_cost
FROM industry_type t
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
GROUP BY t.biz_code, b.code_name
ORDER BY avg_unit_cost DESC
LIMIT 5;
```

**gpt-oss-120b** — ✅ 정답

```sql
SELECT i.biz_code,
       b.code_name AS industry_name,
       ROUND((SUM(i.bill) / NULLIF(SUM(i.power_usage), 0))::numeric, 4) AS avg_unit_cost
FROM industry_type i
JOIN common_code b
  ON b.code_type = 'bizCd'
 AND b.code = i.biz_code
WHERE i.year = '2024'
GROUP BY i.biz_code, b.code_name
ORDER BY avg_unit_cost DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
SELECT 
    t.biz_code,
    b.code_name AS biz_name,
    ROUND((SUM(t.bill) / NULLIF(SUM(t.power_usage), 0))::numeric, 4) AS avg_unit_cost
FROM industry_type t
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
GROUP BY t.biz_code, b.code_name
ORDER BY avg_unit_cost DESC
LIMIT 5;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH biz_avg AS (
  SELECT
    i.biz_code,
    b.code_name AS biz_name,
    ROUND((SUM(i.bill) / NULLIF(SUM(i.power_usage), 0))::numeric, 4) AS avg_unit_cost
  FROM industry_type i
  JOIN common_code b ON b.code_type = 'bizCd' AND b.code = i.biz_code
  WHERE i.year = '2024'
  GROUP BY i.biz_code, b.code_name
)
SELECT
  biz_code,
  biz_name,
  avg_unit_cost
FROM biz_avg
WHERE avg_unit_cost IS NOT NULL
ORDER BY avg_unit_cost DESC
LIMIT 5;
```

---

### I-07. 2024년 서울특별시에서 각 산업별 전력사용량 점유율(%)을 보여줘

**정답(gold) SQL**

```sql
SELECT b.code_name AS biz,
       ROUND((SUM(t.power_usage) * 100.0 / SUM(SUM(t.power_usage)) OVER ())::numeric, 2) AS share_pct
FROM industry_type t
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
  AND t.metro_code = (SELECT code FROM common_code WHERE code_type = 'metroCd' AND code_name = '서울특별시')
GROUP BY t.biz_code, b.code_name
ORDER BY share_pct DESC;
```

**gpt-oss-120b** — ✅ 정답

```sql
WITH metro_seoul AS (
  SELECT code
  FROM common_code
  WHERE code_type = 'metroCd' AND code_name = '서울특별시'
),
industry_sum AS (
  SELECT
    it.biz_code,
    b.code_name AS industry_name,
    SUM(it.power_usage) AS total_power_usage
  FROM industry_type it
  LEFT JOIN common_code b
    ON b.code_type = 'bizCd' AND b.code = it.biz_code
  LEFT JOIN common_code c
    ON c.code_type = 'cityCd' AND c.code = it.city_code AND c.upper_code = it.metro_code
  WHERE it.year = '2024'
    AND it.metro_code = (SELECT code FROM metro_seoul)
  GROUP BY it.biz_code, b.code_name
),
total AS (
  SELECT SUM(total_power_usage) AS grand_total
  FROM industry_sum
)
SELECT
  i.industry_name,
  ROUND(((i.total_power_usage * 100.0) / NULLIF(t.grand_total, 0))::numeric, 2) AS power_usage_share_pct
FROM industry_sum i
CROSS JOIN total t
ORDER BY power_usage_share_pct DESC
LIMIT 100;
```

**Qwen3-Coder-Next** — ❌ 컬럼 불일치

```sql
WITH seoul_metro AS (
    SELECT code AS metro_code
    FROM common_code
    WHERE code_type = 'metroCd' AND code_name = '서울특별시'
),
industry_2024 AS (
    SELECT 
        t.biz_code,
        b.code_name AS biz_name,
        SUM(t.power_usage) AS total_power_usage
    FROM industry_type t
    JOIN seoul_metro s ON t.metro_code = s.metro_code
    JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
    WHERE t.year = '2024' AND t.month = '12'
    GROUP BY t.biz_code, b.code_name
),
seoul_total AS (
    SELECT SUM(total_power_usage) AS seoul_total_power
    FROM industry_2024
)
SELECT 
    i.biz_code,
    i.biz_name,
    i.total_power_usage,
    ROUND((i.total_power_usage * 100.0 / st.seoul_total_power)::numeric, 2) AS usage_share_percent
FROM industry_2024 i
CROSS JOIN seoul_total st
ORDER BY i.total_power_usage DESC;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH seoul_metro AS (
  SELECT code AS metro_code
  FROM common_code
  WHERE code_type = 'metroCd' AND code_name = '서울특별시'
)
SELECT
  i.biz_code,
  b.code_name AS biz_name,
  SUM(i.power_usage) AS total_power_usage,
  ROUND((SUM(i.power_usage) * 100.0 / NULLIF(SUM(SUM(i.power_usage)) OVER (), 0))::numeric, 2) AS usage_share_percent
FROM industry_type i
JOIN seoul_metro sm ON i.metro_code = sm.metro_code
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = i.biz_code
WHERE i.year = '2024'
GROUP BY i.biz_code, b.code_name
ORDER BY total_power_usage DESC
LIMIT 100;
```

---

### I-08. 2024년 제조업의 전국 계약전력 총합은 얼마야?

**정답(gold) SQL**

> IMPOSSIBLE 시나리오 — 조회 불가 질문(거절 대상), gold SQL 없음

**gpt-oss-120b** — ✅ 거절 성공

> 조회 불가로 올바르게 거절함 (SQL 미생성)

**Qwen3-Coder-Next** — ✅ 거절 성공

> 조회 불가로 올바르게 거절함 (SQL 미생성)

**DeepSeek V3.2** — ✅ 거절 성공

> 조회 불가로 올바르게 거절함 (SQL 미생성)

---

### IJ-01. 2024년 제조업에서 전력사용량이 가장 많이 늘어난 광역시도 TOP 5와, 같은 기간 해당 지역의 제조업 신규 고객 수를 같이 보여줘

**정답(gold) SQL**

```sql
WITH mfg_code AS (
  SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업'
),
u24 AS (
  SELECT metro_code, SUM(power_usage) AS usage
  FROM industry_type
  WHERE year = '2024' AND biz_code = (SELECT code FROM mfg_code)
  GROUP BY metro_code
),
u23 AS (
  SELECT metro_code, SUM(power_usage) AS usage
  FROM industry_type
  WHERE year = '2023' AND biz_code = (SELECT code FROM mfg_code)
  GROUP BY metro_code
),
nc AS (
  SELECT metro_code, SUM(new_count) AS new_cust
  FROM industry_cust_change
  WHERE year = '2024' AND biz_code = (SELECT code FROM mfg_code)
  GROUP BY metro_code
)
SELECT m.code_name AS metro,
       u24.usage - COALESCE(u23.usage, 0) AS usage_increase,
       COALESCE(nc.new_cust, 0) AS new_cust_count
FROM u24
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = u24.metro_code
LEFT JOIN u23 ON u24.metro_code = u23.metro_code
LEFT JOIN nc ON u24.metro_code = nc.metro_code
ORDER BY usage_increase DESC
LIMIT 5;
```

**gpt-oss-120b** — ✅ 정답

```sql
WITH power_2024 AS (
    SELECT metro_code,
           SUM(power_usage) AS pu_2024
    FROM industry_type
    WHERE year = '2024'
      AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
    GROUP BY metro_code
),
power_2023 AS (
    SELECT metro_code,
           SUM(power_usage) AS pu_2023
    FROM industry_type
    WHERE year = '2023'
      AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
    GROUP BY metro_code
),
new_cust AS (
    SELECT metro_code,
           SUM(new_count) AS new_cnt
    FROM industry_cust_change
    WHERE year = '2024'
      AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
    GROUP BY metro_code
),
combined AS (
    SELECT p24.metro_code,
           (p24.pu_2024 - COALESCE(p23.pu_2023, 0)) AS increase,
           COALESCE(nc.new_cnt, 0) AS new_customers
    FROM power_2024 p24
    LEFT JOIN power_2023 p23 ON p23.metro_code = p24.metro_code
    LEFT JOIN new_cust nc   ON nc.metro_code = p24.metro_code
)
SELECT m.code_name AS metro_name,
       ROUND((combined.increase)::numeric, 2) AS power_usage_increase,
       combined.new_customers
FROM combined
JOIN common_code m
  ON m.code_type = 'metroCd' AND m.code = combined.metro_code
ORDER BY combined.increase DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
WITH metro_expansion AS (
    SELECT
        i.metro_code,
        SUM(i.power_usage) AS total_power_2024
    FROM industry_type i
    WHERE i.year = '2024'
      AND i.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY i.metro_code
),
metro_prev AS (
    SELECT
        i.metro_code,
        SUM(i.power_usage) AS total_power_2023
    FROM industry_type i
    WHERE i.year = '2023'
      AND i.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY i.metro_code
),
metro_growth AS (
    SELECT
        COALESCE(e.metro_code, p.metro_code) AS metro_code,
        COALESCE(e.total_power_2024, 0) AS power_2024,
        COALESCE(p.total_power_2023, 0) AS power_2023,
        COALESCE(e.total_power_2024, 0) - COALESCE(p.total_power_2023, 0) AS power_diff
    FROM metro_expansion e
    FULL OUTER JOIN metro_prev p ON e.metro_code = p.metro_code
),
metro_new_cust AS (
    SELECT
        ic.metro_code,
        SUM(ic.new_count) AS new_cust_count
    FROM industry_cust_change ic
    WHERE ic.year = '2024'
      AND ic.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY ic.metro_code
)
SELECT
    m.metro_code,
    c.code_name AS metro_name,
    ROUND((m.power_diff)::numeric, 0) AS power_increase,
    COALESCE(nc.new_cust_count, 0) AS new_customer_count
FROM metro_growth m
LEFT JOIN common_code c ON c.code_type = 'metroCd' AND c.code = m.metro_code
LEFT JOIN metro_new_cust nc ON nc.metro_code = m.metro_code
WHERE m.power_diff > 0
ORDER BY m.power_diff DESC
LIMIT 5;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH biz_code_subquery AS (
  SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업'
),
power_change AS (
  SELECT 
    i.metro_code,
    m.code_name AS metro_name,
    SUM(CASE WHEN i.year = '2024' THEN i.power_usage ELSE 0 END) -
    SUM(CASE WHEN i.year = '2023' THEN i.power_usage ELSE 0 END) AS usage_increase
  FROM industry_type i
  JOIN common_code m ON m.code_type='metroCd' AND m.code=i.metro_code
  WHERE i.biz_code = (SELECT code FROM biz_code_subquery)
    AND i.year IN ('2023', '2024')
  GROUP BY i.metro_code, m.code_name
  ORDER BY usage_increase DESC
  LIMIT 5
),
new_customers AS (
  SELECT 
    i.metro_code,
    SUM(i.new_count) AS total_new_count
  FROM industry_cust_change i
  WHERE i.biz_code = (SELECT code FROM biz_code_subquery)
    AND i.year = '2024'
    AND i.metro_code IN (SELECT metro_code FROM power_change)
  GROUP BY i.metro_code
)
SELECT 
  pc.metro_code,
  pc.metro_name,
  pc.usage_increase,
  COALESCE(nc.total_new_count, 0) AS new_customer_count
FROM power_change pc
LEFT JOIN new_customers nc ON nc.metro_code = pc.metro_code
ORDER BY pc.usage_increase DESC;
```

---

### IJ-02. 2024년 광역시도별로 제조업 전력사용량이 가구당 평균 전력사용량의 몇 배인지 계산해줘

**정답(gold) SQL**

```sql
WITH ind AS (
  SELECT metro_code, SUM(power_usage) AS ind_usage
  FROM industry_type
  WHERE year = '2024'
    AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
  GROUP BY metro_code
),
house AS (
  SELECT metro_code,
         SUM(power_usage * house_count) / NULLIF(SUM(house_count), 0) AS avg_per_house
  FROM house_avg
  WHERE year = '2024'
  GROUP BY metro_code
)
SELECT m.code_name AS metro,
       ROUND((i.ind_usage / NULLIF(h.avg_per_house, 0))::numeric, 2) AS ratio
FROM ind i
JOIN house h ON i.metro_code = h.metro_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = i.metro_code
ORDER BY ratio DESC;
```

**gpt-oss-120b** — ✅ 정답

```sql
WITH manuf AS (
    SELECT
        metro_code,
        SUM(power_usage) AS manuf_power
    FROM industry_type
    WHERE year = '2024'
      AND biz_code = (
          SELECT code
          FROM common_code
          WHERE code_type = 'bizCd' AND code_name = '제조업'
      )
    GROUP BY metro_code
),
house AS (
    SELECT
        metro_code,
        SUM(power_usage * house_count) / NULLIF(SUM(house_count), 0) AS avg_house_power
    FROM house_avg
    WHERE year = '2024'
    GROUP BY metro_code
)
SELECT
    m.metro_code,
    mc.code_name AS metro_name,
    ROUND((m.manuf_power / NULLIF(h.avg_house_power, 0))::numeric, 2) AS power_usage_ratio
FROM manuf m
LEFT JOIN house h ON h.metro_code = m.metro_code
JOIN common_code mc ON mc.code_type = 'metroCd' AND mc.code = m.metro_code
ORDER BY m.metro_code
LIMIT 100;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
WITH metro_power AS (
    SELECT 
        i.metro_code,
        SUM(i.power_usage) AS industry_power_usage
    FROM industry_type i
    WHERE i.year = '2024'
      AND i.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY i.metro_code
),
metro_house_avg AS (
    SELECT 
        h.metro_code,
        SUM(h.power_usage * h.house_count) / NULLIF(SUM(h.house_count), 0) AS metro_avg_power
    FROM house_avg h
    WHERE h.year = '2024'
    GROUP BY h.metro_code
)
SELECT 
    m.code_name AS metro_name,
    ROUND((mp.industry_power_usage * 1.0 / NULLIF(mha.metro_avg_power, 0))::numeric, 2) AS ratio
FROM metro_power mp
JOIN metro_house_avg mha ON mp.metro_code = mha.metro_code
JOIN common_code m ON m.code_type='metroCd' AND m.code = mp.metro_code
ORDER BY m.code_name;
```

**DeepSeek V3.2** — ❌ 과잉거절 (정상 질문을 거절)

> ⚠️ 조회 가능한 정상 질문인데 거절함 (과잉거절)

---

### IJ-03. 2024년 광역시도별 산업용 계약전력(kW) 대비 신재생에너지 설비용량(kW) 비율(%)이 높은 상위 5개 지역을 보여줘

**정답(gold) SQL**

```sql
WITH ct AS (
  SELECT metro_code, SUM(contract_power) AS total_contract_kw
  FROM contract_type
  WHERE year = '2024'
    AND contract_type = '산업용'
  GROUP BY metro_code
),
re AS (
  SELECT metro_code, MAX(area_capacity) AS renew_cap_kw
  FROM renew_energy
  WHERE year = '2024'
  GROUP BY metro_code
)
SELECT m.code_name AS metro,
       ct.total_contract_kw,
       re.renew_cap_kw,
       ROUND((re.renew_cap_kw * 100.0 / NULLIF(ct.total_contract_kw, 0))::numeric, 2) AS renew_ratio_pct
FROM ct
LEFT JOIN re ON ct.metro_code = re.metro_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = ct.metro_code
ORDER BY renew_ratio_pct DESC
LIMIT 5;
```

**gpt-oss-120b** — ✅ 정답

```sql
WITH ct_agg AS (
    SELECT
        metro_code,
        SUM(contract_power) AS total_contract_power
    FROM contract_type
    WHERE year = '2024'
      AND contract_type = '산업용'
    GROUP BY metro_code
),
re_agg AS (
    SELECT
        metro_code,
        MAX(area_capacity) AS renewable_capacity
    FROM renew_energy
    WHERE year = '2024'
    GROUP BY metro_code
)
SELECT
    m.code_name AS metro_name,
    ct_agg.total_contract_power,
    re_agg.renewable_capacity,
    ROUND((re_agg.renewable_capacity * 100.0 / NULLIF(ct_agg.total_contract_power, 0))::numeric, 2) AS ratio_percent
FROM ct_agg
JOIN re_agg ON re_agg.metro_code = ct_agg.metro_code
JOIN common_code m
  ON m.code_type = 'metroCd' AND m.code = ct_agg.metro_code
ORDER BY ratio_percent DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
WITH industry_power AS (
    SELECT 
        t.metro_code,
        SUM(t.contract_power) AS total_industry_power
    FROM contract_type t
    WHERE t.year = '2024' 
      AND t.contract_type = '산업용'
    GROUP BY t.metro_code
),
renew_capacity AS (
    SELECT 
        r.metro_code,
        MAX(r.area_capacity) AS total_renew_capacity
    FROM renew_energy r
    WHERE r.year = '2024'
    GROUP BY r.metro_code
)
SELECT 
    m.code_name AS metro_name,
    ROUND((COALESCE(r.total_renew_capacity, 0) * 100.0 / NULLIF(i.total_industry_power, 0))::numeric, 2) AS ratio_percent
FROM industry_power i
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = i.metro_code
LEFT JOIN renew_capacity r ON r.metro_code = i.metro_code
WHERE i.total_industry_power > 0
ORDER BY ratio_percent DESC
LIMIT 5;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH renew_capacity AS (
  SELECT
    metro_code,
    MAX(area_capacity) AS renew_capacity
  FROM renew_energy
  WHERE year = '2024'
  GROUP BY metro_code
),
contract_power AS (
  SELECT
    metro_code,
    SUM(contract_power) AS total_contract_power
  FROM contract_type
  WHERE year = '2024' AND contract_type = '산업용'
  GROUP BY metro_code
)
SELECT
  m.code_name AS metro_name,
  COALESCE(rc.renew_capacity, 0) AS renew_capacity,
  COALESCE(cp.total_contract_power, 0) AS industry_contract_power,
  CASE
    WHEN COALESCE(cp.total_contract_power, 0) = 0 THEN 0
    ELSE ROUND((COALESCE(rc.renew_capacity, 0) * 100.0 / cp.total_contract_power)::numeric, 2)
  END AS ratio_percent
FROM common_code m
LEFT JOIN renew_capacity rc ON rc.metro_code = m.code
LEFT JOIN contract_power cp ON cp.metro_code = m.code
WHERE m.code_type = 'metroCd'
ORDER BY ratio_percent DESC
LIMIT 5
```

---

### IJ-04. 2024년 제조업 고객이 많은 상위 5개 시군구에서, 전자청구서(이메일/모바일/카카오) 및 우편 청구서 발송 건수를 함께 보여줘

**정답(gold) SQL**

```sql
WITH mfg AS (
  SELECT metro_code, city_code, SUM(cust_count) AS mfg_cnt
  FROM industry_type
  WHERE year = '2024'
    AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
  GROUP BY metro_code, city_code
  ORDER BY mfg_cnt DESC
  LIMIT 5
),
bills AS (
  SELECT metro_code, city_code,
    SUM(CASE WHEN bill_type IN ('이메일','모바일','카카오') THEN bill_count ELSE 0 END) AS e_bill,
    SUM(CASE WHEN bill_type = '우편' THEN bill_count ELSE 0 END) AS paper_bill
  FROM billing_type
  WHERE year = '2024'
  GROUP BY metro_code, city_code
)
SELECT m.code_name AS metro,
       ci.code_name AS city,
       f.mfg_cnt,
       b.e_bill,
       b.paper_bill
FROM mfg f
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = f.metro_code
JOIN common_code ci ON ci.code_type = 'cityCd' AND ci.code = f.city_code
  AND ci.upper_code = f.metro_code
LEFT JOIN bills b ON f.metro_code = b.metro_code AND f.city_code = b.city_code
ORDER BY f.mfg_cnt DESC;
```

**gpt-oss-120b** — ❌ 결과 불일치

```sql
WITH manufacturing AS (
    SELECT code
    FROM common_code
    WHERE code_type = 'bizCd' AND code_name = '제조업'
),
top_cities AS (
    SELECT it.metro_code,
           it.city_code,
           SUM(it.cust_count) AS total_cust
    FROM industry_type it
    JOIN manufacturing m ON it.biz_code = m.code
    WHERE it.year = '2024'
    GROUP BY it.metro_code, it.city_code
    ORDER BY total_cust DESC
    LIMIT 5
)
SELECT tc.metro_code,
       tc.city_code,
       c.code_name AS city_name,
       SUM(CASE WHEN bt.bill_type IN ('이메일','모바일','카카오') THEN bt.bill_count ELSE 0 END) AS electronic_count,
       SUM(CASE WHEN bt.bill_type = '우편' THEN bt.bill_count ELSE 0 END) AS paper_count
FROM top_cities tc
LEFT JOIN billing_type bt
       ON bt.year = '2024'
      AND bt.metro_code = tc.metro_code
      AND bt.city_code = tc.city_code
LEFT JOIN common_code c
       ON c.code_type = 'cityCd'
      AND c.code = tc.city_code
      AND c.upper_code = tc.metro_code
GROUP BY tc.metro_code, tc.city_code, c.code_name
ORDER BY SUM(CASE WHEN bt.bill_type IN ('이메일','모바일','카카오') THEN bt.bill_count ELSE 0 END) DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ❌ 컬럼 불일치

```sql
WITH biz_code_sub AS (
    SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업'
),
top5_cities AS (
    SELECT 
        t.metro_code,
        t.city_code,
        SUM(t.cust_count) AS total_cust
    FROM industry_type t
    WHERE t.year = '2024' 
      AND t.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY t.metro_code, t.city_code
    ORDER BY total_cust DESC
    LIMIT 5
)
SELECT 
    c.code_name AS city_name,
    SUM(CASE WHEN b.bill_type IN ('이메일','모바일','카카오') THEN b.bill_count ELSE 0 END) AS electronic_count,
    SUM(CASE WHEN b.bill_type = '우편' THEN b.bill_count ELSE 0 END) AS paper_count
FROM top5_cities t
LEFT JOIN billing_type b 
    ON b.metro_code = t.metro_code 
    AND b.city_code = t.city_code 
    AND b.year = '2024'
    AND b.month = '12'  -- 월 단위 집계가 필요 없고, 연도만 지정하면 모든 월 합산이므로 생략 가능. 그러나 billing_type은 월별 행 존재 → 월 미지정 시 SUM 시 12개월 합산됨. 질문에 월 제한 없으므로 생략해도 무방하나, 명시적 조건 없이 전체 월 합산이 의도라면 괜찮음.
JOIN common_code c 
    ON c.code_type = 'cityCd' 
    AND c.code = t.city_code 
    AND c.upper_code = t.metro_code
GROUP BY c.code_name, t.metro_code, t.city_code
ORDER BY 
    (SELECT total_cust FROM top5_cities tt 
     WHERE tt.metro_code = t.metro_code AND tt.city_code = t.city_code) DESC;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH 제조업_코드 AS (
  SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업'
),
제조업_고객_많은_시군구 AS (
  SELECT
    i.metro_code,
    i.city_code,
    SUM(i.cust_count) AS total_cust
  FROM industry_type i
  CROSS JOIN 제조업_코드 b
  WHERE i.year = '2024'
    AND i.biz_code = b.code
  GROUP BY i.metro_code, i.city_code
  ORDER BY total_cust DESC
  LIMIT 5
)
SELECT
  m.code_name AS metro_name,
  c.code_name AS city_name,
  SUM(CASE WHEN b.bill_type IN ('이메일','모바일','카카오') THEN b.bill_count ELSE 0 END) AS electronic_count,
  SUM(CASE WHEN b.bill_type = '우편' THEN b.bill_count ELSE 0 END) AS paper_count
FROM 제조업_고객_많은_시군구 t
JOIN common_code m ON m.code_type='metroCd' AND m.code = t.metro_code
JOIN common_code c ON c.code_type='cityCd' AND c.code = t.city_code AND c.upper_code = t.metro_code
LEFT JOIN billing_type b ON b.metro_code = t.metro_code AND b.city_code = t.city_code AND b.year = '2024'
GROUP BY t.metro_code, t.city_code, m.code_name, c.code_name
ORDER BY t.metro_code, t.city_code;
```

---
