# NL2SQL full 시맨틱레이어 — 모델별 생성 쿼리 보고서

- 평가일: 2026-06-19
- 조건: PostgreSQL, full 시맨틱레이어, temperature=0
- 대상 모델: gpt-oss-120b · Qwen3-Coder-Next · DeepSeek V3.2
- 데이터 출처: `tests/comparison_results_20260619.json`

> ⚠️ OpenRouter 경유 모델은 temperature=0에서도 비결정적이라 실행마다 일부 시나리오 결과가 달라질 수 있다. 본 보고서는 2026-06-19 재평가 1회 결과이다.

## 요약

| 모델 | EX | 거절 정확도 |
|------|:--:|:----------:|
| gpt-oss-120b | 9/11 (81.8%) | 0/1 (0.0%) |
| Qwen3-Coder-Next | 10/11 (90.9%) | 0/1 (0.0%) |
| DeepSeek V3.2 | 9/11 (81.8%) | 1/1 (100.0%) |

## 시나리오별 정오 매트릭스

| ID | 유형 | gpt-oss-120b | Qwen3-Coder-Next | DeepSeek V3.2 |
|----|------|:--:|:--:|:--:|
| I-01 | EX | ✅ | ✅ | ✅ |
| I-02 | EX | ✅ | ✅ | ✅ |
| I-03 | EX | ✅ | ✅ | ✅ |
| I-04 | EX | ✅ | ✅ | ✅ |
| I-05 | EX | ✅ | ✅ | ❌ |
| I-06 | EX | ✅ | ✅ | ✅ |
| I-07 | EX | ❌ | ✅ | ✅ |
| I-08 | 거절 | ❌ | ❌ | ✅ |
| IJ-01 | EX | ❌ | ❌ | ❌ |
| IJ-02 | EX | ✅ | ✅ | ✅ |
| IJ-03 | EX | ✅ | ✅ | ✅ |
| IJ-04 | EX | ✅ | ✅ | ✅ |

## 시나리오별 생성 쿼리 상세

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
SELECT ROUND((SUM(it.power_usage))::numeric, 0) AS total_power_usage
FROM industry_type it
WHERE it.year = '2024'
  AND it.month = '03'
  AND it.biz_code = (
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
WITH metro_filter AS (
  SELECT code AS biz_code
  FROM common_code
  WHERE code_type = 'bizCd' AND code_name = '제조업'
)
SELECT
  SUM(power_usage) AS total_power_usage
FROM industry_type t
WHERE t.year = '2024' AND t.month = '03'
  AND t.biz_code = (SELECT biz_code FROM metro_filter)
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
LEFT JOIN common_code b
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
  LEFT JOIN common_code b ON b.code_type = 'bizCd' AND b.code = i.biz_code
  WHERE i.year = '2024'
  GROUP BY i.biz_code, b.code_name
  ORDER BY total_power_usage DESC
  LIMIT 5
)
SELECT 
  biz_code,
  biz_name,
  total_power_usage
FROM industry_usage;
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
    m.code_name AS metro_name,
    SUM(t.power_usage) AS total_power_usage
FROM industry_type t
LEFT JOIN common_code m
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
    SUM(t.power_usage) AS total_power_usage
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
WITH 건설업_코드 AS (
  SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='건설업'
)
SELECT 
  m.code_name AS metro_name,
  SUM(t.power_usage) AS total_power_usage
FROM industry_type t
JOIN 건설업_코드 bc ON t.biz_code = bc.code
JOIN common_code m ON m.code_type='metroCd' AND m.code=t.metro_code
WHERE t.year='2024'
GROUP BY m.code_name
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
         NULLIF(SUM(CASE WHEN year = '2023' THEN power_usage ELSE 0 END), 0))::numeric, 2) AS power_usage_change_pct
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
WITH code_filter AS (
  SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업'
)
SELECT
  ROUND(((y2024.total_usage - y2023.total_usage) * 100.0 / NULLIF(y2023.total_usage, 0))::numeric, 2) AS growth_rate_percent
FROM
  (SELECT SUM(power_usage) AS total_usage FROM industry_type WHERE year='2024' AND biz_code=(SELECT code FROM code_filter)) y2024,
  (SELECT SUM(power_usage) AS total_usage FROM industry_type WHERE year='2023' AND biz_code=(SELECT code FROM code_filter)) y2023;
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

**gpt-oss-120b** — ✅ 정답

```sql
SELECT
  CASE
    WHEN t.month IN ('03','04','05') THEN '봄'
    WHEN t.month IN ('06','07','08') THEN '여름'
    WHEN t.month IN ('09','10','11') THEN '가을'
    ELSE '겨울'
  END AS season,
  b.code_name AS industry_name,
  SUM(t.power_usage) AS total_power_usage
FROM industry_type t
JOIN common_code b
  ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
  AND t.biz_code = (
    SELECT code
    FROM common_code
    WHERE code_type = 'bizCd' AND code_name = '숙박 및 음식점업'
  )
GROUP BY season, b.code_name
ORDER BY season
LIMIT 100;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
WITH seasonal_data AS (
    SELECT 
        CASE 
            WHEN month IN ('03', '04', '05') THEN '봄'
            WHEN month IN ('06', '07', '08') THEN '여름'
            WHEN month IN ('09', '10', '11') THEN '가을'
            WHEN month IN ('12', '01', '02') THEN '겨울'
        END AS season,
        power_usage
    FROM industry_type
    WHERE year = '2024'
      AND biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='숙박 및 음식점업')
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

**DeepSeek V3.2** — ❌ 실행 오류

```sql
WITH biz_filter AS (
  SELECT code AS biz_code
  FROM common_code
  WHERE code_type = 'bizCd' AND code_name = '숙박 및 음식점업'
)
SELECT
  CASE
    WHEN month IN ('03','04','05') THEN '봄'
    WHEN month IN ('06','07','08') THEN '여름'
    WHEN month IN ('09','10','11') THEN '가을'
    WHEN month IN ('12','01','02') THEN '겨울'
  END AS season,
  SUM(power_usage) AS total_power_usage
FROM industry_type t
WHERE year = '2024'
  AND t.biz_code = (SELECT biz_code FROM biz_filter)
GROUP BY
  CASE
    WHEN month IN ('03','04','05') THEN '봄'
    WHEN month IN ('06','07','08') THEN '여름'
    WHEN month IN ('09','10','11') THEN '가을'
    WHEN month IN ('12','01','02') THEN '겨울'
  END
ORDER BY
  CASE season
    WHEN '봄' THEN 1
    WHEN '여름' THEN 2
    WHEN '가을' THEN 3
    WHEN '겨울' THEN 4
  END
```

> 실행 오류: `column "season" does not exist
LINE 25:   CASE season
                ^
`

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
SELECT
    it.biz_code,
    bc.code_name AS industry_name,
    ROUND((SUM(it.bill) / NULLIF(SUM(it.power_usage), 0))::numeric, 4) AS avg_unit_cost
FROM industry_type it
LEFT JOIN common_code bc
    ON bc.code_type = 'bizCd'
   AND bc.code = it.biz_code
WHERE it.year = '2024'
GROUP BY it.biz_code, bc.code_name
ORDER BY avg_unit_cost DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
SELECT 
    t.biz_code,
    b.code_name,
    ROUND((SUM(t.bill) / NULLIF(SUM(t.power_usage), 0))::numeric, 4) AS avg_unit_cost
FROM industry_type t
JOIN common_code b 
    ON b.code_type = 'bizCd' 
    AND b.code = t.biz_code
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
  HAVING SUM(i.power_usage) > 0
)
SELECT
  biz_code,
  biz_name,
  avg_unit_cost
FROM biz_avg
ORDER BY avg_unit_cost DESC
LIMIT 5
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

**gpt-oss-120b** — ❌ 결과 불일치

```sql
WITH metro AS (
    SELECT code AS metro_code
    FROM common_code
    WHERE code_type = 'metroCd' AND code_name = '서울특별시'
),
industry_sum AS (
    SELECT t.biz_code,
           SUM(t.power_usage) AS power_usage
    FROM industry_type t
    JOIN metro m ON t.metro_code = m.metro_code
    GROUP BY t.biz_code
),
total AS (
    SELECT SUM(power_usage) AS total_power
    FROM industry_sum
)
SELECT b.code_name AS industry,
       ROUND(((i.power_usage / NULLIF(t.total_power, 0)) * 100)::numeric, 2) AS usage_share_percent
FROM industry_sum i
CROSS JOIN total t
JOIN common_code b
  ON b.code_type = 'bizCd' AND b.code = i.biz_code
ORDER BY usage_share_percent DESC
LIMIT 100;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
SELECT 
    t.biz_code,
    b.code_name,
    ROUND((SUM(t.power_usage) * 100.0 / NULLIF((SELECT SUM(power_usage) FROM industry_type WHERE year='2024' AND metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')), 0))::numeric, 2) AS usage_share_percent
FROM industry_type t
JOIN common_code b ON b.code_type='bizCd' AND b.code = t.biz_code
WHERE t.year = '2024' 
  AND t.metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
GROUP BY t.biz_code, b.code_name
ORDER BY usage_share_percent DESC;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH metro_filter AS (
  SELECT code AS metro_code
  FROM common_code
  WHERE code_type = 'metroCd' AND code_name = '서울특별시'
)
SELECT
  i.biz_code,
  b.code_name AS biz_name,
  SUM(i.power_usage) AS total_power_usage,
  ROUND((SUM(i.power_usage) * 100.0 / NULLIF(SUM(SUM(i.power_usage)) OVER(), 0))::numeric, 2) AS share_percentage
FROM industry_type i
CROSS JOIN metro_filter mf
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = i.biz_code
WHERE i.year = '2024' AND i.metro_code = mf.metro_code
GROUP BY i.biz_code, b.code_name
ORDER BY total_power_usage DESC
LIMIT 100;
```

---

### I-08. 2024년 제조업의 전국 계약전력 총합은 얼마야?

**정답(gold) SQL**

> IMPOSSIBLE 시나리오 — 조회 불가 질문(거절 대상), gold SQL 없음

**gpt-oss-120b** — ❌ 거절 실패

```sql
SELECT ROUND(SUM(ct.contract_power)::numeric, 0) AS total_contract_power
FROM contract_type ct
JOIN industry_type it
  ON it.year = ct.year
 AND it.month = ct.month
 AND it.metro_code = ct.metro_code
 AND it.city_code = ct.city_code
WHERE ct.year = '2024'
  AND it.biz_code = (
        SELECT code
        FROM common_code
        WHERE code_type = 'bizCd'
          AND code_name = '제조업'
      );
```

**Qwen3-Coder-Next** — ❌ 거절 실패

```sql
SELECT SUM(contract_power) AS total_contract_power
FROM contract_type
WHERE year = '2024'
  AND contract_type IN ('산업용', '일반용')
  AND metro_code IN (
    SELECT code FROM common_code WHERE code_type = 'metroCd'
  );
```

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

**gpt-oss-120b** — ❌ 결과 불일치

```sql
WITH power_2024 AS (
    SELECT it.metro_code,
           SUM(it.power_usage) AS power_2024
    FROM industry_type it
    WHERE it.year = '2024'
      AND it.biz_code = (
          SELECT code
          FROM common_code
          WHERE code_type = 'bizCd' AND code_name = '제조업'
      )
    GROUP BY it.metro_code
),
power_2023 AS (
    SELECT it.metro_code,
           SUM(it.power_usage) AS power_2023
    FROM industry_type it
    WHERE it.year = '2023'
      AND it.biz_code = (
          SELECT code
          FROM common_code
          WHERE code_type = 'bizCd' AND code_name = '제조업'
      )
    GROUP BY it.metro_code
),
increase AS (
    SELECT COALESCE(p24.metro_code, p23.metro_code) AS metro_code,
           COALESCE(p24.power_2024, 0) - COALESCE(p23.power_2023, 0) AS power_increase
    FROM power_2024 p24
    FULL OUTER JOIN power_2023 p23
        ON p24.metro_code = p23.metro_code
),
new_cust AS (
    SELECT icc.metro_code,
           SUM(icc.new_count) AS new_customers
    FROM industry_cust_change icc
    WHERE icc.year = '2024'
      AND icc.biz_code = (
          SELECT code
          FROM common_code
          WHERE code_type = 'bizCd' AND code_name = '제조업'
      )
    GROUP BY icc.metro_code
)
SELECT m.code_name AS metro_name,
       inc.power_increase,
       nc.new_customers
FROM increase inc
JOIN new_cust nc
    ON nc.metro_code = inc.metro_code
JOIN common_code m
    ON m.code_type = 'metroCd' AND m.code = inc.metro_code
ORDER BY inc.power_increase DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ❌ 컬럼 불일치

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
metro_diff AS (
    SELECT 
        m_e.metro_code,
        m_e.total_power_2024,
        COALESCE(m_p.total_power_2023, 0) AS total_power_2023,
        m_e.total_power_2024 - COALESCE(m_p.total_power_2023, 0) AS power_diff
    FROM metro_expansion m_e
    LEFT JOIN metro_prev m_p ON m_e.metro_code = m_p.metro_code
),
top5 AS (
    SELECT 
        metro_code,
        power_diff
    FROM metro_diff
    WHERE total_power_2023 > 0
    ORDER BY power_diff DESC
    LIMIT 5
)
SELECT 
    t.metro_code,
    m.code_name AS metro_name,
    ROUND((t.power_diff)::numeric, 0) AS power_increase,
    COALESCE(ic.new_count, 0) AS new_cust_count
FROM top5 t
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = t.metro_code
LEFT JOIN industry_cust_change ic 
    ON ic.metro_code = t.metro_code 
    AND ic.year = '2024' 
    AND ic.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
ORDER BY t.power_diff DESC;
```

**DeepSeek V3.2** — ❌ 컬럼 불일치

```sql
WITH biz_filter AS (
  SELECT code AS biz_code
  FROM common_code
  WHERE code_type = 'bizCd' AND code_name = '제조업'
),
industry_2024 AS (
  SELECT
    i.metro_code,
    m.code_name AS metro_name,
    SUM(i.power_usage) AS total_power_usage,
    SUM(COALESCE(icc.new_count, 0)) AS new_customer_count
  FROM industry_type i
  JOIN common_code m ON m.code_type = 'metroCd' AND m.code = i.metro_code
  LEFT JOIN industry_cust_change icc
    ON icc.metro_code = i.metro_code
    AND icc.city_code = i.city_code
    AND icc.year = i.year
    AND icc.month = i.month
    AND icc.biz_code = (SELECT biz_code FROM biz_filter)
  CROSS JOIN biz_filter bf
  WHERE i.year = '2024'
    AND i.biz_code = bf.biz_code
  GROUP BY i.metro_code, m.code_name
),
industry_2023 AS (
  SELECT
    i.metro_code,
    SUM(i.power_usage) AS total_power_usage
  FROM industry_type i
  CROSS JOIN biz_filter bf
  WHERE i.year = '2023'
    AND i.biz_code = bf.biz_code
  GROUP BY i.metro_code
)
SELECT
  i24.metro_code,
  i24.metro_name,
  i24.total_power_usage AS power_usage_2024,
  COALESCE(i23.total_power_usage, 0) AS power_usage_2023,
  (i24.total_power_usage - COALESCE(i23.total_power_usage, 0)) AS power_increase,
  i24.new_customer_count
FROM industry_2024 i24
LEFT JOIN industry_2023 i23 ON i23.metro_code = i24.metro_code
ORDER BY power_increase DESC
LIMIT 5;
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
    SELECT it.metro_code,
           SUM(it.power_usage) AS manuf_power
    FROM industry_type it
    WHERE it.year = '2024'
      AND it.biz_code = (
          SELECT code
          FROM common_code
          WHERE code_type = 'bizCd' AND code_name = '제조업'
      )
    GROUP BY it.metro_code
),
house AS (
    SELECT ha.metro_code,
           SUM(ha.power_usage * ha.house_count) / NULLIF(SUM(ha.house_count), 0) AS house_avg_power
    FROM house_avg ha
    WHERE ha.year = '2024'
    GROUP BY ha.metro_code
)
SELECT m.metro_code,
       mc.code_name AS metro_name,
       m.manuf_power,
       h.house_avg_power,
       ROUND((m.manuf_power / NULLIF(h.house_avg_power, 0))::numeric, 4) AS ratio
FROM manuf m
JOIN house h ON h.metro_code = m.metro_code
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
metro_house AS (
    SELECT 
        h.metro_code,
        SUM(h.power_usage * h.house_count) / NULLIF(SUM(h.house_count), 0) AS avg_power_per_house
    FROM house_avg h
    WHERE h.year = '2024'
    GROUP BY h.metro_code
)
SELECT 
    m.code_name AS metro_name,
    ROUND((mp.industry_power_usage / NULLIF(mh.avg_power_per_house, 0))::numeric, 2) AS ratio
FROM metro_power mp
JOIN metro_house mh ON mp.metro_code = mh.metro_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = mp.metro_code;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH
manufacturing_usage AS (
    SELECT
        it.metro_code,
        SUM(it.power_usage) AS total_power_usage
    FROM industry_type it
    WHERE it.year = '2024'
      AND it.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY it.metro_code
),
household_avg_usage AS (
    SELECT
        ha.metro_code,
        SUM(ha.power_usage * ha.house_count) / NULLIF(SUM(ha.house_count), 0) AS avg_power_usage
    FROM house_avg ha
    WHERE ha.year = '2024'
    GROUP BY ha.metro_code
)
SELECT
    m.code_name AS metro_name,
    ROUND((mu.total_power_usage / NULLIF(hau.avg_power_usage, 0))::numeric, 2) AS ratio
FROM manufacturing_usage mu
LEFT JOIN household_avg_usage hau ON mu.metro_code = hau.metro_code
LEFT JOIN common_code m ON m.code_type = 'metroCd' AND m.code = mu.metro_code
ORDER BY ratio DESC NULLS LAST
LIMIT 100;
```

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
WITH industrial AS (
    SELECT
        metro_code,
        SUM(contract_power) AS total_contract_power
    FROM contract_type
    WHERE year = '2024'
      AND contract_type = '산업용'
    GROUP BY metro_code
),
renew AS (
    SELECT
        metro_code,
        MAX(area_capacity) AS total_renew_capacity
    FROM renew_energy
    WHERE year = '2024'
    GROUP BY metro_code
)
SELECT
    m.code_name AS metro_name,
    i.total_contract_power,
    r.total_renew_capacity,
    ROUND(((r.total_renew_capacity * 100.0) / NULLIF(i.total_contract_power, 0))::numeric, 2) AS ratio_percent
FROM industrial i
LEFT JOIN renew r
    ON r.metro_code = i.metro_code
LEFT JOIN common_code m
    ON m.code_type = 'metroCd' AND m.code = i.metro_code
ORDER BY ratio_percent DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
WITH industry_power AS (
    SELECT 
        metro_code,
        SUM(contract_power) AS total_industry_power
    FROM contract_type
    WHERE year = '2024' AND contract_type = '산업용'
    GROUP BY metro_code
),
renew_capacity AS (
    SELECT 
        metro_code,
        MAX(area_capacity) AS total_renew_capacity
    FROM renew_energy
    WHERE year = '2024'
    GROUP BY metro_code
)
SELECT 
    m.code_name AS metro_name,
    ROUND((COALESCE(r.total_renew_capacity, 0) * 100.0 / NULLIF(i.total_industry_power, 0))::numeric, 2) AS ratio_percent
FROM industry_power i
LEFT JOIN renew_capacity r ON i.metro_code = r.metro_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = i.metro_code
WHERE i.total_industry_power IS NOT NULL AND i.total_industry_power > 0
ORDER BY ratio_percent DESC
LIMIT 5;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH metro_contract AS (
  SELECT 
    metro_code,
    SUM(contract_power) AS total_contract_power
  FROM contract_type
  WHERE year = '2024'
    AND contract_type = '산업용'
  GROUP BY metro_code
),
metro_renew AS (
  SELECT 
    metro_code,
    MAX(area_capacity) AS total_renew_capacity
  FROM renew_energy
  WHERE year = '2024'
  GROUP BY metro_code
)
SELECT 
  m.code_name AS metro_name,
  COALESCE(c.total_contract_power, 0) AS contract_power,
  COALESCE(r.total_renew_capacity, 0) AS renew_capacity,
  ROUND((COALESCE(r.total_renew_capacity, 0) * 100.0 / NULLIF(COALESCE(c.total_contract_power, 0), 0))::numeric, 2) AS ratio_percent
FROM common_code m
LEFT JOIN metro_contract c ON c.metro_code = m.code
LEFT JOIN metro_renew r ON r.metro_code = m.code
WHERE m.code_type = 'metroCd'
  AND c.total_contract_power > 0
ORDER BY ratio_percent DESC NULLS LAST
LIMIT 5;
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

**gpt-oss-120b** — ✅ 정답

```sql
WITH top_cities AS (
    SELECT
        it.metro_code,
        it.city_code,
        SUM(it.cust_count) AS total_cust
    FROM industry_type it
    WHERE it.year = '2024'
      AND it.biz_code = (
          SELECT code
          FROM common_code
          WHERE code_type = 'bizCd' AND code_name = '제조업'
      )
    GROUP BY it.metro_code, it.city_code
    ORDER BY total_cust DESC
    LIMIT 5
)
SELECT
    m.code_name AS metro_name,
    c.code_name AS city_name,
    SUM(CASE WHEN bt.bill_type IN ('이메일','모바일','카카오') THEN bt.bill_count ELSE 0 END) AS electronic_count,
    SUM(CASE WHEN bt.bill_type = '우편' THEN bt.bill_count ELSE 0 END) AS paper_count
FROM billing_type bt
JOIN top_cities tc
    ON bt.metro_code = tc.metro_code
   AND bt.city_code = tc.city_code
LEFT JOIN common_code m
    ON m.code_type = 'metroCd' AND m.code = bt.metro_code
LEFT JOIN common_code c
    ON c.code_type = 'cityCd' AND c.code = bt.city_code AND c.upper_code = bt.metro_code
WHERE bt.year = '2024'
GROUP BY m.code_name, c.code_name
ORDER BY electronic_count DESC
LIMIT 5;
```

**Qwen3-Coder-Next** — ✅ 정답

```sql
WITH metro_biz AS (
    SELECT 
        t.metro_code,
        t.city_code,
        SUM(t.cust_count) AS total_cust
    FROM industry_type t
    WHERE t.year = '2024' 
      AND t.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY t.metro_code, t.city_code
),
top5_cities AS (
    SELECT 
        metro_code,
        city_code
    FROM metro_biz
    ORDER BY total_cust DESC
    LIMIT 5
)
SELECT 
    c.code_name AS city_name,
    SUM(CASE WHEN b.bill_type IN ('이메일','모바일','카카오') THEN b.bill_count ELSE 0 END) AS electronic_count,
    SUM(CASE WHEN b.bill_type = '우편' THEN b.bill_count ELSE 0 END) AS paper_count
FROM billing_type b
JOIN top5_cities t ON b.metro_code = t.metro_code AND b.city_code = t.city_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = b.city_code AND c.upper_code = b.metro_code
WHERE b.year = '2024'
GROUP BY c.code_name
ORDER BY 
    (SUM(CASE WHEN b.bill_type IN ('이메일','모바일','카카오') THEN b.bill_count ELSE 0 END) +
     SUM(CASE WHEN b.bill_type = '우편' THEN b.bill_count ELSE 0 END)) DESC;
```

**DeepSeek V3.2** — ✅ 정답

```sql
WITH 제조업_코드 AS (
  SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업'
),
제조업_고객_집계 AS (
  SELECT
    i.metro_code,
    i.city_code,
    SUM(i.cust_count) AS total_cust
  FROM industry_type i
  WHERE i.year = '2024'
    AND i.biz_code = (SELECT code FROM 제조업_코드)
  GROUP BY i.metro_code, i.city_code
  ORDER BY total_cust DESC
  LIMIT 5
)
SELECT
  m.code_name AS metro_name,
  c.code_name AS city_name,
  SUM(CASE WHEN b.bill_type IN ('이메일','모바일','카카오') THEN b.bill_count ELSE 0 END) AS electronic_count,
  SUM(CASE WHEN b.bill_type = '우편' THEN b.bill_count ELSE 0 END) AS paper_count
FROM 제조업_고객_집계 t
JOIN common_code m ON m.code_type='metroCd' AND m.code=t.metro_code
JOIN common_code c ON c.code_type='cityCd' AND c.code=t.city_code AND c.upper_code=t.metro_code
LEFT JOIN billing_type b ON b.metro_code=t.metro_code AND b.city_code=t.city_code AND b.year='2024'
GROUP BY t.metro_code, t.city_code, m.code_name, c.code_name
ORDER BY t.metro_code, t.city_code;
```

---
