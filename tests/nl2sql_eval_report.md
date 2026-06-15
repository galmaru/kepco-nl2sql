# NL2SQL 평가 리포트

**생성일시**: 2026-06-15 18:29  
**데이터**: `tests/comparison_results.json`

---

## 1. 전체 성능 요약

### 전체 스키마 투입 방식

| 모델 | semantic | EX | Valid% | IMPOSSIBLE 거절 |
|------|----------|----|--------|-----------------|
| gpt-oss-120b | [none] | 6/11 (54.5%) | 90.9% | 1/1 (100.0%) |
| qwen3-32b | [none] | 4/11 (36.4%) | 81.8% | 0/1 (0.0%) |
| gpt-4o | [none] | 3/11 (27.3%) | 63.6% | 0/1 (0.0%) |
| gpt-oss-120b | [examples] | 5/11 (45.5%) | 100.0% | 1/1 (100.0%) |
| qwen3-32b | [examples] | 5/11 (45.5%) | 81.8% | 0/1 (0.0%) |
| gpt-4o | [examples] | 4/11 (36.4%) | 90.9% | 0/1 (0.0%) |
| gpt-oss-120b | [full] | 11/11 (100.0%) | 100.0% | 1/1 (100.0%) |
| qwen3-32b | [full] | 7/11 (63.6%) | 81.8% | 1/1 (100.0%) |
| gpt-4o | [full] | 10/11 (90.9%) | 81.8% | 1/1 (100.0%) |

### Classifier 기반 (레거시)

| 모델 | EX | Valid% | IMPOSSIBLE 거절 |
|------|----|--------|-----------------|
| claude-sonnet-4-6 | 3/11 (27.3%) | 72.7% | 0/1 (0.0%) |
| gpt-oss-120b | 1/11 (9.1%) | 90.9% | 0/1 (0.0%) |
| qwen3-32b | 2/11 (18.2%) | 72.7% | 0/1 (0.0%) |
| gpt-4o | 2/11 (18.2%) | 72.7% | 0/1 (0.0%) |

---

## 2. 시나리오별 결과 매트릭스

> gpt-oss-120b[full], qwen3-32b[full], gpt-4o[full] (semantic=full)

| ID | 질문 요약 | gpt-oss-120b[full] | qwen3-32b[full] | gpt-4o[full] |
|----|-----------| --- | --- | --- |
| I-01 | 2024년 3월 제조업 전력사용량 전국 … | ✅ | ✅ | ✅ |
| I-02 | 2024년 한 해 동안 전력을 가장 많이… | ✅ | ✅ | ✅ |
| I-03 | 2024년 건설업 전력사용량이 가장 많은… | ✅ | ✅ | ✅ |
| I-04 | 2023년 대비 2024년 제조업 전력사… | ✅ | ❌ 결과 불일치 | ✅ |
| I-05 | 2024년 전국 숙박 및 음식점업의 계절… | ✅ | ✅ | ✅ |
| I-06 | 2024년 산업별 평균 판매단가(원/kW… | ✅ | ✅ | ✅ |
| I-07 | 2024년 서울특별시에서 각 산업별 전력… | ✅ | ✅ | ✅ |
| I-08 | 2024년 제조업의 전국 계약전력 총합은… *(IMPOSSIBLE)* | ✅ | ✅ | ✅ |
| IJ-01 | 2024년 제조업에서 전력사용량이 가장 … | ✅ | ❌ 결과 불일치 | ✅ |
| IJ-02 | 2024년 광역시도별로 제조업 전력사용량… | ✅ | ❌ 결과 불일치 | ✅ |
| IJ-03 | 2024년 광역시도별 산업용 계약전력(k… | ✅ | ✅ | ✅ |
| IJ-04 | 2024년 제조업 고객이 많은 상위 5개… | ✅ | ❌ 타임아웃 | ❌ 타임아웃 |

---

## 3. 실패 상세 분석

### I-04 — 2023년 대비 2024년 제조업 전력사용량 증감률은?

<details><summary>Gold SQL</summary>

```sql
SELECT
  SUM(CASE WHEN year = '2023' THEN power_usage END) AS usage_2023,
  SUM(CASE WHEN year = '2024' THEN power_usage END) AS usage_2024,
  ROUND(
    (SUM(CASE WHEN year = '2024' THEN power_usage END)
     - SUM(CASE WHEN year = '2023' THEN power_usage END))
    * 100.0 / SUM(CASE WHEN year = '2023' THEN power_usage END),
    6
  ) AS yoy_pct
FROM industry_type
WHERE biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
  AND year IN ('2023', '2024');
```
</details>

**qwen3-32b[full]** — `결과 불일치`
  (생성 1행 / gold 1행)

```sql
SELECT 
  SUM(CASE WHEN year = '2023' THEN power_usage ELSE 0 END) AS prev_usage,
  SUM(CASE WHEN year = '2024' THEN power_usage ELSE 0 END) AS current_usage,
  ROUND(
    (SUM(CASE WHEN year = '2024' THEN power_usage ELSE 0 END) - SUM(CASE WHEN year = '2023' THEN power_usage ELSE 0 END)) 
    / NULLIF(SUM(CASE WHEN year = '2023' THEN power_usage ELSE 0 END), 0) * 100, 
    2
  ) AS growth_rate
FROM industry_type
WHERE biz_code = 'C'
```


### IJ-01 — 2024년 제조업에서 전력사용량이 가장 많이 늘어난 광역시도 TOP 5와, 같은 기간 해당 지역의 제조업 신규 고객 수를 같이 보여줘

<details><summary>Gold SQL</summary>

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
</details>

**qwen3-32b[full]** — `결과 불일치`
  (생성 5행 / gold 5행)

```sql
WITH industry_growth AS (
    SELECT 
        i1.metro_code,
        SUM(i2.power_usage) - SUM(i1.power_usage) AS usage_increase
    FROM 
        industry_type i1
    JOIN 
        industry_type i2 
        ON i1.metro_code = i2.metro_code 
        AND i1.biz_code = i2.biz_code
    WHERE 
        i1.year = '2023' AND i1.month = '12'
        AND i2.year = '2024' AND i2.month = '12'
        AND i1.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY 
        i1.metro_code
),
new_customers AS (
    SELECT 
        i.metro_code,
        SUM(i.new_count) AS new_customer_count
    FROM 
        industry_cust_change i
    WHERE 
        i.year = '2024'
        AND i.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
    GROUP BY 
        i.metro_code
)
SELECT 
    m.code_name AS metro_name,
    g.usage_increase,
    COALESCE(n.new_customer_count, 0) AS new_customer_count
FROM 
    industry_growth g
JOIN 
    common_code m ON m.code_type = 'metroCd' AND m.code = g.metro_code
LEFT JOIN 
    new_customers n ON n.metro_code = g.metro_code
ORDER BY 
    g.usage_increase DESC
LIMIT 5;
```


### IJ-02 — 2024년 광역시도별로 제조업 전력사용량이 가구당 평균 전력사용량의 몇 배인지 계산해줘

<details><summary>Gold SQL</summary>

```sql
WITH ind AS (
  SELECT metro_code, SUM(power_usage) AS ind_usage
  FROM industry_type
  WHERE year = '2024'
    AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
  GROUP BY metro_code
),
house AS (
  SELECT metro_code, AVG(power_usage) AS avg_per_house
  FROM house_avg
  WHERE year = '2024'
  GROUP BY metro_code
)
SELECT m.code_name AS metro,
       i.ind_usage,
       ROUND(h.avg_per_house, 2) AS avg_house_usage,
       ROUND(i.ind_usage / NULLIF(h.avg_per_house, 0), 1) AS ratio
FROM ind i
JOIN house h ON i.metro_code = h.metro_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = i.metro_code
ORDER BY ratio DESC;
```
</details>

**qwen3-32b[full]** — `결과 불일치`
  (생성 60행 / gold 5행)

```sql
WITH industry_data AS (
    SELECT 
        i.year,
        i.month,
        m.code AS metro_code,
        m.code_name AS metro_name,
        SUM(i.power_usage) AS total_industry_usage
    FROM industry_type i
    JOIN common_code m ON m.code_type = 'metroCd' AND m.code = i.metro_code
    WHERE i.year = '2024' AND i.biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
    GROUP BY i.year, i.month, m.code, m.code_name
),
house_data AS (
    SELECT 
        h.year,
        h.month,
        m.code AS metro_code,
        m.code_name AS metro_name,
        SUM(h.power_usage * h.house_count) / SUM(h.house_count) AS avg_house_usage
    FROM house_avg h
    JOIN common_code m ON m.code_type = 'metroCd' AND m.code = h.metro_code
    WHERE h.year = '2024'
    GROUP BY h.year, h.month, m.code, m.code_name
)
SELECT 
    i.year,
    i.month,
    i.metro_name,
    ROUND(i.total_industry_usage / NULLIF(h.avg_house_usage, 0), 2) AS usage_ratio
FROM industry_data i
JOIN house_data h ON i.year = h.year AND i.month = h.month AND i.metro_code = h.metro_code
ORDER BY i.year, i.month, i.metro_name
LIMIT 100;
```


### IJ-04 — 2024년 제조업 고객이 많은 상위 5개 시군구에서, 전자청구서(이메일/모바일/카카오) 및 우편 청구서 발송 건수를 함께 보여줘

<details><summary>Gold SQL</summary>

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
</details>

**qwen3-32b[full]** — `타임아웃`

> 실행 오류: `실행 타임아웃 (5초 초과)`

```sql
WITH biz_code_sub AS (
    SELECT code AS biz_code
    FROM common_code
    WHERE code_type = 'bizCd' AND code_name = '제조업'
),
metro_city_list AS (
    SELECT 
        c.code AS city_code,
        c.upper_code AS metro_code,
        c.code_name AS city_name
    FROM industry_type i
    JOIN common_code c ON c.code_type = 'cityCd' AND c.code = i.city_code AND c.upper_code = i.metro_code
    WHERE i.year = '2024'
      AND i.biz_code = (SELECT biz_code FROM biz_code_sub)
    GROUP BY c.code
    ORDER BY SUM(i.cust_count) DESC
    LIMIT 5
)
SELECT 
    mcl.city_name,
    b.bill_type,
    SUM(b.bill_count) AS total_bill_count
FROM billing_type b
JOIN metro_city_list mcl ON b.city_code = mcl.city_code AND b.metro_code = mcl.metro_code
WHERE b.year = '2024'
  AND b.bill_type IN ('이메일', '모바일', '카카오', '우편')
GROUP BY mcl.city_name, b.bill_type
ORDER BY mcl.city_name, b.bill_type
LIMIT 100;
```

**gpt-4o[full]** — `타임아웃`

> 실행 오류: `실행 타임아웃 (5초 초과)`

```sql
WITH top_cities AS (
    SELECT 
        t.city_code,
        SUM(t.cust_count) AS total_customers
    FROM 
        industry_type t
    JOIN 
        common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
    JOIN 
        common_code c ON c.code_type = 'cityCd' AND c.code = t.city_code AND c.upper_code = t.metro_code
    WHERE 
        t.year = '2024' AND b.code_name = '제조업'
    GROUP BY 
        t.city_code
    ORDER BY 
        total_customers DESC
    LIMIT 5
)
SELECT 
    c.code_name AS city_name,
    SUM(CASE WHEN b.bill_type IN ('이메일', '모바일', '카카오') THEN b.bill_count ELSE 0 END) AS electronic_count,
    SUM(CASE WHEN b.bill_type = '우편' THEN b.bill_count ELSE 0 END) AS paper_count
FROM 
    billing_type b
JOIN 
    top_cities tc ON tc.city_code = b.city_code
JOIN 
    common_code c ON c.code_type = 'cityCd' AND c.code = b.city_code
WHERE 
    b.year = '2024'
GROUP BY 
    c.code_name
LIMIT 100;
```

