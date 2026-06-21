# NL2SQL 빠른 성능 테스트 (industry_type)

`industry_type` 중심 **10분 이내 수행 가능한** NL2SQL 평가 시나리오.
한국표준산업분류(KSIC) 기반 대분류별 통계를 다루며, `business_type`보다 범용적인 산업 질문을 테스트합니다.

**구성**: 단일 테이블 8개(`I-01~I-08`) + 다중 테이블 JOIN 4개(`IJ-01~IJ-04`) = 총 12개 시나리오.

---

## 테이블 개요

**`industry_type`** — 한국표준산업분류(KSIC) 대분류별 전력 사용량·요금 통계

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `year` | TEXT | 연도 ('2021'~'2026') |
| `month` | TEXT | 월 ('01'~'12') |
| `metro` | TEXT | 광역시도명 |
| `city` | TEXT | 시군구명 |
| `biz` | TEXT | 산업 분류명 (예: `제조업`, `건설업`, `숙박 및 음식점업`) |
| `cust_count` | INTEGER | 고객 수 |
| `power_usage` | REAL | 전력 사용량 (kWh) |
| `bill` | REAL | 전기요금 (원) |
| `unit_cost` | REAL | 단가 (원/kWh) |

**데이터 범위**: 2021-01 ~ 2026-02 (총 274,138행)

---

## 데이터 특성 주의사항 (함정 요소)

| 함정 | 설명 |
|------|------|
| **산업명 변형** | 동일 산업도 공백, 슬래시(/), 쉼표(,) 사용이 제각각 (예: `농업, 임업 및 어업`, `농업/ 임업 및 어업 `). **LIKE 연산자 사용 필수** |
| **지역명 통일성** | 이 테이블은 대부분 `강원특별자치도`로 통일되어 있으나, 다른 테이블과 JOIN 시 `강원도`와 매핑 주의 |
| **요금 정보 포함** | `business_type`과 달리 `bill`, `unit_cost` 컬럼이 있어 요금 분석 가능 |
| **계약전력 없음** | `contract_power` 컬럼이 없음 (해당 질문은 답할 수 없음) |
| **2026년 부분 데이터** | 1~2월까지만 존재 |

---

## 평가 축

각 시나리오는 아래 축 중 하나 이상을 테스트합니다.

- **BASIC**: WHERE/GROUP BY 기본 집계
- **RANK**: 정렬·TOP N
- **TIME**: 연도 비교·계절·추이
- **SCHEMA**: 스키마 함정 (산업명 변형·공백)
- **IMPOSSIBLE**: 답할 수 없는 질문 거절 능력
- **JOIN**: 다중 테이블 조인·CTE 구성 능력

---

## JOIN 파트너 테이블 요약

| 테이블 | JOIN 키 | 쓰임 |
|--------|---------|------|
| `industry_cust_change` | `year, month, metro, city, biz` | 산업별 신규/증설/해지 고객 수 |
| `house_avg` | `year, month, metro, city` | 가구당 평균 전력·요금 (산업 vs 가정 비교) |
| `renew_energy` | `year, metro, city` | 지역별 신재생 발전원 설비 용량 |
| `billing_type` | `year, month, metro, city` | 청구서 유형별 발송 건수 |

---

## 시나리오 (12개)

### I-01. 특정 월·산업 사용량 조회 ⭐ [BASIC + SCHEMA]

```
자연어: 2024년 3월 제조업 전력사용량 전국 총합은?
```

```sql
SELECT SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024' AND month = '03' AND biz LIKE '%제조업%';
```

**평가 포인트**: 정확한 WHERE 필터, 명칭 변형에 대비한 LIKE 사용 여부

---

### I-02. 산업별 사용량 TOP 5 ⭐ [BASIC + RANK]

```
자연어: 2024년 한 해 동안 전력을 가장 많이 쓴 산업 TOP 5 알려줘
```

```sql
SELECT biz, SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024'
GROUP BY biz
ORDER BY total_usage DESC
LIMIT 5;
```

**평가 포인트**: GROUP BY + ORDER BY + LIMIT

---

### I-03. 지역별 특정 산업 순위 ⭐⭐ [RANK]

```
자연어: 2024년 건설업 전력사용량이 가장 많은 광역시도 TOP 3
```

```sql
SELECT metro, SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024' AND biz LIKE '%건설업%'
GROUP BY metro
ORDER BY total_usage DESC
LIMIT 3;
```

**평가 포인트**: 산업명 필터링 및 지역별 그룹화 정확성

---

### I-04. 전년 대비 증감률 (YoY) ⭐⭐ [TIME]

```
자연어: 2023년 대비 2024년 제조업 전력사용량 증감률은?
```

```sql
SELECT
  SUM(CASE WHEN year='2023' THEN power_usage END) AS usage_2023,
  SUM(CASE WHEN year='2024' THEN power_usage END) AS usage_2024,
  ROUND(
    (SUM(CASE WHEN year='2024' THEN power_usage END)
     - SUM(CASE WHEN year='2023' THEN power_usage END)) * 100.0
    / SUM(CASE WHEN year='2023' THEN power_usage END), 2
  ) AS yoy_pct
FROM industry_type
WHERE biz LIKE '%제조업%' AND year IN ('2023', '2024');
```

**평가 포인트**: 연도 비교를 위한 집계 쿼리 구성 능력

---

### I-05. 계절별 집계 ⭐⭐ [TIME]

```
자연어: 2024년 전국 숙박 및 음식점업의 계절별(봄/여름/가을/겨울) 전력사용량 비교
```

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
WHERE year = '2024' AND biz LIKE '%숙박%'
GROUP BY season
ORDER BY total_usage DESC;
```

**평가 포인트**: 월별 데이터를 계절로 그룹화하는 로직

---

### I-06. 평균 판매단가 분석 ⭐⭐ [BASIC + RANK]

```
자연어: 2024년 산업별 평균 판매단가(원/kWh)가 가장 비싼 산업 TOP 5
```

```sql
SELECT
  biz,
  SUM(bill) AS total_bill,
  SUM(power_usage) AS total_usage,
  ROUND(SUM(bill) / NULLIF(SUM(power_usage), 0), 2) AS avg_unit_cost
FROM industry_type
WHERE year = '2024'
GROUP BY biz
ORDER BY avg_unit_cost DESC
LIMIT 5;
```

**평가 포인트**: 요금/사용량 기반 파생 지표 계산 및 0 나눗셈 방지

---

### I-07. 지역 내 산업 점유율 ⭐⭐⭐ [RANK]

```
자연어: 2024년 서울특별시에서 각 산업별 전력사용량 점유율(%)을 보여줘
```

```sql
SELECT
  biz,
  SUM(power_usage) AS usage,
  ROUND(SUM(power_usage) * 100.0 / SUM(SUM(power_usage)) OVER (), 2) AS share_pct
FROM industry_type
WHERE year = '2024' AND metro = '서울특별시'
GROUP BY biz
ORDER BY usage DESC;
```

**평가 포인트**: 윈도우 함수(SUM OVER)를 활용한 비율 계산

---

### I-08. 답할 수 없는 질문 (거절 능력) ⭐⭐⭐ [IMPOSSIBLE]

```
자연어: 2024년 제조업의 전국 계약전력 총합은 얼마야?
```

```sql
-- 정답: industry_type 테이블에는 'contract_power' 컬럼이 없음.
-- 모델은 "해당 테이블에 계약전력 정보 없음" 응답 또는 business_type/contract_type 테이블 제안을 해야 함.
-- 틀린 답 예시: SELECT SUM(contract_power) FROM industry_type ... (존재하지 않는 컬럼)
```

**평가 포인트**: 환각(hallucination) 방지, 스키마 한계 인식

---

## JOIN 시나리오 (4개)

### IJ-01. 산업 전력 증가 × 신규 고객 수 ⭐⭐ [JOIN + TIME]

```
자연어: 2024년 제조업에서 전력사용량이 가장 많이 늘어난 광역시도 TOP 5와,
        같은 기간 해당 지역의 제조업 신규 고객 수를 같이 보여줘
```

```sql
WITH usage_2024 AS (
  SELECT metro, SUM(power_usage) AS usage
  FROM industry_type
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro
),
usage_2023 AS (
  SELECT metro, SUM(power_usage) AS usage
  FROM industry_type
  WHERE year='2023' AND biz LIKE '%제조업%'
  GROUP BY metro
),
new_cust AS (
  SELECT metro, SUM(new_count) AS new_cust_2024
  FROM industry_cust_change
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro
)
SELECT
  u24.metro,
  u24.usage - COALESCE(u23.usage, 0) AS usage_increase,
  COALESCE(nc.new_cust_2024, 0) AS new_cust_count
FROM usage_2024 u24
LEFT JOIN usage_2023 u23 ON u24.metro = u23.metro
LEFT JOIN new_cust   nc  ON u24.metro = nc.metro
ORDER BY usage_increase DESC
LIMIT 5;
```

**평가 포인트**: 복수 CTE 구성, 5개 키 중 일부(metro, biz)만 사용한 부분 집계 JOIN

---

### IJ-02. 산업용 vs 가정용 전력 비중 ⭐⭐ [JOIN + BASIC]

```
자연어: 2024년 광역시도별로 제조업 전력사용량이 가구당 평균 전력사용량의
        몇 배인지 계산해줘 (광역시도별 연간 합계 기준)
```

```sql
WITH ind AS (
  SELECT metro, SUM(power_usage) AS ind_usage
  FROM industry_type
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro
),
house AS (
  SELECT metro,
         SUM(power_usage) AS total_house_usage,
         AVG(power_usage * 1.0 / NULLIF(house_count,0)) AS avg_per_house
  FROM house_avg
  WHERE year='2024'
  GROUP BY metro
)
SELECT
  i.metro,
  i.ind_usage,
  h.avg_per_house,
  ROUND(i.ind_usage / NULLIF(h.avg_per_house, 0), 1) AS ratio
FROM ind i
JOIN house h ON i.metro = h.metro
ORDER BY ratio DESC;
```

**평가 포인트**: 서로 다른 집계 단위(총합 vs 평균) JOIN, 가구당 계산 정확성

---

### IJ-03. 산업전력 대비 신재생 설비 용량 비율 ⭐⭐⭐ [JOIN + RANK]

```
자연어: 2024년 광역시도별 전체 산업 전력사용량 대비 신재생에너지 설비 총 용량의
        비율을 계산하고, 신재생 비중이 높은 지역 TOP 5를 보여줘
```

```sql
WITH ind AS (
  SELECT metro, SUM(power_usage) AS ind_usage
  FROM industry_type
  WHERE year='2024'
  GROUP BY metro
),
renew AS (
  SELECT metro, SUM(capacity) AS renew_capacity
  FROM renew_energy
  WHERE year='2024'
  GROUP BY metro
)
SELECT
  i.metro,
  i.ind_usage,
  r.renew_capacity,
  ROUND(r.renew_capacity * 1.0 / NULLIF(i.ind_usage, 0) * 1e6, 3) AS renew_per_mwh
FROM ind i
LEFT JOIN renew r ON i.metro = r.metro
ORDER BY renew_per_mwh DESC
LIMIT 5;
```

**평가 포인트**: 단위 다른 두 값(kWh vs kW)의 비율 계산, LEFT JOIN으로 신재생 없는 지역 포함 여부

---

### IJ-04. 산업별 전자청구서 보급률 ⭐⭐⭐ [JOIN + SCHEMA]

```
자연어: 2024년 제조업 고객이 많은 상위 5개 시군구에서, 해당 지역의 전체
        전자청구서(이메일/모바일/카카오) 발송 건수와 우편 청구서 발송 건수를 함께 보여줘
```

> 💡 **bill_type 실제 값**: `우편`, `이메일`, `모바일`, `카카오`, `인편`, `''`(빈값)
> 모델이 `종이`/`e-bill` 같이 존재하지 않는 값을 환각하는지 관찰.

```sql
WITH mfg_cust AS (
  SELECT metro, city, SUM(cust_count) AS mfg_cust_cnt
  FROM industry_type
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro, city
  ORDER BY mfg_cust_cnt DESC
  LIMIT 5
),
bills AS (
  SELECT
    metro, city,
    SUM(CASE WHEN bill_type IN ('이메일','모바일','카카오') THEN bill_count ELSE 0 END) AS e_bill,
    SUM(CASE WHEN bill_type = '우편' THEN bill_count ELSE 0 END) AS paper_bill
  FROM billing_type
  WHERE year='2024'
  GROUP BY metro, city
)
SELECT
  m.metro, m.city, m.mfg_cust_cnt,
  b.e_bill, b.paper_bill
FROM mfg_cust m
LEFT JOIN bills b ON m.metro = b.metro AND m.city = b.city
ORDER BY m.mfg_cust_cnt DESC;
```

**평가 포인트**:
- metro+city 복합키 JOIN
- `bill_type` 실제 값 사용 여부(모델이 '종이'같은 값을 환각하는지 관찰)
- 산업별 필터와 전체 청구서 집계의 범위 차이 이해
