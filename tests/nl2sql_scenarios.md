# NL2SQL 테스트 시나리오

한전 OPEN API 기반 SQLite DB (`kepco.db`) 대상 자연어 → SQL 변환 테스트

---

## 단일 테이블 / 단순 쿼리

### S-01. 특정 연월 + 지역 + 계약종별 전력사용량
```
자연어: 2023년 1월 서울 주택용 전력 사용량 알려줘
테이블: contract_type
```
```sql
SELECT metro, city, contract_type, SUM(power_usage) AS total_usage
FROM contract_type
WHERE year = '2023' AND month = '01'
  AND metro = '서울특별시' AND contract_type = '주택용'
GROUP BY metro, city, contract_type;
```

---

### S-02. 특정 지역 전체 계약종별 전기요금 조회
```
자연어: 2023년 12월 경기도 계약종별 전기요금 보여줘
테이블: contract_type
```
```sql
SELECT contract_type, SUM(bill) AS total_bill
FROM contract_type
WHERE year = '2023' AND month = '12' AND metro = '경기도'
GROUP BY contract_type
ORDER BY total_bill DESC;
```

---

### S-03. 전국 월별 평균 판매단가 추이
```
자연어: 2023년 월별 전국 평균 판매단가 추이 알려줘
테이블: contract_type
```
```sql
SELECT month, AVG(unit_cost) AS avg_unit_cost
FROM contract_type
WHERE year = '2023' AND metro = '전체'
GROUP BY month
ORDER BY month;
```

---

### S-04. 특정 산업분류 전력사용량 조회
```
자연어: 2023년 제조업 전력 사용량이 가장 많은 지역 TOP 5
테이블: industry_type
```
```sql
SELECT metro, city, SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2023' AND biz LIKE '%제조업%'
GROUP BY metro, city
ORDER BY total_usage DESC
LIMIT 5;
```

---

### S-05. EV 충전소 현황 조회
```
자연어: 서울에서 급속충전기가 가장 많은 구 TOP 5
테이블: ev_charge
```
```sql
SELECT city, SUM(rapid_count) AS rapid_total
FROM ev_charge
WHERE metro = '서울특별시'
GROUP BY city
ORDER BY rapid_total DESC
LIMIT 5;
```

---

### S-06. 복지할인 수혜자 현황
```
자연어: 2023년 기초수급자 복지할인 대상이 가장 많은 지역은?
테이블: welfare_discount
```
```sql
SELECT metro, city, SUM(welfare_count) AS total_count
FROM welfare_discount
WHERE year = '2023' AND welfare_type = '기초수급자'
GROUP BY metro, city
ORDER BY total_count DESC
LIMIT 10;
```

---

### S-07. 요금청구 방식 현황
```
자연어: 2023년 서울 구별 모바일 청구 건수 알려줘
테이블: billing_type
```
```sql
SELECT city, SUM(bill_count) AS mobile_count
FROM billing_type
WHERE year = '2023' AND metro = '서울특별시' AND bill_type = '모바일'
GROUP BY city
ORDER BY mobile_count DESC;
```

---

### S-08. 신재생에너지 용량 조회
```
자연어: 2023년 태양광 설치 용량이 가장 큰 시군구 TOP 10
테이블: renew_energy
```
```sql
SELECT metro, city, SUM(capacity) AS total_capacity
FROM renew_energy
WHERE year = '2023' AND gen_source = '태양광'
GROUP BY metro, city
ORDER BY total_capacity DESC
LIMIT 10;
```

---

### S-09. 가구 평균 전력사용량
```
자연어: 2023년 여름(7~8월) 서울 가구당 평균 전력사용량은?
테이블: house_avg
```
```sql
SELECT month, metro, AVG(power_usage) AS avg_usage
FROM house_avg
WHERE year = '2023' AND month IN ('07', '08') AND metro = '서울특별시'
GROUP BY month, metro
ORDER BY month;
```

---

### S-10. 산업별 고객 증감
```
자연어: 2023년 제조업 전기 신규 계약이 가장 많은 지역은?
테이블: industry_cust_change
```
```sql
SELECT metro, city, SUM(new_count) AS total_new
FROM industry_cust_change
WHERE year = '2023' AND biz LIKE '%제조업%'
GROUP BY metro, city
ORDER BY total_new DESC
LIMIT 10;
```

---

## 단일 테이블 / 복합 쿼리

### C-01. 연도별 비교 (YoY)
```
자연어: 2022년 대비 2023년 전국 산업용 전력 사용량 변화율은?
테이블: contract_type
```
```sql
SELECT
  a.year AS base_year,
  b.year AS compare_year,
  SUM(a.power_usage) AS usage_2022,
  SUM(b.power_usage) AS usage_2023,
  ROUND((SUM(b.power_usage) - SUM(a.power_usage)) * 100.0 / SUM(a.power_usage), 2) AS change_pct
FROM contract_type a
JOIN contract_type b
  ON a.metro = b.metro AND a.city = b.city
  AND a.month = b.month AND a.contract_type = b.contract_type
WHERE a.year = '2022' AND b.year = '2023'
  AND a.contract_type = '산업용' AND a.metro != '전체'
GROUP BY a.year, b.year;
```

---

### C-02. 계절별 집계
```
자연어: 2023년 계절별(봄/여름/가을/겨울) 전국 주택용 전력사용량 비교
테이블: contract_type
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
FROM contract_type
WHERE year = '2023' AND contract_type = '주택용' AND metro = '전체'
GROUP BY season
ORDER BY total_usage DESC;
```

---

### C-03. 비율 계산
```
자연어: 2023년 서울 복지할인 유형별 비율 알려줘
테이블: welfare_discount
```
```sql
SELECT
  welfare_type,
  SUM(welfare_count) AS count,
  ROUND(SUM(welfare_count) * 100.0 / SUM(SUM(welfare_count)) OVER (), 2) AS ratio_pct
FROM welfare_discount
WHERE year = '2023' AND metro = '서울특별시'
GROUP BY welfare_type
ORDER BY count DESC;
```

---

### C-04. 조건부 순위
```
자연어: 2023년 전력사용량 상위 5개 지역의 월별 사용량 추이
테이블: contract_type
```
```sql
WITH top_cities AS (
  SELECT metro, city, SUM(power_usage) AS total
  FROM contract_type
  WHERE year = '2023' AND metro != '전체'
  GROUP BY metro, city
  ORDER BY total DESC
  LIMIT 5
)
SELECT ct.month, ct.metro, ct.city, SUM(ct.power_usage) AS monthly_usage
FROM contract_type ct
JOIN top_cities tc ON ct.metro = tc.metro AND ct.city = tc.city
WHERE ct.year = '2023'
GROUP BY ct.month, ct.metro, ct.city
ORDER BY ct.month, monthly_usage DESC;
```

---

## 멀티 테이블 / 단순 쿼리

### M-01. 계약종별 + 복지할인 (지역 기준 JOIN)
```
자연어: 2023년 서울 구별 주택용 전력사용량과 복지할인 수혜 건수를 같이 보여줘
테이블: contract_type + welfare_discount
```
```sql
SELECT
  ct.city,
  SUM(ct.power_usage) AS power_usage,
  SUM(wd.welfare_count) AS welfare_count
FROM contract_type ct
LEFT JOIN welfare_discount wd
  ON ct.year = wd.year AND ct.month = wd.month
  AND ct.metro = wd.metro AND ct.city = wd.city
WHERE ct.year = '2023' AND ct.metro = '서울특별시'
  AND ct.contract_type = '주택용'
GROUP BY ct.city
ORDER BY power_usage DESC;
```

---

### M-02. EV 충전소 + 공통코드
```
자연어: 시도별 EV 충전소 급속/완속 합계 알려줘
테이블: ev_charge + common_code
```
```sql
SELECT
  e.metro,
  SUM(e.rapid_count) AS rapid_total,
  SUM(e.slow_count) AS slow_total,
  SUM(e.rapid_count + e.slow_count) AS total
FROM ev_charge e
GROUP BY e.metro
ORDER BY total DESC;
```

---

### M-03. 신재생에너지 + 계약종별
```
자연어: 2023년 태양광 설치 용량 상위 지역의 전력 자급률은?
테이블: renew_energy + contract_type
```
```sql
WITH renew AS (
  SELECT metro, city, SUM(capacity) AS solar_capacity
  FROM renew_energy
  WHERE year = '2023' AND gen_source = '태양광'
  GROUP BY metro, city
  ORDER BY solar_capacity DESC
  LIMIT 10
),
usage AS (
  SELECT metro, city, SUM(power_usage) AS total_usage
  FROM contract_type
  WHERE year = '2023' AND metro != '전체'
  GROUP BY metro, city
)
SELECT
  r.metro, r.city,
  r.solar_capacity,
  u.total_usage,
  ROUND(r.solar_capacity * 100.0 / NULLIF(u.total_usage, 0), 4) AS self_ratio_pct
FROM renew r
LEFT JOIN usage u ON r.metro = u.metro AND r.city = u.city
ORDER BY self_ratio_pct DESC;
```

---

## 멀티 테이블 / 복합 쿼리

### MC-01. 전력사용·고객증감·복지할인 종합 대시보드
```
자연어: 2023년 경기도 시군구별 전력사용량, 신규 고객 수, 복지할인 수혜자를 한번에 보여줘
테이블: contract_type + industry_cust_change + welfare_discount
```
```sql
SELECT
  ct.city,
  SUM(ct.power_usage)     AS total_power_usage,
  SUM(ic.new_count)       AS total_new_customers,
  SUM(wd.welfare_count)   AS total_welfare_count
FROM contract_type ct
LEFT JOIN industry_cust_change ic
  ON ct.year = ic.year AND ct.month = ic.month
  AND ct.metro = ic.metro AND ct.city = ic.city
LEFT JOIN welfare_discount wd
  ON ct.year = wd.year AND ct.month = wd.month
  AND ct.metro = wd.metro AND ct.city = wd.city
WHERE ct.year = '2023' AND ct.metro = '경기도'
GROUP BY ct.city
ORDER BY total_power_usage DESC;
```

---

### MC-02. 청구방식 전환율 + 전력사용량 상관
```
자연어: 2023년 모바일 청구 비율이 높은 지역이 전력 사용량도 많은지 비교해줘
테이블: billing_type + contract_type
```
```sql
WITH mobile_ratio AS (
  SELECT metro, city,
    SUM(CASE WHEN bill_type = '모바일' THEN bill_count ELSE 0 END) * 100.0
      / NULLIF(SUM(bill_count), 0) AS mobile_pct
  FROM billing_type
  WHERE year = '2023'
  GROUP BY metro, city
),
power AS (
  SELECT metro, city, SUM(power_usage) AS total_usage
  FROM contract_type
  WHERE year = '2023' AND metro != '전체'
  GROUP BY metro, city
)
SELECT
  m.metro, m.city,
  ROUND(m.mobile_pct, 2) AS mobile_pct,
  p.total_usage
FROM mobile_ratio m
JOIN power p ON m.metro = p.metro AND m.city = p.city
ORDER BY mobile_pct DESC
LIMIT 20;
```

---

### MC-03. EV 인프라 + 신재생에너지 그린지수
```
자연어: 지역별 태양광 용량과 EV 충전기 수 기준으로 친환경 점수 계산해줘
테이블: renew_energy + ev_charge
```
```sql
WITH solar AS (
  SELECT metro, SUM(capacity) AS solar_cap
  FROM renew_energy WHERE year = '2023' AND gen_source = '태양광'
  GROUP BY metro
),
ev AS (
  SELECT metro,
    SUM(rapid_count) AS rapid,
    SUM(slow_count) AS slow
  FROM ev_charge
  GROUP BY metro
)
SELECT
  s.metro,
  ROUND(s.solar_cap, 0)       AS solar_capacity_kwh,
  ev.rapid + ev.slow          AS total_chargers,
  ROUND(s.solar_cap / 10000 + (ev.rapid * 2 + ev.slow), 2) AS green_score
FROM solar s
JOIN ev ON s.metro = ev.metro
ORDER BY green_score DESC;
```

---

### MC-04. 입찰계약 + 전력사용 이상치 탐지
```
자연어: 2023년 전력사용량이 전년 대비 20% 이상 증가한 지역의 신재생에너지 설치 현황은?
테이블: contract_type + renew_energy
```
```sql
WITH usage_change AS (
  SELECT
    a.metro, a.city,
    SUM(a.power_usage) AS usage_2022,
    SUM(b.power_usage) AS usage_2023,
    (SUM(b.power_usage) - SUM(a.power_usage)) * 100.0
      / NULLIF(SUM(a.power_usage), 0) AS change_pct
  FROM contract_type a
  JOIN contract_type b ON a.metro = b.metro AND a.city = b.city AND a.month = b.month
  WHERE a.year = '2022' AND b.year = '2023' AND a.metro != '전체'
  GROUP BY a.metro, a.city
  HAVING change_pct >= 20
)
SELECT
  uc.metro, uc.city,
  ROUND(uc.change_pct, 2) AS power_increase_pct,
  SUM(re.capacity) AS renew_capacity
FROM usage_change uc
LEFT JOIN renew_energy re ON uc.metro = re.metro AND uc.city = re.city
GROUP BY uc.metro, uc.city, uc.change_pct
ORDER BY uc.change_pct DESC;
```

---

## 시나리오 요약

| ID | 유형 | 난이도 | 자연어 질문 |
|----|------|--------|------------|
| S-01 | 단일/단순 | ⭐ | 2023년 1월 서울 주택용 전력 사용량 |
| S-02 | 단일/단순 | ⭐ | 2023년 12월 경기도 계약종별 전기요금 |
| S-03 | 단일/단순 | ⭐ | 2023년 월별 전국 평균 판매단가 추이 |
| S-04 | 단일/단순 | ⭐ | 2023년 제조업 전력 사용량 TOP 5 지역 |
| S-05 | 단일/단순 | ⭐ | 서울 급속충전기 가장 많은 구 TOP 5 |
| S-06 | 단일/단순 | ⭐ | 2023년 기초수급자 복지할인 대상 많은 지역 |
| S-07 | 단일/단순 | ⭐ | 2023년 서울 구별 모바일 청구 건수 |
| S-08 | 단일/단순 | ⭐ | 2023년 태양광 설치 용량 큰 시군구 TOP 10 |
| S-09 | 단일/단순 | ⭐ | 2023년 여름 서울 가구당 평균 전력사용량 |
| S-10 | 단일/단순 | ⭐ | 2023년 제조업 신규 계약 많은 지역 |
| C-01 | 단일/복합 | ⭐⭐ | 2022→2023 산업용 전력 사용량 변화율 |
| C-02 | 단일/복합 | ⭐⭐ | 2023년 계절별 주택용 전력사용량 비교 |
| C-03 | 단일/복합 | ⭐⭐ | 2023년 서울 복지할인 유형별 비율 |
| C-04 | 단일/복합 | ⭐⭐ | 전력사용량 상위 5개 지역 월별 추이 |
| M-01 | 멀티/단순 | ⭐⭐ | 서울 구별 주택용 전력사용량 + 복지할인 |
| M-02 | 멀티/단순 | ⭐⭐ | 시도별 EV 급속/완속 충전소 합계 |
| M-03 | 멀티/단순 | ⭐⭐ | 태양광 상위 지역 전력 자급률 |
| MC-01 | 멀티/복합 | ⭐⭐⭐ | 경기도 시군구별 전력·신규고객·복지 종합 |
| MC-02 | 멀티/복합 | ⭐⭐⭐ | 모바일 청구 비율 vs 전력사용량 상관 |
| MC-03 | 멀티/복합 | ⭐⭐⭐ | 태양광+EV 기반 친환경 점수 |
| MC-04 | 멀티/복합 | ⭐⭐⭐ | 전력 급증 지역 신재생에너지 설치 현황 |
