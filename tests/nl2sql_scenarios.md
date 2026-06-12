# NL2SQL 테스트 시나리오

한전 OPEN API 기반 SQLite DB (`kepco.db`) 대상 자연어 → SQL 변환 테스트

> **스키마 기준**: 지역명은 `metro_code`/`city_code` 코드 컬럼으로 저장.
> 지역명 표시 시 `common_code` JOIN 필요. city_code는 광역시도 내 고유값이므로
> city JOIN에는 반드시 `AND c.upper_code = <table>.metro_code` 조건을 추가.
>
> `metro_code` 주요값: 11=서울, 21=부산, 22=대구, 23=인천, 24=광주, 25=대전, 26=울산, 31=경기도, 32=강원, 33=충북, 34=충남, 35=전북, 36=전남, 37=경북, 38=경남, 39=제주, 41=세종.
> `biz_code` 주요값: C=제조업, D=전기·가스, G=도소매, I=숙박음식, KEPCO01=주택용.

---

## 단일 테이블 / 단순 쿼리

### S-01. 특정 연월 + 지역 + 계약종별 전력사용량
```
자연어: 2023년 1월 서울 주택용 전력 사용량 알려줘
테이블: contract_type
```
```sql
SELECT m.code_name AS metro, c.code_name AS city, ct.contract_type, SUM(ct.power_usage) AS total_usage
FROM contract_type ct
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = ct.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = ct.city_code AND c.upper_code = ct.metro_code
WHERE ct.year = '2023' AND ct.month = '01'
  AND ct.metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시') AND ct.contract_type = '주택용'
GROUP BY ct.metro_code, ct.city_code, ct.contract_type;
```

---

### S-02. 특정 지역 전체 계약종별 전기요금 조회
```
자연어: 2023년 12월 경기도 계약종별 전기요금 보여줘
테이블: contract_type
```
```sql
SELECT ct.contract_type, SUM(ct.bill) AS total_bill
FROM contract_type ct
WHERE ct.year = '2023' AND ct.month = '12' AND ct.metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='경기도')
GROUP BY ct.contract_type
ORDER BY total_bill DESC;
```

---

### S-03. 전국 월별 가중평균 판매단가 추이
```
자연어: 2023년 월별 전국 평균 판매단가 추이 알려줘
테이블: contract_type
```
```sql
SELECT month,
  ROUND(SUM(bill) * 1.0 / NULLIF(SUM(power_usage), 0), 4) AS avg_unit_cost
FROM contract_type
WHERE year = '2023'
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
SELECT m.code_name AS metro, c.code_name AS city, SUM(it.power_usage) AS total_usage
FROM industry_type it
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = it.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = it.city_code AND c.upper_code = it.metro_code
WHERE it.year = '2023' AND it.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
GROUP BY it.metro_code, it.city_code
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
SELECT c.code_name AS city, SUM(ec.rapid_count) AS rapid_total
FROM ev_charge ec
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = ec.city_code AND c.upper_code = ec.metro_code
WHERE ec.metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
GROUP BY ec.city_code
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
SELECT m.code_name AS metro, c.code_name AS city, SUM(wd.welfare_count) AS total_count
FROM welfare_discount wd
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = wd.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = wd.city_code AND c.upper_code = wd.metro_code
WHERE wd.year = '2023' AND wd.welfare_type = '기초수급자'
GROUP BY wd.metro_code, wd.city_code
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
SELECT c.code_name AS city, SUM(bt.bill_count) AS mobile_count
FROM billing_type bt
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = bt.city_code AND c.upper_code = bt.metro_code
WHERE bt.year = '2023' AND bt.metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시') AND bt.bill_type = '모바일'
GROUP BY bt.city_code
ORDER BY mobile_count DESC;
```

---

### S-08. 신재생에너지 용량 조회
```
자연어: 2024년 태양광 설치 용량이 가장 큰 시군구 TOP 10
테이블: renew_energy
비고: renew_energy는 2024년 데이터만 존재
```
```sql
SELECT m.code_name AS metro, c.code_name AS city, SUM(re.capacity) AS total_capacity
FROM renew_energy re
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = re.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = re.city_code AND c.upper_code = re.metro_code
WHERE re.year = '2024' AND re.gen_source = '태양광'
GROUP BY re.metro_code, re.city_code
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
SELECT ht.month, m.code_name AS metro, AVG(ht.power_usage) AS avg_usage
FROM house_avg ht
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = ht.metro_code
WHERE ht.year = '2023' AND ht.month IN ('07', '08') AND ht.metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
GROUP BY ht.month, ht.metro_code
ORDER BY ht.month;
```

---

### S-10. 산업별 고객 증감
```
자연어: 2023년 제조업 전기 신규 계약이 가장 많은 지역은?
테이블: industry_cust_change
```
```sql
SELECT m.code_name AS metro, c.code_name AS city, SUM(ic.new_count) AS total_new
FROM industry_cust_change ic
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = ic.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = ic.city_code AND c.upper_code = ic.metro_code
WHERE ic.year = '2023' AND ic.biz_code = (SELECT code FROM common_code WHERE code_type='bizCd' AND code_name='제조업')
GROUP BY ic.metro_code, ic.city_code
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
  ON a.metro_code = b.metro_code AND a.city_code = b.city_code
  AND a.month = b.month AND a.contract_type = b.contract_type
WHERE a.year = '2022' AND b.year = '2023'
  AND a.contract_type = '산업용'
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
WHERE year = '2023' AND contract_type = '주택용'
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
WHERE year = '2023' AND metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
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
  SELECT metro_code, city_code, SUM(power_usage) AS total
  FROM contract_type
  WHERE year = '2023'
  GROUP BY metro_code, city_code
  ORDER BY total DESC
  LIMIT 5
)
SELECT ct.month, m.code_name AS metro, c.code_name AS city, SUM(ct.power_usage) AS monthly_usage
FROM contract_type ct
JOIN top_cities tc ON ct.metro_code = tc.metro_code AND ct.city_code = tc.city_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = ct.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = ct.city_code AND c.upper_code = ct.metro_code
WHERE ct.year = '2023'
GROUP BY ct.month, ct.metro_code, ct.city_code
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
WITH ct_agg AS (
  SELECT city_code, SUM(power_usage) AS power_usage
  FROM contract_type
  WHERE year = '2023'
    AND metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
    AND contract_type = '주택용'
  GROUP BY city_code
),
wd_agg AS (
  SELECT city_code, SUM(welfare_count) AS welfare_count
  FROM welfare_discount
  WHERE year = '2023'
    AND metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
  GROUP BY city_code
)
SELECT c.code_name AS city, ct_agg.power_usage, COALESCE(wd_agg.welfare_count, 0) AS welfare_count
FROM ct_agg
LEFT JOIN wd_agg ON ct_agg.city_code = wd_agg.city_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = ct_agg.city_code
  AND c.upper_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='서울특별시')
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
  m.code_name AS metro,
  SUM(ec.rapid_count) AS rapid_total,
  SUM(ec.slow_count) AS slow_total,
  SUM(ec.rapid_count + ec.slow_count) AS total
FROM ev_charge ec
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = ec.metro_code
GROUP BY ec.metro_code
ORDER BY total DESC;
```

---

### M-03. 신재생에너지 + 계약종별
```
자연어: 2024년 태양광 설치 용량 상위 지역의 전력 자급률은?
테이블: renew_energy + contract_type
비고: renew_energy는 2024년 데이터만 존재
```
```sql
WITH renew AS (
  SELECT metro_code, city_code, SUM(capacity) AS solar_capacity
  FROM renew_energy
  WHERE year = '2024' AND gen_source = '태양광'
  GROUP BY metro_code, city_code
  ORDER BY solar_capacity DESC
  LIMIT 10
),
usage AS (
  SELECT metro_code, city_code, SUM(power_usage) AS total_usage
  FROM contract_type
  WHERE year = '2024'
  GROUP BY metro_code, city_code
)
SELECT
  m.code_name AS metro, c.code_name AS city,
  r.solar_capacity,
  u.total_usage,
  ROUND(r.solar_capacity * 100.0 / NULLIF(u.total_usage, 0), 4) AS self_ratio_pct
FROM renew r
LEFT JOIN usage u ON r.metro_code = u.metro_code AND r.city_code = u.city_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = r.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = r.city_code AND c.upper_code = r.metro_code
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
  c.code_name AS city,
  SUM(ct.power_usage)     AS total_power_usage,
  SUM(ic.new_count)       AS total_new_customers,
  SUM(wd.welfare_count)   AS total_welfare_count
FROM contract_type ct
LEFT JOIN industry_cust_change ic
  ON ct.year = ic.year AND ct.month = ic.month
  AND ct.metro_code = ic.metro_code AND ct.city_code = ic.city_code
LEFT JOIN welfare_discount wd
  ON ct.year = wd.year AND ct.month = wd.month
  AND ct.metro_code = wd.metro_code AND ct.city_code = wd.city_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = ct.city_code AND c.upper_code = ct.metro_code
WHERE ct.year = '2023' AND ct.metro_code = (SELECT code FROM common_code WHERE code_type='metroCd' AND code_name='경기도')
GROUP BY ct.city_code
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
  SELECT metro_code, city_code,
    SUM(CASE WHEN bill_type = '모바일' THEN bill_count ELSE 0 END) * 100.0
      / NULLIF(SUM(bill_count), 0) AS mobile_pct
  FROM billing_type
  WHERE year = '2023'
  GROUP BY metro_code, city_code
),
power AS (
  SELECT metro_code, city_code, SUM(power_usage) AS total_usage
  FROM contract_type
  WHERE year = '2023'
  GROUP BY metro_code, city_code
)
SELECT
  m.code_name AS metro, c.code_name AS city,
  ROUND(mr.mobile_pct, 2) AS mobile_pct,
  p.total_usage
FROM mobile_ratio mr
JOIN power p ON mr.metro_code = p.metro_code AND mr.city_code = p.city_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = mr.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = mr.city_code AND c.upper_code = mr.metro_code
ORDER BY mobile_pct DESC
LIMIT 20;
```

---

### MC-03. EV 인프라 + 신재생에너지 그린지수
```
자연어: 지역별 태양광 용량과 EV 충전기 수 기준으로 친환경 점수 계산해줘
테이블: renew_energy + ev_charge
비고: renew_energy는 2024년 데이터만 존재
```
```sql
WITH solar AS (
  SELECT metro_code, SUM(capacity) AS solar_cap
  FROM renew_energy WHERE year = '2024' AND gen_source = '태양광'
  GROUP BY metro_code
),
ev AS (
  SELECT metro_code,
    SUM(rapid_count) AS rapid,
    SUM(slow_count) AS slow
  FROM ev_charge
  GROUP BY metro_code
)
SELECT
  m.code_name AS metro,
  ROUND(s.solar_cap, 0)       AS solar_capacity_kwh,
  ev.rapid + ev.slow          AS total_chargers,
  ROUND(s.solar_cap / 10000 + (ev.rapid * 2 + ev.slow), 2) AS green_score
FROM solar s
JOIN ev ON s.metro_code = ev.metro_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = s.metro_code
ORDER BY green_score DESC;
```

---

### MC-04. 입찰계약 + 전력사용 이상치 탐지
```
자연어: 2023년 전력사용량이 전년 대비 20% 이상 증가한 지역의 신재생에너지 설치 현황은?
테이블: contract_type + renew_energy
비고: renew_energy는 2024년 데이터만 존재 → 신재생 설치 현황은 2024년 기준
```
```sql
WITH usage_change AS (
  SELECT
    a.metro_code, a.city_code,
    SUM(a.power_usage) AS usage_2022,
    SUM(b.power_usage) AS usage_2023,
    (SUM(b.power_usage) - SUM(a.power_usage)) * 100.0
      / NULLIF(SUM(a.power_usage), 0) AS change_pct
  FROM contract_type a
  JOIN contract_type b ON a.metro_code = b.metro_code AND a.city_code = b.city_code AND a.month = b.month
  WHERE a.year = '2022' AND b.year = '2023'
  GROUP BY a.metro_code, a.city_code
  HAVING change_pct >= 20
)
SELECT
  m.code_name AS metro, c.code_name AS city,
  ROUND(uc.change_pct, 2) AS power_increase_pct,
  SUM(re.capacity) AS renew_capacity
FROM usage_change uc
LEFT JOIN renew_energy re ON uc.metro_code = re.metro_code AND uc.city_code = re.city_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = uc.metro_code
JOIN common_code c ON c.code_type = 'cityCd' AND c.code = uc.city_code AND c.upper_code = uc.metro_code
GROUP BY uc.metro_code, uc.city_code, uc.change_pct
ORDER BY uc.change_pct DESC;
```

---

---

## 신규 테이블 시나리오 (12개 파일데이터 기반)

> **주의**: 아래 N-시리즈는 `build_filedata_db.py`로 적재하는 파일데이터 테이블 대상.
> 해당 테이블들이 DB에 없으면 실행 불가.

### N-01. 판매통계 계약종별 수익
```
자연어: 2024년 시도별 산업용 전력 판매 수익 TOP 10 알려줘
테이블: sales_stat
```
```sql
SELECT metro, SUM(revenue) AS total_revenue
FROM sales_stat
WHERE SUBSTR(period, 1, 4) = '2024' AND contract_type = '산업용'
GROUP BY metro
ORDER BY total_revenue DESC
LIMIT 10;
```

---

### N-02. 시간대별 전력수요 피크
```
자연어: 2024년 시간대별 전국 평균 전력수요 패턴을 알려줘
테이블: hourly_power
```
```sql
SELECT hour,
  ROUND(AVG(power_usage), 2) AS avg_usage,
  MAX(power_usage) AS peak_usage
FROM hourly_power
WHERE SUBSTR(date, 1, 4) = '2024'
GROUP BY hour
ORDER BY hour;
```

---

### N-03. 하계/동계 피크 비교
```
자연어: 여름(7~8월)과 겨울(12~1월) 시간대별 전력수요 차이를 비교해줘
테이블: hourly_power
```
```sql
SELECT hour,
  ROUND(AVG(CASE WHEN SUBSTR(date,6,2) IN ('07','08') THEN power_usage END), 2) AS summer_avg,
  ROUND(AVG(CASE WHEN SUBSTR(date,6,2) IN ('12','01') THEN power_usage END), 2) AS winter_avg
FROM hourly_power
GROUP BY hour
ORDER BY hour;
```

---

### N-04. 동별 상계거래 현황
```
자연어: 2024년 상계거래 태양광 설치 건수가 가장 많은 구·동 TOP 10
테이블: net_metering_usage
```
```sql
SELECT metro, city, dong, SUM(total_count) AS total_installs
FROM net_metering_usage
WHERE year = '2024'
GROUP BY metro, city, dong
ORDER BY total_installs DESC
LIMIT 10;
```

---

### N-05. 상계거래 잉여율 분석
```
자연어: 2024년 시도별 상계거래 잉여율(잉여전력 / 사용전력)이 가장 높은 지역은?
테이블: net_metering_usage + net_metering_surplus
```
```sql
SELECT u.metro,
  SUM(u.power_usage) AS total_usage,
  SUM(s.surplus_power) AS total_surplus,
  ROUND(SUM(s.surplus_power) * 100.0 / NULLIF(SUM(u.power_usage), 0), 2) AS surplus_ratio_pct
FROM net_metering_usage u
JOIN net_metering_surplus s
  ON u.metro = s.metro AND u.city = s.city AND u.dong = s.dong
  AND u.year = s.year AND u.month = s.month
WHERE u.year = '2024'
GROUP BY u.metro
ORDER BY surplus_ratio_pct DESC;
```

---

### N-06. PPA 에너지원별 누적 용량
```
자연어: PPA 계약 에너지원별 누적 용량 추이를 연도별로 보여줘
테이블: ppa_by_source
```
```sql
SELECT year, gen_source,
  SUM(vendor_count) AS vendors,
  ROUND(SUM(capacity_kw) / 1000, 2) AS capacity_mw
FROM ppa_by_source
GROUP BY year, gen_source
ORDER BY year, capacity_mw DESC;
```

---

### N-07. PPA 지역별 태양광 집중도
```
자연어: 태양광 PPA 계약 용량이 가장 많은 시군구 TOP 10 알려줘
테이블: ppa_by_region
```
```sql
SELECT region, city,
  count AS contract_count,
  ROUND(capacity_kw / 1000, 2) AS capacity_mw
FROM ppa_by_region
WHERE gen_source = '태양광'
ORDER BY capacity_kw DESC
LIMIT 10;
```

---

### N-08. 요금조정 이력
```
자연어: 연도별 주택용 전기요금 조정 이력을 알려줘 (변동이 있었던 연도만)
테이블: tariff_adjustment
```
```sql
SELECT year, month, residential AS residential_adj_pct, total AS total_adj_pct
FROM tariff_adjustment
WHERE residential != 0
ORDER BY year, month;
```

---

### N-09. 동별 산업 전력사용 밀집 지역
```
자연어: 제조업 전력사용량이 가장 높은 읍면동 TOP 10 알려줘
테이블: dong_industry_power
```
```sql
SELECT metro, city, dong, biz_name_large,
  SUM(power_usage) AS total_usage
FROM dong_industry_power
WHERE biz_name_large = '제조업'
GROUP BY metro, city, dong, biz_name_large
ORDER BY total_usage DESC
LIMIT 10;
```

---

### N-10. 산업별 수용률 여름/겨울 비교
```
자연어: 산업별 수용률에서 여름과 겨울 차이가 가장 큰 업종은?
테이블: industry_utilization
```
```sql
SELECT biz_name, power_range,
  ROUND(util_jul, 4) AS summer_util,
  ROUND(util_jan, 4) AS winter_util,
  ROUND(ABS(util_jul - util_jan), 4) AS season_diff
FROM industry_utilization
WHERE util_jul IS NOT NULL AND util_jan IS NOT NULL
ORDER BY season_diff DESC
LIMIT 10;
```

---

### N-11. EV 충전소 위치 + 충전기 용량 분석
```
자연어: 충전기 용량 100kW 이상 초급속 충전기가 설치된 충전소 목록과 주소 알려줘
테이블: ev_station_location + ev_charger_capacity
```
```sql
SELECT l.station_name, l.address,
  COUNT(c.charger_id) AS charger_count,
  MAX(c.capacity_kw) AS max_capacity_kw
FROM ev_station_location l
JOIN ev_charger_capacity c ON l.station_id = c.station_id
WHERE c.capacity_kw >= 100
GROUP BY l.station_id, l.station_name, l.address
ORDER BY max_capacity_kw DESC
LIMIT 20;
```

---

### N-12. 판매통계 vs 계약종별 연계 분석
```
자연어: 2024년 시도별 주택용 고객 1인당 전력사용량은?
테이블: sales_stat
```
```sql
SELECT metro,
  SUM(cust_count) AS total_customers,
  ROUND(SUM(power_usage) / NULLIF(SUM(cust_count), 0), 2) AS usage_per_customer_kwh
FROM sales_stat
WHERE SUBSTR(period, 1, 4) = '2024' AND contract_type = '주택용'
GROUP BY metro
ORDER BY usage_per_customer_kwh DESC;
```

---

### N-13. 상계거래 + PPA 지역 융합 분석
```
자연어: 태양광 PPA 계약이 많은 지역과 상계거래 설치 건수가 많은 지역이 일치하는지 비교해줘
테이블: ppa_by_region + net_metering_usage
```
```sql
WITH ppa AS (
  SELECT region, SUM(count) AS ppa_count, ROUND(SUM(capacity_kw)/1000, 2) AS ppa_mw
  FROM ppa_by_region WHERE gen_source = '태양광'
  GROUP BY region
),
net AS (
  SELECT metro, SUM(total_count) AS net_count
  FROM net_metering_usage WHERE year = '2024'
  GROUP BY metro
)
SELECT p.region, p.ppa_count, p.ppa_mw, n.net_count
FROM ppa p
LEFT JOIN net n ON n.metro LIKE '%' || p.region || '%'
ORDER BY p.ppa_mw DESC;
```

---

### N-14. 시간대별 지역 전력수요 + 판매통계 피크 대응
```
자연어: 2024년 전력수요 피크 시간대(11시)에 경기도 전력사용량은 얼마야?
테이블: hourly_power
```
```sql
SELECT date, region, power_usage
FROM hourly_power
WHERE hour = 11
  AND SUBSTR(date, 1, 4) = '2024'
  AND region LIKE '%경기%'
ORDER BY power_usage DESC
LIMIT 10;
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
| S-08 | 단일/단순 | ⭐ | 2024년 태양광 설치 용량 큰 시군구 TOP 10 |
| S-09 | 단일/단순 | ⭐ | 2023년 여름 서울 가구당 평균 전력사용량 |
| S-10 | 단일/단순 | ⭐ | 2023년 제조업 신규 계약 많은 지역 |
| C-01 | 단일/복합 | ⭐⭐ | 2022→2023 산업용 전력 사용량 변화율 |
| C-02 | 단일/복합 | ⭐⭐ | 2023년 계절별 주택용 전력사용량 비교 |
| C-03 | 단일/복합 | ⭐⭐ | 2023년 서울 복지할인 유형별 비율 |
| C-04 | 단일/복합 | ⭐⭐ | 전력사용량 상위 5개 지역 월별 추이 |
| M-01 | 멀티/단순 | ⭐⭐ | 서울 구별 주택용 전력사용량 + 복지할인 |
| M-02 | 멀티/단순 | ⭐⭐ | 시도별 EV 급속/완속 충전소 합계 |
| M-03 | 멀티/단순 | ⭐⭐ | 태양광 상위 지역 전력 자급률 (2024년) |
| MC-01 | 멀티/복합 | ⭐⭐⭐ | 경기도 시군구별 전력·신규고객·복지 종합 |
| MC-02 | 멀티/복합 | ⭐⭐⭐ | 모바일 청구 비율 vs 전력사용량 상관 |
| MC-03 | 멀티/복합 | ⭐⭐⭐ | 태양광+EV 기반 친환경 점수 |
| MC-04 | 멀티/복합 | ⭐⭐⭐ | 전력 급증 지역 신재생에너지 설치 현황 |
| N-01 | 신규/단순 | ⭐ | 2024년 시도별 산업용 판매 수익 TOP 10 |
| N-02 | 신규/단순 | ⭐ | 2024년 시간대별 전국 평균 전력수요 패턴 |
| N-03 | 신규/복합 | ⭐⭐ | 여름·겨울 시간대별 전력수요 비교 |
| N-04 | 신규/단순 | ⭐ | 2024년 상계거래 설치 건수 많은 동 TOP 10 |
| N-05 | 신규/멀티 | ⭐⭐ | 2024년 시도별 상계거래 잉여율 |
| N-06 | 신규/단순 | ⭐ | PPA 에너지원별 연도별 누적 용량 |
| N-07 | 신규/단순 | ⭐ | 태양광 PPA 용량 많은 시군구 TOP 10 |
| N-08 | 신규/단순 | ⭐ | 주택용 전기요금 조정 이력 |
| N-09 | 신규/단순 | ⭐ | 제조업 전력사용량 높은 읍면동 TOP 10 |
| N-10 | 신규/복합 | ⭐⭐ | 수용률 여름·겨울 차이가 큰 업종 |
| N-11 | 신규/멀티 | ⭐⭐ | 초급속(100kW+) 충전기 설치 충전소 목록 |
| N-12 | 신규/복합 | ⭐⭐ | 2024년 시도별 주택용 고객 1인당 전력사용량 |
| N-13 | 신규/멀티 | ⭐⭐⭐ | PPA 집중 지역 vs 상계거래 지역 비교 |
| N-14 | 신규/복합 | ⭐⭐ | 2024년 피크 시간대(11시) 경기도 전력수요 |
