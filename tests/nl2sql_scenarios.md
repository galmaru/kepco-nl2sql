# NL2SQL 테스트 시나리오

한전 OPEN API 기반 SQLite DB (`kepco.db`) 대상 자연어 → SQL 변환 테스트

> **스키마 기준**: 지역명은 `metro_code`/`city_code` 코드 컬럼으로 저장.
> 지역명 표시 시 `common_code` JOIN 필요. city_code는 광역시도 내 고유값이므로
> city JOIN에는 반드시 `AND c.upper_code = <table>.metro_code` 조건을 추가.
>
> `metro_code` 주요값: 11=서울특별시, 21=부산광역시, 22=대구광역시, 23=인천광역시, 24=광주광역시, 25=대전광역시, 26=울산광역시, 31=경기도, 32=강원특별자치도, 33=충청북도, 34=충청남도, 35=전북특별자치도, 36=전라남도, 37=경상북도, 38=경상남도, 39=제주특별자치도, 41=세종특별자치시.
> `biz_code` 주요값: A=농업·임업·어업, B=광업, C=제조업, D=전기·가스, E=수도·하수, F=건설업, G=도소매, H=운수·창고, I=숙박·음식점, J=정보통신, K=금융·보험, L=부동산, M=전문·과학기술, N=사업시설관리, O=공공행정, P=교육, Q=보건복지, R=예술·스포츠, S=협회·단체, T=가구내고용, U=국제기관.

---

## 단일 테이블 시나리오 (I-01 ~ I-08)

### I-01. 특정 월 산업별 전력사용량 전국 합계 [BASIC+SCHEMA]

```
자연어: 2024년 3월 제조업 전력사용량 전국 총합은?
평가 포인트: common_code 서브쿼리로 biz_code 조회, year/month TEXT 타입 처리
테이블: industry_type, common_code
```
```sql
SELECT SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024'
  AND month = '03'
  AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업');
```
| total_usage |
|---|
| 22020974768.0 |

---

### I-02. 연간 전력사용량 TOP 5 산업 [BASIC+RANK]

```
자연어: 2024년 한 해 동안 전력을 가장 많이 쓴 산업 TOP 5
평가 포인트: GROUP BY + ORDER BY DESC + LIMIT 5, 산업명 출력 시 common_code JOIN
테이블: industry_type, common_code
```
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
| biz | total_usage |
|---|---|
| 제조업 | 256532402896.0 |
| 부동산업 | 42852444517.0 |
| 도매 및 소매업 | 20027041441.0 |
| 농업, 임업 및 어업 | 19045847475.0 |
| 숙박 및 음식점업 | 17128659618.0 |

---

### I-03. 특정 산업 전력사용량 광역시도 TOP 3 [RANK]

```
자연어: 2024년 건설업 전력사용량이 가장 많은 광역시도 TOP 3
평가 포인트: common_code 서브쿼리로 biz_code 조회, metro_code 기준 GROUP BY, city_code 오선택 여부 관찰
테이블: industry_type, common_code
```
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
| metro | total_usage |
|---|---|
| 경기도 | 1328295729.0 |
| 서울특별시 | 692278457.0 |
| 인천광역시 | 270375761.0 |

---

### I-04. 전년 대비 전력사용량 증감률 [TIME]

```
자연어: 2023년 대비 2024년 제조업 전력사용량 증감률은?
평가 포인트: CASE WHEN 연도 피벗, 정수 나눗셈 방지 (* 100.0)
테이블: industry_type
```
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
| yoy_pct |
|---------|
| -1.67 |

---

### I-05. 특정 산업 계절별 전력사용량 비교 [TIME]

```
자연어: 2024년 전국 숙박 및 음식점업의 계절별 전력사용량 비교
평가 포인트: 월 → 계절 CASE WHEN 로직, common_code 서브쿼리로 biz_code 조회
테이블: industry_type, common_code
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
WHERE year = '2024'
  AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '숙박 및 음식점업')
GROUP BY season
ORDER BY total_usage DESC;
```
| season | total_usage |
|---|---|
| 여름 | 4792374405.0 |
| 가을 | 4388387206.0 |
| 겨울 | 4339067140.0 |
| 봄 | 3608830867.0 |

---

### I-06. 산업별 평균 판매단가 TOP 5 [BASIC+RANK]

```
자연어: 2024년 산업별 평균 판매단가(원/kWh)가 가장 비싼 산업 TOP 5
평가 포인트: NULLIF 0 나눗셈 방지, AVG(unit_cost) 직접 사용 금지
테이블: industry_type, common_code
```
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
| biz | avg_unit_cost |
|---|---|
| 가구 내 고용활동 및 달리 분류되지 않은 자가 소비 생산활동 | 202.77 |
| 광업 | 189.11 |
| 협회 및 단체, 수리 및 기타 개인 서비스업 | 184.92 |
| 사업시설 관리, 사업 지원 및 임대 서비스업 | 184.44 |
| 건설업 | 182.24 |

---

### I-07. 특정 지역 산업별 전력사용량 점유율 [RANK]

```
자연어: 2024년 서울특별시에서 각 산업별 전력사용량 점유율(%)을 보여줘
평가 포인트: SUM() OVER() 윈도우 함수 필수, metro_code 서브쿼리로 조회
테이블: industry_type, common_code
```
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
| biz | share_pct |
|-----|-----------|
| 부동산업 | 38.37 |
| 도매 및 소매업 | 10.16 |
| 숙박 및 음식점업 | 7.09 |
| 운수 및 창고업 | 6.55 |
| 정보통신업 | 6.34 |

---

### I-08. 존재하지 않는 컬럼 질문 거절 [IMPOSSIBLE]

```
자연어: 2024년 제조업의 전국 계약전력 총합은 얼마야?
평가 포인트: industry_type에 contract_power 컬럼 없음 → 거절 또는 컬럼 오류 필수
            정상 실행 가능한 SQL을 반환하거나 환각 결과를 출력하면 실패
테이블: industry_type
```
```sql
-- IMPOSSIBLE: industry_type 테이블에 contract_power 컬럼 없음
-- 올바른 응답: 존재하지 않는 컬럼임을 안내하고 SQL 생성 거부
-- 오답 예시: SELECT SUM(contract_power) FROM industry_type WHERE biz_code = 'C'
```

---

## JOIN 시나리오 (IJ-01 ~ IJ-04)

### IJ-01. 전력사용 증가 TOP 5 광역시도 + 신규 고객 수 [JOIN+TIME]

```
자연어: 2024년 제조업에서 전력사용량이 가장 많이 늘어난 광역시도 TOP 5와, 같은 기간 해당 지역의 제조업 신규 고객 수를 같이 보여줘
평가 포인트: 복수 CTE 구성, COALESCE NULL 처리, LEFT JOIN 필수
테이블: industry_type, industry_cust_change, common_code
```
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
| metro | usage_increase | new_cust_count |
|---|---|---|
| 경기도 | 569162000.0 | 1001033 |
| 충청남도 | 115615915.0 | 0 |
| 세종특별자치시 | 63000499.0 | 17543 |
| 광주광역시 | 43842136.0 | 0 |
| 제주특별자치도 | 3279975.0 | 0 |

---

### IJ-02. 광역시도별 제조업 전력사용량 대비 가구당 평균 전력사용량 배율 [JOIN+BASIC]

```
자연어: 2024년 광역시도별로 제조업 전력사용량이 가구당 평균 전력사용량의 몇 배인지 계산해줘
평가 포인트: 가구당 평균 = 총전력사용량/총가구수 (가구 수 가중평균), NULLIF 적용
테이블: industry_type, house_avg, common_code
```
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
| metro | ratio |
|---|---|
| 경기도 | 254691615.39 |
| 울산광역시 | 101640402.65 |
| 부산광역시 | 26918528.39 |
| 세종특별자치시 | 6349836.18 |
| 서울특별시 | 5480852.48 |

---

### IJ-03. 광역시도별 산업용 계약전력 대비 신재생에너지 설비용량 비율(%) TOP 5 [JOIN+RANK]

```
자연어: 2024년 광역시도별 산업용 계약전력(kW) 대비 신재생에너지 설비용량(kW) 비율(%)이 높은 상위 5개 지역을 보여줘
평가 포인트: contract_type='산업용' 필터, renew_energy는 MAX(area_capacity) GROUP BY metro_code,
            동일 단위(kW/kW) 백분율 계산(×100), LEFT JOIN으로 신재생 없는 지역 포함
테이블: contract_type, renew_energy, common_code
```
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
| metro | total_contract_kw | renew_cap_kw | renew_ratio_pct |
|---|---|---|---|
| 전북특별자치도 | 67909309.0 | 733913.31 | 1.08 |
| 강원특별자치도 | 44816844.0 | 383089.53 | 0.85 |
| 경상북도 | 171281775.0 | 1454747.43 | 0.85 |
| 전라남도 | 112826209.0 | 903097.68 | 0.8 |
| 광주광역시 | 22794544.0 | 169876.74 | 0.75 |

---

### IJ-04. 제조업 고객 많은 상위 5 시군구의 전자/우편 청구서 발송건수 [JOIN+SCHEMA]

```
자연어: 2024년 제조업 고객이 많은 상위 5개 시군구에서, 전자청구서(이메일/모바일/카카오) 및 우편 청구서 발송 건수를 함께 보여줘
평가 포인트: metro_code+city_code 복합키 JOIN, bill_type 실제 값 사용 ('이메일','모바일','카카오','우편')
테이블: industry_type, billing_type, common_code
```
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
| metro | city | mfg_cnt | e_bill | paper_bill |
|---|---|---|---|---|
| 경기도 | 화성시 | 272694 | 2137097 | 1575720 |
| 경기도 | 김포시 | 146623 | 971923 | 748498 |
| 경상남도 | 김해시 | 111118 | 1392662 | 1166526 |
| 경기도 | 시흥시 | 109756 | 1111719 | 950626 |
| 경기도 | 포천시 | 99649 | 787806 | 837917 |
