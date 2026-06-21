# 한국전력 공개 데이터 기반 NL2SQL 테스트 시나리오 보고서

내부 검토용 — 2026-06-12

---

## 1. 테스트 개요

| 항목 | 내용 |
|---|---|
| 테스트 목적 | NL2SQL 솔루션의 자연어 → SQL 변환 정확도 검증 |
| 평가 방법 | 자연어 질문 입력 → 생성 SQL 실행 → gold SQL 실행 결과와 비교 (Execution Accuracy) |
| 시나리오 구성 | 단일 테이블 8개 (I-01~I-08) + JOIN 4개 (IJ-01~IJ-04) = 총 12개 |
| gold SQL 기준 | 실제 DB 실행 검증 완료 (I-08 IMPOSSIBLE 제외) |

---

## 2. 대상 데이터

### 2.1 데이터 소개

| 항목 | 내용 |
|---|---|
| 데이터 출처 | 한국전력공사(KEPCO) 전력데이터 개방 포털 |
| 포털 주소 | https://bigdata.kepco.co.kr |
| API 수 | 11종 (평가 대상) |
| 데이터 규모 | 약 637,250행 (11개 테이블) |
| 기간 | 2022년 1월 ~ 2026년 1월 |

### 2.2 테이블 상세

| No. | 테이블명 | 설명 | 행수 | 주요 컬럼 |
|---|---|---|---:|---|
| 1 | `industry_type` | 산업분류별 월·지역 전력사용·요금 | 272,854 | year, month, metro_code, city_code, biz_code, cust_count, power_usage, bill, unit_cost |
| 2 | `welfare_discount` | 복지할인 유형별 월·지역 할인 현황 | 104,536 | year, month, metro_code, city_code, welfare_type, welfare_count |
| 3 | `contract_type` | 계약종별 월·지역 전력사용 | 97,476 | year, month, metro_code, city_code, contract_type, cust_count, power_usage, bill, unit_cost, contract_power |
| 4 | `industry_cust_change` | 산업별 월·지역 고객 신규·해지 증감 | 76,759 | year, month, metro_code, city_code, biz_code, new_count, expansion_count, cancel_count |
| 5 | `billing_type` | 청구서 유형별 월·지역 발송 현황 | 58,734 | year, month, metro_code, city_code, bill_type, bill_count |
| 6 | `ev_charge_manage` | 전기차 충전기 운영정보 | 8,342 | station_id, charger_id, charger_type, status |
| 7 | `dispersed_gen` | 변전소·배전선로별 분산전원 연계 | 7,526 | substation, line, gen_type, capacity, connect_dt |
| 8 | `house_avg` | 월·지역별 가구당 평균 전력사용량 | 5,492 | year, month, metro_code, city_code, house_count, power_usage, bill |
| 9 | `ev_charge` | 전기차 충전소 설치 현황 | 4,623 | station_id, station_name, metro, city, charger_count |
| 10 | `common_code` | 지역·계약·업종 공통코드 | 684 | code_type, code, code_name, upper_code |
| 11 | `renew_energy` | 연도·지역별 신재생에너지 발전원별 현황 | 224 | year, metro_code, city_code, gen_source, capacity, area_capacity |
| | **합계** | | **637,250** | |

### 2.3 스키마 주의사항

- **코드 기반 설계**: metro_code, city_code, biz_code는 코드값 저장. 지역명·업종명 출력 시 `common_code` JOIN 필수
- **city_code 로컬성**: city_code는 광역시도 내 고유 번호 → city JOIN 시 반드시 `AND c.upper_code = t.metro_code` 조건 추가
- **house_avg.power_usage**: 이미 가구당 평균(kWh) — 재나눗셈 금지, `AVG(power_usage)` 사용
- **renew_energy.area_capacity**: 광역시도 전체 설비 용량 합계(kW) — `MAX(area_capacity)`로 집계
- **집계행 없음**: totData(전국 합계행) 미적재 — 전국 합계는 SUM으로 직접 계산

---

## 3. 테스트 시나리오

### 3.1 단일 테이블 시나리오 (I-01 ~ I-08)

#### I-01. 특정 월 산업별 전력사용량 전국 합계 [BASIC+SCHEMA]

**자연어**: 2024년 3월 제조업 전력사용량 전국 총합은?

**평가 포인트**
- common_code 서브쿼리로 biz_code 조회 (코드값 직접 하드코딩 금지)
- year/month TEXT 타입 처리 ('2024', '03')

**gold SQL**
```sql
SELECT SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024'
  AND month = '03'
  AND biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업');
```

| total_usage |
|---|
| 22,020,974,768.0 |

---

#### I-02. 연간 전력사용량 TOP 5 산업 [BASIC+RANK]

**자연어**: 2024년 한 해 동안 전력을 가장 많이 쓴 산업 TOP 5

**평가 포인트**
- GROUP BY + ORDER BY DESC + LIMIT 5
- 산업명 출력 시 common_code JOIN

**gold SQL**
```sql
SELECT b.code_name AS biz,
       SUM(t.power_usage) AS total_usage
FROM industry_type t
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
GROUP BY t.biz_code
ORDER BY total_usage DESC
LIMIT 5;
```

| biz | total_usage |
|---|---|
| 제조업 | 256,532,402,896.0 |
| 부동산업 | 42,852,444,517.0 |
| 도매 및 소매업 | 20,027,041,441.0 |
| 농업, 임업 및 어업 | 19,045,847,475.0 |
| 숙박 및 음식점업 | 17,128,659,618.0 |

---

#### I-03. 특정 산업 전력사용량 광역시도 TOP 3 [RANK]

**자연어**: 2024년 건설업 전력사용량이 가장 많은 광역시도 TOP 3

**평가 포인트**
- common_code 서브쿼리로 biz_code 조회 (건설업)
- metro_code 기준 GROUP BY — city_code 오선택 여부 관찰

**gold SQL**
```sql
SELECT m.code_name AS metro,
       SUM(t.power_usage) AS total_usage
FROM industry_type t
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = t.metro_code
WHERE t.year = '2024'
  AND t.biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '건설업')
GROUP BY t.metro_code
ORDER BY total_usage DESC
LIMIT 3;
```

| metro | total_usage |
|---|---|
| 경기도 | 1,328,295,729.0 |
| 서울특별시 | 692,278,457.0 |
| 인천광역시 | 270,375,761.0 |

---

#### I-04. 전년 대비 전력사용량 증감률 [TIME]

**자연어**: 2023년 대비 2024년 제조업 전력사용량 증감률은?

**평가 포인트**
- CASE WHEN 연도 피벗
- 정수 나눗셈 방지 (× 100.0)

**gold SQL**
```sql
SELECT
  SUM(CASE WHEN year = '2023' THEN power_usage END) AS usage_2023,
  SUM(CASE WHEN year = '2024' THEN power_usage END) AS usage_2024,
  ROUND(
    (SUM(CASE WHEN year = '2024' THEN power_usage END)
     - SUM(CASE WHEN year = '2023' THEN power_usage END))
    * 100.0 / SUM(CASE WHEN year = '2023' THEN power_usage END),
    2
  ) AS yoy_pct
FROM industry_type
WHERE biz_code = (SELECT code FROM common_code WHERE code_type = 'bizCd' AND code_name = '제조업')
  AND year IN ('2023', '2024');
```

| usage_2023 | usage_2024 | yoy_pct |
|---|---|---|
| 260,881,303,843.0 | 256,532,402,896.0 | -1.67 |

---

#### I-05. 특정 산업 계절별 전력사용량 비교 [TIME]

**자연어**: 2024년 전국 숙박 및 음식점업의 계절별(봄/여름/가을/겨울) 전력사용량 비교

**평가 포인트**
- 월 → 계절 CASE WHEN 로직
- common_code 서브쿼리로 biz_code 조회 (숙박 및 음식점업)

**gold SQL**
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
| 여름 | 4,792,374,405.0 |
| 가을 | 4,388,387,206.0 |
| 겨울 | 4,339,067,140.0 |
| 봄 | 3,608,830,867.0 |

---

#### I-06. 산업별 평균 판매단가 TOP 5 [BASIC+RANK]

**자연어**: 2024년 산업별 평균 판매단가(원/kWh)가 가장 비싼 산업 TOP 5

**평가 포인트**
- `ROUND(SUM(bill) / NULLIF(SUM(power_usage), 0), 2)` 가중평균 사용
- `AVG(unit_cost)` 직접 사용 금지

**gold SQL**
```sql
SELECT b.code_name AS biz,
       ROUND(SUM(t.bill) / NULLIF(SUM(t.power_usage), 0), 2) AS avg_unit_cost
FROM industry_type t
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
GROUP BY t.biz_code
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

#### I-07. 특정 지역 산업별 전력사용량 점유율 [RANK]

**자연어**: 2024년 서울특별시에서 각 산업별 전력사용량 점유율(%)을 보여줘

**평가 포인트**
- `SUM() OVER()` 윈도우 함수 필수
- metro_code를 서브쿼리로 조회 (코드값 직접 하드코딩 금지)

**gold SQL**
```sql
SELECT b.code_name AS biz,
       SUM(t.power_usage) AS usage,
       ROUND(SUM(t.power_usage) * 100.0 / SUM(SUM(t.power_usage)) OVER (), 2) AS share_pct
FROM industry_type t
JOIN common_code b ON b.code_type = 'bizCd' AND b.code = t.biz_code
WHERE t.year = '2024'
  AND t.metro_code = (SELECT code FROM common_code WHERE code_type = 'metroCd' AND code_name = '서울특별시')
GROUP BY t.biz_code
ORDER BY usage DESC;
```

| biz | usage | share_pct |
|---|---|---|
| 부동산업 | 13,078,656,390.0 | 38.37 |
| 도매 및 소매업 | 3,462,580,525.0 | 10.16 |
| 숙박 및 음식점업 | 2,414,902,189.0 | 7.09 |
| 운수 및 창고업 | 2,231,346,030.0 | 6.55 |
| 정보통신업 | 2,162,274,507.0 | 6.34 |

---

#### I-08. 존재하지 않는 컬럼 질문 거절 [IMPOSSIBLE]

**자연어**: 2024년 제조업의 전국 계약전력 총합은 얼마야?

**평가 포인트**
- `industry_type` 테이블에 `contract_power` 컬럼 없음 → 거절 또는 오류 안내 필수
- 실행 가능한 SQL을 생성하거나 환각 결과를 반환하면 실패

**판정 기준**: SQL 생성 없이 컬럼 부재를 안내하면 통과 / SQL 생성 시 실패

---

### 3.2 JOIN 시나리오 (IJ-01 ~ IJ-04)

#### IJ-01. 전력사용 증가 TOP 5 광역시도 + 신규 고객 수 [JOIN+TIME]

**자연어**: 2024년 제조업에서 전력사용량이 가장 많이 늘어난 광역시도 TOP 5와, 같은 기간 해당 지역의 제조업 신규 고객 수를 같이 보여줘

**평가 포인트**
- CTE 3개 구성 (2024년 사용량 / 2023년 사용량 / 신규고객)
- `COALESCE` NULL 처리
- `LEFT JOIN` 필수 (데이터 없는 지역 포함)

**gold SQL**
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
| 경기도 | 569,162,000.0 | 1,001,033 |
| 충청남도 | 115,615,915.0 | 0 |
| 세종특별자치시 | 63,000,499.0 | 17,543 |
| 광주광역시 | 43,842,136.0 | 0 |
| 제주특별자치도 | 3,279,975.0 | 0 |

---

#### IJ-02. 광역시도별 제조업 전력사용량 대비 가구당 평균 전력사용량 배율 [JOIN+BASIC]

**자연어**: 2024년 광역시도별로 제조업 전력사용량이 가구당 평균 전력사용량의 몇 배인지 계산해줘

**평가 포인트**
- 서로 다른 집계 단위 JOIN (산업별 총합 vs 가구당 평균)
- `house_avg.power_usage`는 이미 가구당 평균(kWh) → `AVG(power_usage)` 사용
- `NULLIF` 0 나눗셈 방지

**gold SQL**
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

| metro | ind_usage | avg_house_usage | ratio |
|---|---|---|---|
| 경기도 | 68,684,716,267.0 | 262.46 | 261,700,679.5 |
| 울산광역시 | 25,742,969,854.0 | 254.10 | 101,311,651.6 |
| 부산광역시 | 6,708,004,876.0 | 241.55 | 27,770,967.5 |
| 세종특별자치시 | 1,872,035,526.0 | 294.69 | 6,352,630.6 |
| 서울특별시 | 1,349,813,098.0 | 245.42 | 5,500,023.8 |

> house_avg가 적재된 광역시도만 출력됨 (서울·부산·울산·경기·세종 5개 — API 제공 범위)

---

#### IJ-03. 광역시도별 전체 산업 전력사용량 대비 신재생에너지 설비 비율 TOP 5 [JOIN+RANK]

**자연어**: 2024년 광역시도별 전체 산업 전력사용량 대비 신재생에너지 설비 총 용량 비율 TOP 5

**평가 포인트**
- `renew_energy.area_capacity` 사용 (광역시도 전체 설비 용량, kW)
- 단위 변환 필수 (전력사용량 kWh vs 설비용량 kW → × 1,000,000)
- `LEFT JOIN`으로 신재생 없는 지역 포함

**gold SQL**
```sql
WITH ind AS (
  SELECT metro_code, SUM(power_usage) AS ind_usage
  FROM industry_type
  WHERE year = '2024'
  GROUP BY metro_code
),
renew AS (
  SELECT metro_code, MAX(area_capacity) AS renew_cap
  FROM renew_energy
  WHERE year = '2024'
  GROUP BY metro_code
)
SELECT m.code_name AS metro,
       i.ind_usage,
       r.renew_cap,
       ROUND(r.renew_cap * 1.0 / NULLIF(i.ind_usage, 0) * 1e6, 3) AS renew_per_mwh
FROM ind i
LEFT JOIN renew r ON i.metro_code = r.metro_code
JOIN common_code m ON m.code_type = 'metroCd' AND m.code = i.metro_code
ORDER BY renew_per_mwh DESC
LIMIT 5;
```

| metro | ind_usage | renew_cap (kW) | renew_per_mwh |
|---|---|---|---|
| 전북특별자치도 | 18,117,975,760.0 | 733,913.31 | 40.507 |
| 경상북도 | 38,604,924,993.0 | 1,454,747.43 | 37.683 |
| 전라남도 | 30,108,759,252.0 | 903,097.68 | 29.995 |
| 강원특별자치도 | 12,822,974,897.0 | 383,089.53 | 29.875 |
| 광주광역시 | 6,754,968,047.0 | 169,876.74 | 25.148 |

---

#### IJ-04. 제조업 고객 많은 상위 5 시군구의 전자/우편 청구서 발송건수 [JOIN+SCHEMA]

**자연어**: 2024년 제조업 고객이 많은 상위 5개 시군구에서, 전자청구서(이메일/모바일/카카오) 및 우편 청구서 발송 건수를 함께 보여줘

**평가 포인트**
- metro_code + city_code 복합키 JOIN
- bill_type 실제 값 사용 ('이메일', '모바일', '카카오', '우편') — '종이', 'e-bill' 등 환각 여부 관찰
- `LEFT JOIN`으로 청구서 데이터 없는 지역 포함

**gold SQL**
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
| 경기도 | 화성시 | 272,694 | 2,137,097 | 1,575,720 |
| 경기도 | 김포시 | 146,623 | 971,923 | 748,498 |
| 경상남도 | 김해시 | 111,118 | 1,392,662 | 1,166,526 |
| 경기도 | 시흥시 | 109,756 | 1,111,719 | 950,626 |
| 경기도 | 포천시 | 99,649 | 787,806 | 837,917 |

---

## 4. 평가 항목 설명

| 평가 항목 | 설명 |
|---|---|
| **BASIC** | WHERE / GROUP BY / 기본 집계 — NL2SQL의 가장 기초적인 변환 능력 |
| **RANK** | ORDER BY + LIMIT / 윈도우 함수 — 순위·점유율 계산 능력 |
| **TIME** | 연도 비교(YoY) / 계절 집계 — 시계열 데이터 처리 능력 |
| **SCHEMA** | 코드 기반 필터링(biz_code, metro_code) / 컬럼 오선택 방지 — 스키마 이해도 |
| **IMPOSSIBLE** | 존재하지 않는 컬럼 질문 거절 — 환각(hallucination) 방지 능력 |
| **JOIN** | CTE + 복합 JOIN / 부분 집계 — 다중 테이블 연결 능력 |

---

## 5. 원본 문서 대비 변경 사항

원본 시나리오 문서(내부 검토본)의 DB 스키마가 현재 구축된 `kepco.db`와 다음 항목에서 차이가 있어 gold SQL을 전면 재작성하였습니다.

### 5.1 스키마 차이

| 항목 | 원본 가정 | kepco.db 실제 |
|---|---|---|
| 지역 컬럼 | `metro` (텍스트 직접 저장) | `metro_code` (코드값) + `common_code` JOIN |
| 업종 컬럼 | `biz` (텍스트 직접 저장) | `biz_code` (코드값) + `common_code` JOIN |
| 시군구 컬럼 | `city` (텍스트 직접 저장) | `city_code` (코드값) + `common_code` JOIN |
| 복지할인 유형 | `discount_type` | `welfare_type` |
| 고객증감 컬럼 | `expand_count` | `expansion_count` |
| 신재생 설비용량 | `capacity` (SUM 사용) | `area_capacity` (MAX 사용, 이미 지역 합계) |
| 집계행 포함 | '전체' metro 행 포함 | 미적재 (totData 제거) |

### 5.2 gold SQL 수정 사항

| 시나리오 | 수정 내용 |
|---|---|
| I-01~I-07 전체 | metro/biz 텍스트 필터 → metro_code/biz_code 코드 필터로 교체, common_code JOIN 추가 |
| IJ-01 | `expand_count` → `expansion_count` |
| IJ-02 | `AVG(power_usage / house_count)` → `AVG(power_usage)` (house_avg.power_usage는 이미 가구당 평균) |
| IJ-03 | `SUM(capacity)` → `MAX(area_capacity)` (광역시도 합계 컬럼 사용) |
| I-03 | gold 결과에서 '전체' 집계행 제거 (우리 DB에 미적재) |
