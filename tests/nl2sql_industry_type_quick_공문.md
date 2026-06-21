# NL2SQL 성능 평가 시나리오(`industry_type`) 송부의 건

| 구분 | 내용 |
|------|------|
| 문서번호 | 데이터사업-2026-000호 |
| 시행일자 | 2026. 04. 14. |
| 수　　신 | NL2SQL 파이프라인 구축 수행업체 담당자 |
| 참　　조 | 프로젝트 PM, 품질관리자 |
| 발　　신 | 한국전력공사 데이터사업 담당 부서 |
| 제　　목 | 「NL2SQL 빠른 성능 평가 시나리오(`industry_type`)」 송부 및 회신 요청 |

---

## 1. 관련근거

 가. 「자연어-SQL 변환 파이프라인 구축 용역」 과업지시서 제○조(성능 평가)
 나. 「NL2SQL 품질 평가 기준」 내부 지침
 다. 프로젝트 착수회의(2026. 03. ○○.) 합의사항

## 2. 목적

 본 시나리오는 한국표준산업분류(KSIC) 대분류 기반 `industry_type` 테이블을 대상으로, 수행업체가 개발 중인 NL2SQL 모델의 **SQL 생성 정확도**를 10분 이내 신속히 점검하기 위함임.

## 3. 평가 개요

 가. **대상 테이블**: `industry_type`(KSIC 대분류별 전력사용량·요금 통계)
 나. **구성**: 단일 테이블 시나리오 8건(I-01 ~ I-08) + 다중 테이블 JOIN 시나리오 4건(IJ-01 ~ IJ-04), **총 12건**
 다. **데이터 범위**: 2021. 01. ~ 2026. 02.(총 274,138행)
 라. **평가 축**: BASIC / RANK / TIME / SCHEMA / IMPOSSIBLE / JOIN 등 6개 축

## 4. 대상 테이블 명세

  ### 4.1 `industry_type` 컬럼 정의

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| `year` | TEXT | 연도(`2021` ~ `2026`) |
| `month` | TEXT | 월(`01` ~ `12`) |
| `metro` | TEXT | 광역시도명 |
| `city` | TEXT | 시군구명 |
| `biz` | TEXT | 산업 분류명(예: `제조업`, `건설업`, `숙박 및 음식점업`) |
| `cust_count` | INTEGER | 고객 수 |
| `power_usage` | REAL | 전력 사용량(kWh) |
| `bill` | REAL | 전기요금(원) |
| `unit_cost` | REAL | 단가(원/kWh) |

  ### 4.2 데이터 특성 및 유의사항(평가 시 함정 요소)

| 구분 | 세부 내용 |
|------|-----------|
| 산업명 변형 | 동일 산업이라도 공백·슬래시(/)·쉼표(,) 사용이 상이(예: `농업, 임업 및 어업`, `농업/ 임업 및 어업 `). 반드시 **`LIKE` 연산자 사용 필수** |
| 지역명 통일성 | 본 테이블은 대부분 `강원특별자치도`로 통일되어 있으나, 타 테이블 JOIN 시 `강원도`와의 매핑에 유의 |
| 요금 정보 포함 | `business_type`과 달리 `bill`, `unit_cost` 컬럼 존재 → 요금 분석 가능 |
| 계약전력 없음 | `contract_power` 컬럼 미존재 → 해당 질문은 답변 불가로 처리 |
| 2026년 부분 데이터 | 1~2월까지만 적재 |

  ### 4.3 JOIN 파트너 테이블

| 테이블 | JOIN 키 | 용도 |
|--------|---------|------|
| `industry_cust_change` | `year, month, metro, city, biz` | 산업별 신규/증설/해지 고객 수 |
| `house_avg` | `year, month, metro, city` | 가구당 평균 전력·요금(산업 대비 가정 비교) |
| `renew_energy` | `year, metro, city` | 지역별 신재생 발전원 설비 용량 |
| `billing_type` | `year, month, metro, city` | 청구서 유형별 발송 건수 |

---

## 5. 평가 시나리오

### 5.1 단일 테이블 시나리오(8건)

#### 시나리오 I-01 (난이도 ⭐, 평가축 BASIC + SCHEMA)

 **자연어 질의**
```
2024년 3월 제조업 전력사용량 전국 총합은?
```
 **기준 SQL**
```sql
SELECT SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024' AND month = '03' AND biz LIKE '%제조업%';
```
 **평가 포인트**: 정확한 WHERE 필터 구성, 명칭 변형 대응을 위한 LIKE 사용 여부

---

#### 시나리오 I-02 (난이도 ⭐, 평가축 BASIC + RANK)

 **자연어 질의**
```
2024년 한 해 동안 전력을 가장 많이 쓴 산업 TOP 5 알려줘
```
 **기준 SQL**
```sql
SELECT biz, SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024'
GROUP BY biz
ORDER BY total_usage DESC
LIMIT 5;
```
 **평가 포인트**: GROUP BY + ORDER BY + LIMIT 조합의 정확성

---

#### 시나리오 I-03 (난이도 ⭐⭐, 평가축 RANK)

 **자연어 질의**
```
2024년 건설업 전력사용량이 가장 많은 광역시도 TOP 3
```
 **기준 SQL**
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

#### 시나리오 I-04 (난이도 ⭐⭐, 평가축 TIME)

 **자연어 질의**
```
2023년 대비 2024년 제조업 전력사용량 증감률은?
```
 **기준 SQL**
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
 **평가 포인트**: 연도 비교를 위한 조건부 집계 구성 능력

---

#### 시나리오 I-05 (난이도 ⭐⭐, 평가축 TIME)

 **자연어 질의**
```
2024년 전국 숙박 및 음식점업의 계절별(봄/여름/가을/겨울) 전력사용량 비교
```
 **기준 SQL**
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
 **평가 포인트**: 월 단위 데이터를 계절 단위로 재그룹화하는 로직 정확성

---

#### 시나리오 I-06 (난이도 ⭐⭐, 평가축 BASIC + RANK)

 **자연어 질의**
```
2024년 산업별 평균 판매단가(원/kWh)가 가장 비싼 산업 TOP 5
```
 **기준 SQL**
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
 **평가 포인트**: 요금/사용량 기반 파생지표 계산 및 0 나눗셈 방지 구현

---

#### 시나리오 I-07 (난이도 ⭐⭐⭐, 평가축 RANK)

 **자연어 질의**
```
2024년 서울특별시에서 각 산업별 전력사용량 점유율(%)을 보여줘
```
 **기준 SQL**
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
 **평가 포인트**: 윈도우 함수(`SUM() OVER`) 활용을 통한 비율 계산 능력

---

#### 시나리오 I-08 (난이도 ⭐⭐⭐, 평가축 IMPOSSIBLE)

 **자연어 질의**
```
2024년 제조업의 전국 계약전력 총합은 얼마야?
```
 **기준 응답**
```sql
-- 정답: industry_type 테이블에는 'contract_power' 컬럼이 존재하지 않음.
-- 모델은 "해당 테이블에 계약전력 정보 없음"으로 응답하거나,
--          business_type / contract_type 테이블 사용을 제안하여야 함.
-- 오답 예시: SELECT SUM(contract_power) FROM industry_type ... (존재하지 않는 컬럼 사용)
```
 **평가 포인트**: 환각(hallucination) 방지, 스키마 한계 인식 및 정중한 거절 능력

---

### 5.2 다중 테이블 JOIN 시나리오(4건)

#### 시나리오 IJ-01 (난이도 ⭐⭐, 평가축 JOIN + TIME)

 **자연어 질의**
```
2024년 제조업에서 전력사용량이 가장 많이 늘어난 광역시도 TOP 5와,
같은 기간 해당 지역의 제조업 신규 고객 수를 같이 보여줘
```
 **기준 SQL**
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
 **평가 포인트**: 복수 CTE 구성, 5개 JOIN 키 중 일부(`metro`, `biz`)만 사용한 부분 집계 JOIN 구현 능력

---

#### 시나리오 IJ-02 (난이도 ⭐⭐, 평가축 JOIN + BASIC)

 **자연어 질의**
```
2024년 광역시도별로 제조업 전력사용량이 가구당 평균 전력사용량의
몇 배인지 계산해줘 (광역시도별 연간 합계 기준)
```
 **기준 SQL**
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
 **평가 포인트**: 서로 다른 집계 단위(총합 대비 평균) JOIN 및 가구당 계산 정확성

---

#### 시나리오 IJ-03 (난이도 ⭐⭐⭐, 평가축 JOIN + RANK)

 **자연어 질의**
```
2024년 광역시도별 전체 산업 전력사용량 대비 신재생에너지 설비 총 용량의
비율을 계산하고, 신재생 비중이 높은 지역 TOP 5를 보여줘
```
 **기준 SQL**
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
 **평가 포인트**: 서로 다른 단위(kWh 대비 kW)의 비율 계산, LEFT JOIN을 통한 신재생 미보유 지역의 포함 여부

---

#### 시나리오 IJ-04 (난이도 ⭐⭐⭐, 평가축 JOIN + SCHEMA)

 **자연어 질의**
```
2024년 제조업 고객이 많은 상위 5개 시군구에서, 해당 지역의 전체
전자청구서(이메일/모바일/카카오) 발송 건수와 우편 청구서 발송 건수를 함께 보여줘
```

 ※ `bill_type` 실제 값: `우편`, `이메일`, `모바일`, `카카오`, `인편`, `''`(빈값)
 ※ 모델이 `종이`, `e-bill` 등 존재하지 않는 값을 환각하는지 관찰 필요

 **기준 SQL**
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
 **평가 포인트**
  가. `metro` + `city` 복합키 JOIN 정확성
  나. `bill_type` 실제 값 사용 여부(`종이` 등 환각 값 사용 여부 관찰)
  다. 산업별 필터와 전체 청구서 집계 간 범위 차이에 대한 이해도

---

## 6. 수행업체 요청사항

 가. 상기 12개 시나리오에 대하여 자사 NL2SQL 모델의 **생성 SQL 및 실행 결과**를 본 양식에 병기하여 회신하여 주시기 바랍니다.
 나. 기준 SQL과 상이한 경우에도 **논리적 동치(equivalent) 여부**를 판정할 수 있도록, 실행 결과(행 수·상위 5행 샘플)를 함께 제출하여 주시기 바랍니다.
 다. 시나리오 **I-08(답변 거절)** 의 경우, 모델이 생성한 응답 원문을 그대로 회신하여 주시기 바랍니다.
 라. 회신 기한: **2026. 04. 21.(화) 18:00 까지**
 마. 회신 방법: 전자우편 또는 프로젝트 공유 저장소 업로드

## 7. 붙임

 1. `tests/nl2sql_industry_type_quick.md` (원본 평가 시나리오) …… 1부.
 2. `tests/nl2sql_industry_type_quick_answers.md` (기준 실행 결과) …… 1부. 끝.

---

> ※ 본 문서는 NL2SQL 파이프라인 품질 점검을 위한 내부 평가용 자료이며,
> 수행업체는 본 시나리오 외 추가 테스트 케이스 제안을 자유롭게 송부할 수 있습니다.
