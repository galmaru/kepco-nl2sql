# KEPCO DB Metadata and Data Quality Audit

점검 대상: `data/kepco.db`  
점검일: 2026-06-04  
목적: KEPCO NL2SQL에 사용할 SQLite DB의 메타데이터 품질, 적재 완전성, 값 정합성, 자연어 질의 사용 위험요소를 확인한다.

## 1. 점검 절차 요약

이번 점검은 다음 순서로 진행했다.

1. DB 파일과 스키마 확인
2. SQLite 무결성 확인
3. 테이블별 행 수와 컬럼 수 확인
4. 기간 컬럼의 범위 확인
5. NULL과 빈 문자열 확인
6. 수치 컬럼의 음수 값 확인
7. 자연키 후보 기준 중복 확인
8. 지역명과 카테고리 값의 정규화 상태 확인
9. 이상값의 실제 샘플 행 확인
10. NL2SQL 사용 시 위험도와 정제 필요사항 정리

## 2. 사용한 대표 점검 쿼리

### 2.1 스키마 확인

```sql
.schema
```

확인 목적:

- 현재 DB에 실제로 어떤 테이블이 적재되어 있는지 확인
- 컬럼명, 타입, 테이블 수 확인
- 기존 문서 또는 예상 적재 대상과 실제 DB 상태가 일치하는지 확인

확인 결과:

- 현재 `kepco.db`에는 API 기반 13개 테이블만 존재한다.
- 파일데이터셋 기반 테이블은 현재 DB에 적재되어 있지 않다.

현재 확인된 테이블:

| table | row_count | column_count |
|---|---:|---:|
| billing_type | 59,042 | 6 |
| business_type | 471,668 | 8 |
| common_code | 643 | 5 |
| contract | 58,577 | 22 |
| contract_type | 97,903 | 10 |
| dispersed_gen | 8,339 | 14 |
| ev_charge | 4,623 | 7 |
| ev_charge_manage | 8,342 | 12 |
| house_avg | 5,492 | 7 |
| industry_cust_change | 76,759 | 8 |
| industry_type | 274,138 | 9 |
| renew_energy | 224 | 8 |
| welfare_discount | 104,536 | 6 |

총 행 수: 1,170,286건  
DB 파일 크기: 약 94MB

### 2.2 DB 무결성 확인

```sql
PRAGMA integrity_check;
```

확인 결과:

```text
ok
```

판단:

- SQLite 파일 자체의 물리적 손상은 발견되지 않았다.
- 이후 발견된 문제는 DB 파일 손상이 아니라 적재 범위, 값 정규화, 원천 데이터 품질 문제로 보는 것이 적절하다.

### 2.3 테이블별 건수 확인

```sql
SELECT COUNT(*) FROM billing_type;
SELECT COUNT(*) FROM business_type;
SELECT COUNT(*) FROM common_code;
SELECT COUNT(*) FROM contract;
SELECT COUNT(*) FROM contract_type;
SELECT COUNT(*) FROM dispersed_gen;
SELECT COUNT(*) FROM ev_charge;
SELECT COUNT(*) FROM ev_charge_manage;
SELECT COUNT(*) FROM house_avg;
SELECT COUNT(*) FROM industry_cust_change;
SELECT COUNT(*) FROM industry_type;
SELECT COUNT(*) FROM renew_energy;
SELECT COUNT(*) FROM welfare_discount;
```

확인 목적:

- 테이블별 데이터 규모 확인
- 특정 테이블이 비정상적으로 작거나 누락되었는지 확인
- NL2SQL 평가 시 충분한 데이터 다양성이 있는지 확인

### 2.4 기간 범위 확인

연/월 컬럼이 있는 테이블에 대해 다음 형태의 쿼리를 사용했다.

```sql
SELECT
  MIN(year) AS min_year,
  MAX(year) AS max_year,
  MIN(month) AS min_month,
  MAX(month) AS max_month
FROM business_type;
```

2026년 데이터가 부분 적재인지 확인하기 위해 다음 형태의 쿼리도 사용했다.

```sql
SELECT year, month, COUNT(*) AS row_count
FROM business_type
WHERE year = '2026'
GROUP BY year, month
ORDER BY year, month;
```

발견한 개선점:

- `business_type`, `contract_type`, `industry_type` 등 주요 테이블의 2026년 데이터는 1월까지만 확인되었다.
- 자연어 질의에서 “2026년”을 전체 연도처럼 해석하면 잘못된 결과가 나올 수 있다.

개선 제안:

- 2026년 데이터는 부분 적재 상태로 메타데이터에 명시한다.
- 최신 연도 질의는 “현재 적재된 월 기준”이라는 조건을 붙인다.
- 필요하면 원천 API 재수집으로 2026년 최신 월까지 보강한다.

## 3. 발견한 품질 개선점

## 3.1 현재 DB에는 파일데이터셋 테이블이 없음

발견 방법:

```sql
.schema
```

발견 내용:

- 현재 `kepco.db`에는 13개 API 테이블만 존재한다.
- 파일데이터셋 기반 테이블은 현재 DB에 보이지 않는다.

영향:

- 문서나 프롬프트에서 파일데이터셋 테이블까지 있다고 가정하면 NL2SQL이 존재하지 않는 테이블을 참조할 수 있다.
- 학습/평가용 스키마 설명은 현재 DB 기준으로 다시 맞춰야 한다.

개선 제안:

- 현재 DB를 API 전용 DB로 명시한다.
- 파일데이터셋 테이블을 사용할 계획이면 별도 DB 또는 동일 DB에 적재 후 스키마 문서를 갱신한다.

## 3.2 `house_avg`, `industry_cust_change`의 지역 적재 범위가 불완전함

발견 방법:

```sql
SELECT metro, COUNT(*) AS row_count
FROM house_avg
GROUP BY metro
ORDER BY metro;
```

```sql
SELECT metro, COUNT(*) AS row_count
FROM industry_cust_change
GROUP BY metro
ORDER BY metro;
```

발견 내용:

- 두 테이블 모두 다음 5개 광역시도만 확인되었다.
  - 경기도
  - 부산광역시
  - 서울특별시
  - 세종특별자치시
  - 울산광역시

영향:

- 전국 평균, 전국 지역 비교, 특정 도/광역시 비교 질의에 부적합하다.
- 다른 테이블은 전국 지역이 넓게 들어 있는데 이 두 테이블만 5개 지역이면 사용자 질의 결과가 왜곡될 수 있다.

개선 제안:

- 원천 수집 로직을 확인하고 누락 지역을 재수집한다.
- 재수집 전에는 이 두 테이블을 “부분 지역 데이터”로 표시한다.
- NL2SQL 평가 질문에서 전국 단위 비교 질의에 이 테이블을 사용하지 않는다.

## 3.3 지역명 표기가 혼재되어 있음

발견 방법:

```sql
SELECT DISTINCT metro
FROM business_type
ORDER BY metro;
```

```sql
SELECT DISTINCT metro
FROM billing_type
ORDER BY metro;
```

발견 내용:

- 다음과 같은 행정구역 표기 혼재가 있다.
  - `강원도` / `강원특별자치도`
  - `전라북도` / `전북특별자치도`
  - `전체`
  - `황해북도`

영향:

- “강원 지역 사용량” 같은 질의에서 과거 명칭과 현재 명칭이 분리 집계될 수 있다.
- `전체` 행을 지역 행과 함께 합산하면 중복 집계가 발생할 수 있다.
- `황해북도`는 일반적인 국내 행정구역 분석과 다르게 취급해야 한다.

개선 제안:

- 지역 정규화 매핑 테이블을 추가한다.
- 예: `강원도`와 `강원특별자치도`를 하나의 표준 지역 코드로 매핑한다.
- `전체`는 지역별 집계 대상에서 제외하고 전국 집계 전용 값으로 분리한다.

## 3.4 `전체` 집계 행이 지역별 행과 섞여 있음

발견 방법:

```sql
SELECT COUNT(*) AS row_count
FROM billing_type
WHERE metro = '전체' OR city = '전체' OR metro = '' OR city = '';
```

동일한 형태로 `contract_type`, `industry_type`, `renew_energy`, `welfare_discount`도 확인했다.

발견 내용:

| table | rows_with_total_or_blank_region |
|---|---:|
| billing_type | 308 |
| contract_type | 427 |
| industry_type | 1,284 |
| renew_energy | 1 |
| welfare_discount | 192 |

영향:

- 지역별 합산 질의에서 `전체` 행까지 포함하면 중복 집계된다.
- 자연어 질의가 “전국”인지 “지역별 합”인지 구분하지 못하면 SQL이 잘못 생성될 수 있다.

개선 제안:

- 지역별 분석 view에서는 `metro != '전체' AND city != '전체'` 조건을 기본 적용한다.
- 전국 집계 view는 별도로 만든다.
- 스키마 설명에 `전체` 행의 의미를 명시한다.

## 3.5 수치 컬럼에 음수 값이 있음

발견 방법:

수치형 컬럼에 대해 다음 형태의 쿼리를 사용했다.

```sql
SELECT COUNT(*) AS negative_count
FROM business_type
WHERE power_usage < 0;
```

```sql
SELECT *
FROM business_type
WHERE power_usage < 0 OR contract_power < 0
LIMIT 20;
```

발견 내용:

| table | column | negative_count |
|---|---|---:|
| business_type | power_usage | 20 |
| business_type | contract_power | 5 |
| contract_type | power_usage | 1 |
| contract_type | bill | 9 |
| contract_type | unit_cost | 8 |
| industry_cust_change | expansion_count | 2 |
| industry_type | power_usage | 13 |
| industry_type | bill | 31 |
| industry_type | unit_cost | 25 |

대표 샘플:

| table | sample |
|---|---|
| business_type | 2021-01 부산광역시 기장군 전 철 `power_usage=-31305103` |
| contract_type | 2022-01 세종특별자치시 세종시 일반용 `power_usage=-560490206`, `bill=-33473868153` |
| industry_type | 2022-01 세종특별자치시 세종시 전기, 가스, 증기 및 수도사업 `power_usage=-630531067`, `bill=-42953477359` |
| industry_cust_change | 2021-04 서울특별시 강동구 건설업 `expansion_count=-12` |

영향:

- 사용량, 요금, 단가, 계약전력은 일반적으로 음수를 기대하기 어렵다.
- 음수가 정산/보정값이라면 그대로 둘 수 있지만, 오류라면 집계와 순위 질의가 왜곡된다.

개선 제안:

- 원천 API에서 동일 행을 재확인한다.
- 음수가 정상 보정값이면 `is_adjustment` 또는 `quality_flag`를 추가한다.
- 오류로 판단되면 NULL 처리 또는 제외 view를 만든다.
- NL2SQL 평가에서는 음수 포함 여부를 명확히 설명한다.

## 3.6 NULL과 빈 문자열이 섞여 있음

발견 방법:

컬럼별로 다음 형태의 쿼리를 사용했다.

```sql
SELECT
  SUM(CASE WHEN column_name IS NULL THEN 1 ELSE 0 END) AS null_count,
  SUM(CASE WHEN TRIM(CAST(column_name AS TEXT)) = '' THEN 1 ELSE 0 END) AS blank_count
FROM table_name;
```

발견 내용:

| table | column | issue | count |
|---|---|---|---:|
| billing_type | bill_type | blank | 8 |
| common_code | upper_code | NULL | 78 |
| common_code | upper_code_name | NULL | 78 |
| contract | presumed_price | NULL | 700 |
| contract | contract_req_dept | NULL | 490 |
| contract | presumed_amount | NULL | 24,017 |
| contract | delivery_location | NULL | 183 |
| contract | delivery_due_date | NULL | 158 |
| contract_type | contract_power | blank | 3,204 |
| dispersed_gen | dl_name | blank | 1 |
| ev_charge | car_type | blank | 11 |
| ev_charge_manage | status_updated_at | blank | 8,342 |
| renew_energy | city | blank | 1 |
| welfare_discount | city | blank | 192 |

영향:

- 빈 문자열과 NULL이 혼재하면 조건절 생성이 어려워진다.
- `contract_type.contract_power`는 수치 컬럼처럼 쓰일 가능성이 큰데 빈 문자열이 있어 타입 정합성 문제가 생길 수 있다.
- `ev_charge_manage.status_updated_at`은 전체가 빈 값이므로 사실상 분석 컬럼으로 사용할 수 없다.

개선 제안:

- 빈 문자열을 NULL로 통일한다.
- 수치 컬럼의 빈 문자열은 NULL 변환 후 타입을 정리한다.
- 전체가 빈 컬럼은 스키마 설명에서 “사용 불가 또는 미수집 컬럼”으로 표시한다.

## 3.7 업종명과 카테고리 값의 표기가 흔들림

발견 방법:

```sql
SELECT DISTINCT biz_type
FROM business_type
ORDER BY biz_type;
```

```sql
SELECT DISTINCT biz
FROM industry_type
ORDER BY biz;
```

발견 내용:

- `business_type.biz_type`에는 공백이 많은 값이 있다.
  - 예: `1차   금속`, `관  공  용`
- `industry_type.biz`에는 유사 업종명이 여러 표기로 존재한다.
  - 예: `제조업`, `제조업 `
  - 예: `농업, 임업 및 어업`, `농업/ 임업 및 어업 `

영향:

- 자연어 질의에서 “제조업”을 요청했을 때 trailing space가 있는 값이 누락될 수 있다.
- 업종명 LIKE 검색이 불안정해진다.
- 학습 데이터에서 동일 개념이 여러 값으로 분산될 수 있다.

개선 제안:

- 카테고리 정규화 테이블을 만든다.
- `TRIM()`을 적용한 정제 view를 만든다.
- 표준 업종명과 원본 업종명을 함께 보존한다.

## 3.8 `welfare_discount.city`의 빈 값은 세종시 보정 가능성이 있음

발견 방법:

```sql
SELECT *
FROM welfare_discount
WHERE city = ''
LIMIT 20;
```

발견 내용:

- 빈 `city` 값은 `세종특별자치시` 행에서 주로 나타난다.

영향:

- “세종시 복지할인” 같은 질의에서 `city = '세종시'` 조건만 사용하면 일부 행이 누락될 수 있다.

개선 제안:

- `metro = '세종특별자치시' AND city = ''`인 경우 정제 view에서 `city_norm = '세종시'`로 보정한다.
- 원본 컬럼은 그대로 두고 정규화 컬럼을 추가하는 방식이 안전하다.

## 3.9 자연키 후보 기준 중복은 발견되지 않음

발견 방법:

```sql
SELECT year, month, metro, city, biz_type, COUNT(*) AS duplicate_count
FROM business_type
GROUP BY year, month, metro, city, biz_type
HAVING COUNT(*) > 1;
```

다음 후보 키들도 같은 방식으로 확인했다.

| table | checked_key |
|---|---|
| billing_type | year, month, metro, city, bill_type |
| business_type | year, month, metro, city, biz_type |
| contract_type | year, month, metro, city, contract_type |
| industry_cust_change | year, month, metro, city, biz |
| industry_type | year, month, metro, city, biz |
| house_avg | year, month, metro, city |
| welfare_discount | year, month, metro, city, welfare_type |
| renew_energy | year, metro, city, gen_source |
| common_code | code_type, upper_code, code |
| ev_charge_manage | station_id, charger_id |

확인 결과:

- 위 후보 키 기준으로는 중복 행이 발견되지 않았다.

판단:

- 중복 적재 문제는 현재 우선순위가 낮다.
- 다만 실제 PK가 명시되어 있지 않으므로, 향후 정제 DB에는 unique index 또는 primary key 정의를 검토할 수 있다.

## 4. NL2SQL 관점의 위험도

| risk | description | affected_tables | priority |
|---|---|---|---|
| 부분 지역 적재 | 일부 테이블이 5개 광역시도만 포함 | house_avg, industry_cust_change | 높음 |
| 지역명 혼재 | 행정구역명이 과거/현재 명칭으로 분리 | billing_type, business_type, ev_charge, ev_charge_manage, welfare_discount 등 | 높음 |
| 전체 행 중복 위험 | `전체` 집계 행과 지역 행이 섞임 | billing_type, contract_type, industry_type 등 | 높음 |
| 음수 수치값 | 사용량/요금/단가 등에 음수 존재 | business_type, contract_type, industry_type 등 | 중간 |
| 빈 문자열/NULL 혼재 | 조건절과 집계에서 누락 가능 | 여러 테이블 | 중간 |
| 카테고리 표기 흔들림 | 업종명 공백/표기 차이 | business_type, industry_type | 중간 |
| 2026 부분 적재 | 최신 연도 전체로 오해 가능 | business_type, contract_type, industry_type 등 | 중간 |
| 파일데이터셋 미적재 | 예상 스키마와 실제 DB 차이 | kepco.db 전체 | 중간 |

## 5. 우선 적용할 정제 방향

1. 정규화 view 생성
   - `metro_norm`
   - `city_norm`
   - `biz_norm`
   - `is_total_region`
   - `quality_flag`

2. 지역 집계용 view와 전국 집계용 view 분리
   - 지역별 분석 view에서는 `전체` 행 제외
   - 전국 분석 view에서는 `전체` 행을 우선 사용하거나 지역 행 합산만 사용하도록 규칙화

3. 누락 가능성이 큰 테이블 재수집
   - `house_avg`
   - `industry_cust_change`

4. 음수값 검증
   - 원천 API와 대조
   - 정상 보정값이면 flag 처리
   - 오류면 제외 또는 NULL 처리

5. 빈 문자열 정리
   - 텍스트 컬럼: `NULLIF(TRIM(col), '')`
   - 수치 컬럼: 빈 문자열을 NULL로 변환

6. 스키마 설명 업데이트
   - 현재 DB가 API 테이블 13개만 포함한다는 점 명시
   - 2026년은 부분 적재 상태로 표시
   - `전체` 행의 의미와 사용 규칙 명시

## 6. 추천 후속 산출물

다음 파일을 추가로 만들면 품질 점검을 반복 실행하기 쉬워진다.

| artifact | purpose |
|---|---|
| `analysis/audit_kepco_db.py` | DB 품질 점검 자동화 스크립트 |
| `analysis/output/kepco_db_quality_audit.json` | 테이블별 이슈를 기계가 읽을 수 있는 JSON으로 저장 |
| `analysis/output/kepco_db_quality_audit.html` | 보고서 공유용 HTML |
| `analysis/sql/kepco_clean_views.sql` | 정규화 view 생성 SQL |

