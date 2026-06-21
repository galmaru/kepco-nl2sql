# kepco.db 데이터 품질 점검 보고서

최초 점검일: 2026-06-04  
재검토일: 2026-06-11  
DB: `data/kepco.db` (SQLite, 13개 테이블)

---

## 점검 절차

### 사용 도구
- Python `sqlite3` 모듈 + 직접 SQL 실행
- 외부 프레임워크 없음

### 점검 순서

| 단계 | 항목 | 방법 |
|------|------|------|
| 1 | 테이블 목록 | `sqlite_master` 조회 후 CLAUDE.md 명세와 비교 |
| 2 | 컬럼 구조·행수 | `PRAGMA table_info()` + `COUNT(*)` |
| 3 | 시계열 범위 | `MIN(year)`, `MAX(year)`, `COUNT(DISTINCT month)` |
| 4 | 연도×월 조합 누락 | Python set 차집합 (expected - actual) |
| 5 | NULL 점검 | 주요 컬럼별 `WHERE column IS NULL` |
| 6 | 수치 음수 | `WHERE numeric_col < 0` |
| 7 | 지역 코드 일관성 | `common_code` 테이블 LEFT JOIN 후 미매핑 추출 |
| 8 | 도메인 특화 점검 | 좌표 범위, PK 중복, 날짜 포맷, 업종명 표기 |
| 9 | 집계 행 혼재 | `WHERE metro = '전체'` 존재 여부 및 합계 검증 |

---

## 현재 DB 상태 (2026-06-11 기준)

| 테이블 | 행수 | 비고 |
|--------|-----:|------|
| `billing_type` | 59,042 | |
| `business_type` | 471,668 | |
| `common_code` | 643 | |
| `contract` | 58,577 | |
| `contract_type` | 97,903 | |
| `dispersed_gen` | 7,526 | 중복 813건 제거 후 |
| `ev_charge` | 4,623 | |
| `ev_charge_manage` | 8,342 | |
| `house_avg` | 5,492 | 5개 광역시도만 |
| `industry_cust_change` | 76,759 | 5개 광역시도만 |
| `industry_type` | 272,854 | |
| `renew_energy` | 224 | 2024년만 |
| `welfare_discount` | 104,536 | |
| **총합** | **1,167,549** | |

DB 무결성: `ok`  
2021~2025 연도×월 조합 누락: 주요 4개 테이블 모두 누락 없음  
2026년 적재 현황: `billing_type` 2월까지, 나머지 주요 테이블 1월까지

---

## 처리 완료 이슈

### ✅ `dispersed_gen` 완전 동일 중복 813건 제거

- `build_db.py`의 `load_dispersed_gen()`에서 INSERT 전 `dict.fromkeys()` dedup 적용
- DB: 8,339 → 7,526행

### ✅ 집계 행(`metro='전체'`) 혼재 제거

| 테이블 | 삭제 행수 |
|--------|--------:|
| `contract_type` | 427 |
| `industry_type` | 1,284 |
| `billing_type` | 308 |
| **합계** | **2,019** |

- `build_db.py`에서 `totData` 키 루프 제거 (재수집 시 재적재 방지)

### ✅ 지역명 표기 혼재 → 코드 통일

10개 테이블의 `metro` TEXT, `city` TEXT 컬럼을 `metro_code`, `city_code` 코드 컬럼으로 교체.

| 테이블 | 제거 | 추가 |
|--------|------|------|
| `contract_type` | metro, city | metro_code, city_code |
| `industry_type` | metro, city | metro_code, city_code |
| `business_type` | metro, city | metro_code, city_code |
| `billing_type` | metro, city | metro_code, city_code |
| `welfare_discount` | metro, city | metro_code, city_code |
| `industry_cust_change` | metro, city | metro_code, city_code |
| `house_avg` | metro, city | metro_code, city_code |
| `ev_charge` | metro, city | metro_code, city_code |
| `ev_charge_manage` | metro, city | metro_code, city_code |
| `renew_energy` | metro, city | metro_code, city_code |

- 강원도/강원특별자치도 → metro_code='42' 통일
- 전라북도/전북특별자치도 → metro_code='45' 통일
- 황해북도 metro_code='99', 개성시 city_code='100' 신규 추가

### ✅ 업종명 표기 비일관 → 코드 통일

**`industry_type`**: `biz` TEXT(50개 표기) → `biz_code` TEXT (bizCd A~KEPCO01)

- 50개 표기를 22개 bizCd 코드로 매핑 (NULL 0건)
- 공백 차이, 명칭 변경(구명/신명), 대분류 재편 항목 통합

**`business_type`**: `biz_type` TEXT(38개) → `biz_type_code` TEXT (bizTypeCd 01~38)

- 한전 자체 업종 분류 (공식 숫자 코드 없음, 내부 임의 부여)

**`industry_cust_change`**: `biz` TEXT(22개) → `biz_code` TEXT (bizCd + KEPCO01)

### ✅ `common_code` 정비

| 항목 | 내용 |
|------|------|
| metroCd 추가 | 99=황해북도 |
| cityCd 추가 | 100=개성시 (황해북도 소속) |
| metroCd 현행화 | 32: 강원도→강원특별자치도, 35: 전라북도→전북특별자치도 |
| bizCd trailing space | 21개 코드명 공백 제거 |
| bizCd 코드명 현행화 | KSIC 11차 기준 8개 업데이트 (D, E, H, J, L, N, S, T) |
| bizCd 신규 | KEPCO01=주택용 (KSIC 외 한전 자체 분류) |
| bizTypeCd 신규 | 01~38 한전 자체 업종분류 38개 등록 |

#### bizCd 최종 목록 (KSIC 11차 기준)

| 코드 | 명칭 |
|------|------|
| A | 농업, 임업 및 어업 |
| B | 광업 |
| C | 제조업 |
| D | 전기, 가스, 증기 및 공기 조절 공급업 |
| E | 수도, 하수 및 폐기물 처리, 원료 재생업 |
| F | 건설업 |
| G | 도매 및 소매업 |
| H | 운수 및 창고업 |
| I | 숙박 및 음식점업 |
| J | 정보통신업 |
| K | 금융 및 보험업 |
| L | 부동산업 |
| M | 전문, 과학 및 기술 서비스업 |
| N | 사업시설 관리, 사업 지원 및 임대 서비스업 |
| O | 공공행정, 국방 및 사회보장 행정 |
| P | 교육 서비스업 |
| Q | 보건업 및 사회복지 서비스업 |
| R | 예술, 스포츠 및 여가관련 서비스업 |
| S | 협회 및 단체, 수리 및 기타 개인 서비스업 |
| T | 가구 내 고용활동 및 달리 분류되지 않은 자가 소비 생산활동 |
| U | 국제 및 외국기관 |
| KEPCO01 | 주택용 (KSIC 외 한전 자체 분류) |

---

## 미처리 이슈

### 🟠 주의 (잔여)

#### 1. 수치 음수

| 테이블 | 컬럼 | 음수 건수 |
|--------|------|-------:|
| `contract_type` | `power_usage` | 1 |
| `contract_type` | `bill` | 9 |
| `contract_type` | `unit_cost` | 8 |
| `industry_type` | `power_usage` | 13 |
| `industry_type` | `bill` | 31 |
| `industry_type` | `unit_cost` | 25 |
| `business_type` | `power_usage` | 20 |
| `business_type` | `contract_power` | 5 |
| `industry_cust_change` | `expansion_count` | 2 |

- **판정**: `data/raw/` 원본 JSON 대조 완료. 모두 **API 자체가 음수로 반환한 값**이며 DB 변환 오류 아님
  - `power_usage`, `bill` 음수: 정산 차감 또는 역전력 (D·E 업종, 일반용 계약종 중심)
  - `unit_cost` 음수: `power_usage`/`bill`이 음수일 때 파생 계산 결과
  - `contract_power` 음수 5건: 원본도 음수. 물리적 의미 불명확하나 API 반환값이므로 수정 불가
  - `expansion_count` 음수 2건: 원본도 음수. 용량 축소·해약 처리로 발생 가능
- **조치**: 원본 유지. 집계 쿼리 작성 시 음수 포함 여부를 명시적으로 처리

#### 2. `house_avg` · `industry_cust_change` 부분 지역 적재

- 두 테이블 모두 5개 광역시도만 존재: **경기도, 부산광역시, 서울특별시, 세종특별자치시, 울산광역시**
- **원인**: API 자체 제한 — `metroCd` 파라미터의 코드 체계가 `common_code`와 달라 다른 지역 조회 불가. 재수집으로 해결 불가
- **조치**: 전국 비교 질의에 사용 금지. 5개 지역 한정 분석에만 활용

### 🟡 참고 (잔여)

#### 3. NULL · 빈 문자열 혼재

| 테이블 | 컬럼 | 이슈 | 건수 | 처리 |
|--------|------|------|-----:|------|
| `billing_type` | `bill_type` | 빈문자열 | 4 | 미분류 항목 추정, 원본 유지 |
| `common_code` | `upper_code` | NULL | 78 | 최상위 코드 특성상 정상 |
| `contract` | `presumed_price` | NULL | 700 | 입찰 유형별 미제공 항목 |
| `contract` | `presumed_amount` | NULL | 24,017 | 낙찰 전 공고 단계 정상값 |
| `contract` | `delivery_location` | NULL+빈문자열 | 189 | 원본 유지 |
| `contract_type` | `contract_power` | 빈문자열→NULL | 3,190 | ✅ NULL 변환 완료 (`build_db.py` 미반영) |
| `ev_charge_manage` | `status_updated_at` | 빈문자열(전체) | 8,342 | ⛔ 사용 불가 컬럼 — API 미제공 |
| `welfare_discount` | `city_code` | 빈값 | 0 | ✅ city_code 전환으로 해소 |

- `contract_type.contract_power` 빈문자열 3,190건 → NULL 변환 완료 (2026-06-11). `build_db.py`에는 미반영이므로 DB 재구축 시 재발생
- `ev_charge_manage.status_updated_at` API가 값을 제공하지 않는 컬럼. NL2SQL 쿼리 생성 대상에서 제외
- `contract.presumed_amount` NULL 24,017건은 낙찰 전 입찰공고 단계의 정상 상태
- `billing_type.bill_type` 빈문자열 4건은 비분류 소량 항목으로 원본 유지

#### 4. 황해북도 데이터 99건

| 테이블 | 행수 |
|--------|-----:|
| `industry_type` | 30 |
| `billing_type` | 8 |
| `business_type` | 61 |
| **합계** | **99** |

- 한전 관할 외 지역. metro_code='99', city_code='100' 코드 부여 완료
- 일반 국내 집계 시 `WHERE metro_code != '99'` 조건 권장

#### 5. 2026년 부분 적재

- 주요 사용량 테이블: 2026-01월 데이터만 존재
- `billing_type`만 2026-02월까지 적재
- `renew_energy`: 2024년 데이터만 존재
- **조치**: "2026년 전체" 해석 방지를 위해 메타데이터에 최신 적재 기준월 명시

---

## 우선순위 요약 (잔여)

| 순위 | 이슈 | 영향 테이블 | 조치 방향 |
|------|------|------------|----------|
| 1 | 부분 지역 적재 | `house_avg`, `industry_cust_change` | API 재수집 불가, 5개 지역 한정 활용 |
| 2 | 음수 수치 | `contract_type`, `industry_type`, `business_type` | API 원본값 확인 완료, 원본 유지 |
| 3 | NULL·빈 문자열 혼재 | `contract_type`, `ev_charge_manage` 외 | `contract_power` NULL 변환 완료, `status_updated_at` 사용 불가 명시 |
| 4 | 황해북도 데이터 99건 | 3개 테이블 | 집계 시 `WHERE metro_code != '99'` |
| 5 | 2026 부분 적재 | 주요 사용량 테이블 | 적재 기준월 메타 표시 |
