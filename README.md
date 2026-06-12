# KEPCO NL2SQL

한국전력공사 전력데이터를 대상으로 자연어 기반 데이터 탐색(Text2SQL)의 업무 적용 가능성을 검증하는 프로젝트입니다.

자연어 질문을 SQLite SQL로 변환하고, 조회 결과를 반환하는 파이프라인을 구축하여 정확도, 안정성, 보안성을 평가하는 것이 목표입니다.

```text
자연어 질문
  -> 대상 테이블 및 의도 분류
  -> 공통코드와 기간 조건 해석
  -> SQLite SQL 생성
  -> 읽기 전용 실행 및 검증
  -> 결과 반환과 평가
```

## 현재 상태

기준일: 2026-06-11

| 영역 | 상태 | 주요 결과 |
|---|---|---|
| 기술 및 벤치마크 조사 | ✅ 완료 | BIRD 유사 DB 선정, 587개 질의 분류, Spider2-Lite 후보 추출 |
| 공개 데이터 수집 | ✅ 완료 | KEPCO OPEN API 2022~2026년 데이터 및 파일데이터 수집 |
| SQLite 테스트 DB | ✅ 완료 | API 기반 13개 테이블, 1,167,549행 |
| 데이터 품질 점검·이슈 처리 | ✅ 완료 | 중복 제거, 집계행 삭제, 지역·업종 코드 통일, KSIC 11차 반영 |
| 스키마 문서 동기화 | 🔄 진행 중 | CLAUDE.md 반영 완료 |
| 업무 테스트셋 | ⛔ 보류 | 원천 데이터 접근 및 비식별 자연어-정답 SQL 쌍 확보 필요 |
| NL2SQL 파이프라인 | ⏳ 대기 | 스키마 동기화 완료 후 착수 |
| 최종 평가 보고서 | ⏳ 대기 | 정확도 측정 이후 작성 |

상세 일정과 작업 우선순위는 [`ROADMAP.md`](ROADMAP.md)를 참고합니다.

## 데이터 현황

현재 `data/kepco.db`는 API 전용 SQLite DB입니다. 파일데이터셋은 수집되어 있지만 현재 DB에는 적재되어 있지 않습니다.

| 테이블 | 행 수 | 설명 |
|---|---:|---|
| `business_type` | 471,668 | 업종별 전력사용 현황 |
| `industry_type` | 272,854 | 산업분류별 전력사용 |
| `welfare_discount` | 104,536 | 복지할인 현황 |
| `contract_type` | 97,476 | 계약종별 전력사용 현황 |
| `industry_cust_change` | 76,759 | 산업분류별 고객 증감 |
| `billing_type` | 58,734 | 요금청구방식 변동추이 |
| `contract` | 58,577 | 전자입찰계약 정보 |
| `ev_charge_manage` | 8,342 | 전기차충전소 운영정보 |
| `dispersed_gen` | 7,526 | 분산전원연계 정보 |
| `house_avg` | 5,492 | 가구평균 전력사용량 |
| `ev_charge` | 4,623 | 전기차 충전소 설치현황 |
| `common_code` | 643 | 공통코드 |
| `renew_energy` | 224 | 신재생 에너지 현황 |
| **합계** | **1,167,549** | |

`PRAGMA integrity_check` 결과는 `ok`입니다. 평가 전에 다음 제약을 반영해야 합니다.

- `house_avg`, `industry_cust_change`는 서울·부산·울산·경기·세종 5개 광역시도만 적재 — API 자체 제한
- 주요 사용량 테이블의 2026년 데이터는 1월까지만 확인됩니다.

지역·업종 컬럼은 코드 기반으로 정비되었습니다. `common_code` 테이블에서 코드명을 JOIN하여 사용합니다.

전체 품질 점검 결과는 [`analysis/db_quality_report.md`](analysis/db_quality_report.md)에 있습니다.

## 테스트 및 벤치마크

### KEPCO 시나리오

- [`tests/nl2sql_scenarios.md`](tests/nl2sql_scenarios.md): 기본 평가 시나리오 35개
- [`tests/nl2sql_business_type_quick.md`](tests/nl2sql_business_type_quick.md): 업종별 빠른 테스트
- [`tests/nl2sql_industry_type_quick.md`](tests/nl2sql_industry_type_quick.md): 산업분류별 빠른 테스트
- [`tests/nl2sql_industry_type_quick_answers.md`](tests/nl2sql_industry_type_quick_answers.md): 산업분류별 정답 예시
- [`tests/nl2sql_industry_type_unit_test.md`](tests/nl2sql_industry_type_unit_test.md): 단위 테스트 설계

### 공개 벤치마크 조사

- BIRD에서 KEPCO와 유사한 DB 10개를 선정했습니다.
- `bike_share_1` 113개와 `works_cycles` 474개, 총 587개 질의를 SQL 패턴 기준으로 분류했습니다.
- Spider2-Lite에서 SQLite/local 질문 135개를 추출하고 한국어 번역본을 생성했습니다.
- Spider2-Lite 135개 중 공개 정답 SQL이 있는 질문은 24개입니다.

주요 산출물:

- [`analysis/output/bird_kepco_similarity_top10.md`](analysis/output/bird_kepco_similarity_top10.md)
- [`analysis/output/bird_selected_query_classification.md`](analysis/output/bird_selected_query_classification.md)
- [`analysis/output/spider2_lite_sqlite_questions.md`](analysis/output/spider2_lite_sqlite_questions.md)

동일한 결과의 HTML과 JSON 버전도 `analysis/output/`에 있습니다.

## 프로젝트 구조

```text
kepco-nl2sql/
|-- README.md
|-- ROADMAP.md
|-- JOB.md
|-- CLAUDE.md
|-- requirements.txt
|-- collect_data.py
|-- collect_retry.py
|-- collect_new_years.py
|-- collect_filedatasets.py
|-- collect_bird_metadata.py
|-- build_db.py
|-- build_filedata_db.py
|-- aggregate_data.py
|-- analysis/
|   |-- output/
|   `-- *.py
|-- insights/
|-- tests/
`-- data/                 # 원천 데이터와 SQLite DB, Git 제외
```

## 환경 설정

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
echo "KEPCO_API_KEY=발급받은키" > .env
```

데이터 수집 및 DB 재구축:

```bash
python collect_data.py
python collect_retry.py
python collect_new_years.py
python build_db.py
```

데이터 디렉터리와 `.env`는 Git에서 제외됩니다. 수집 API의 호출 제한과 데이터 기간을 확인한 뒤 실행해야 합니다.

## 다음 구현 범위

1. 스키마 문서 동기화 마무리 (README 기준선 확정)
2. 실제 업무 자료추출 요청에서 자연어-정답 SQL 쌍을 확보하고 비식별화합니다.
3. 스키마 선택, 코드 매핑, SQL 생성, 읽기 전용 실행기로 구성된 최소 파이프라인을 구현합니다.
4. 35개 기본 시나리오와 업무 테스트셋으로 실행 정확도와 오류 유형을 측정합니다.
5. 정확도, 안정성, 보안성 기준으로 업무 도입 가능성을 평가합니다.

## 참고

- 과제 수행계획: [`JOB.md`](JOB.md)
- 한전 전력데이터 개방 포털: https://bigdata.kepco.co.kr
- API Base URL: `https://bigdata.kepco.co.kr/openapi/v1/`
