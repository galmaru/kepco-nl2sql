# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 목적

자연어 질문을 한전 OPEN API 파라미터 또는 SQLite SQL로 자동 변환하는 NL2SQL 파이프라인.
현재는 데이터 수집·DB 구축·인사이트 도출 단계가 완료되었고, LLM 연동 NL2SQL 구현이 다음 단계.

---

## 자주 쓰는 명령

```bash
# 의존성 설치
pip install -r requirements.txt

# API 키 설정 (.env 파일)
echo "KEPCO_API_KEY=발급받은키" > .env

# 데이터 수집 (2022~2023)
python collect_data.py

# 데이터 수집 (2024~2026)
python collect_new_years.py

# 수집 실패 API 재수집
python collect_retry.py

# SQLite DB 구축 (data/raw/ → data/kepco.db, 13개 테이블)
python build_db.py

# 파일데이터셋 기반 12개 테이블 추가 적재 (data/filedatasets/files/ → data/kepco.db)
python build_filedata_db.py

# 집계 테이블(agg_*) 생성
python aggregate_data.py

# 파일데이터셋 메타 수집 (data.go.kr 크롤링)
python collect_filedatasets.py meta

# 파일데이터셋 실파일 다운로드
python collect_filedatasets.py download
python collect_filedatasets.py download 0 50   # 인덱스 범위 지정

# 분석 스크립트 실행 (결과: analysis/output/)
python analysis/plot_industry_power.py
python analysis/plot_city_industry.py

# DB 직접 조회
python3 -c "import sqlite3; conn=sqlite3.connect('data/kepco.db'); ..."
```

---

## 아키텍처 개요

### 데이터 파이프라인

```
한전 OPEN API (bigdata.kepco.co.kr)
    └─ collect_data.py / collect_new_years.py / collect_retry.py
         └─ data/raw/{api_name}/{year_month}.json   ← git 제외
              └─ build_db.py
                   └─ data/kepco.db (SQLite)        ← git 제외
                        └─ aggregate_data.py → agg_* 테이블

data.go.kr (한전 파일데이터 305건)
    └─ collect_filedatasets.py
         └─ data/filedatasets/files/{pk}_*.csv     ← git 제외
              └─ build_filedata_db.py
                   └─ data/kepco.db (같은 DB에 추가)
```

### DB 테이블 구성 (data/kepco.db)

**API 기반 13개 테이블** (build_db.py, 2022~2026년):

| 테이블 | 설명 | 키 컬럼 |
|--------|------|--------|
| `contract_type` | 계약종별 전력사용 | year, month, metro, city, contract_type |
| `industry_type` | 산업분류별 전력사용 | year, month, metro, city, biz |
| `business_type` | 업종별 전력사용 | year, month, metro, city, biz_type |
| `billing_type` | 청구서 유형별 발송 | year, month, metro, city, bill_type |
| `welfare_discount` | 복지할인 현황 | year, month, metro, city, welfare_type |
| `industry_cust_change` | 산업별 고객 증감 | year, month, metro, city, biz |
| `house_avg` | 가구당 평균 전력사용 | year, month, metro, city |
| `contract` | 전자입찰계약 공고 | notice_date, company_id, name |
| `ev_charge` | EV 충전소 설치현황 | metro, city, station_place |
| `ev_charge_manage` | EV 충전기 운영현황 | cs_id, cp_id |
| `dispersed_gen` | 분산전원연계 | subst_cd, mtr_no |
| `renew_energy` | 신재생에너지 현황 | year, metro, city, gen_source |
| `common_code` | 지역·계약·업종 코드 | code_type, code |

**파일데이터 기반 12개 테이블** (build_filedata_db.py):

| 테이블 | 설명 | 특이사항 |
|--------|------|---------|
| `sales_stat` | 영업통계 (2024~) | period='YYYY-MM' |
| `hourly_power` | 시간별 전력수요 (2024~) | date, hour, region |
| `dong_industry_power` | 법정동별 산업 전력사용 | 2025년 데이터만 존재, biz_name_large는 파이프 없는 단어 (e.g. `제조업`) |
| `industry_utilization` | 산업별 월별 수용률 | util_jan~util_dec 컬럼 |
| `industry_code` | 산업 분류 코드 | |
| `ev_station_location` | EV 충전소 위경도 | station_id로 ev_charger_capacity JOIN |
| `ev_charger_capacity` | 충전기별 용량(kW) | station_id로 ev_station_location JOIN |
| `ppa_by_source` | 발전원별 PPA 계약 | 2006~현재 |
| `ppa_by_region` | 지역별 PPA 계약 | region은 광역시도 줄임말 (e.g. '전북') |
| `net_metering_usage` | 상계거래 전력사용량 | metro, city, dong, year, month |
| `net_metering_surplus` | 상계거래 잉여전력량 | net_metering_usage와 같은 키로 JOIN |
| `tariff_adjustment` | 전기요금 조정율 | 1982년~현재 이력, 값은 % 변동폭 |

**집계 테이블 6개** (aggregate_data.py, `agg_` 접두사):
전국 합계 사전 집계본. `agg_contract_type_monthly`, `agg_business_type_monthly`, `agg_industry_type_monthly`, `agg_billing_type_monthly`, `agg_renew_energy_yearly`, `agg_welfare_discount_monthly`.

### 공통 코드 매핑

`common_code` 테이블의 `code_type`별 주요 값:
- `metroCd`: 11=서울, 26=부산, 27=대구, 28=인천, 29=광주, 30=대전, 31=울산, 36=세종, 41=경기, 42=강원, 43=충북, 44=충남, 45=전북, 46=전남, 47=경북, 48=경남, 50=제주
- `cntrCd`: 100=주택용, 200=일반용, 300=교육용, 400=산업용, 500=농사용, 600=가로등
- `bizCd`: A=농림어업, B=광업, C=제조업, D=전기가스수도, E=하수·환경, F=건설업, G=도소매, H=운수, I=숙박음식 등

### SQL 주의사항

- SQLite는 `LEFT()` 함수 미지원 → `SUBSTR(column, 1, N)` 사용
- `period` 컬럼(sales_stat)은 'YYYY-MM' 형식 → `SUBSTR(period, 1, 4)`로 연도 추출
- `dong_industry_power.biz_name_large`는 파이프(`|`)가 없는 단어 (e.g. `제조업`, `건설업`)로 등호 조건 사용
- `ppa_by_region.region`은 줄임말 (`전북`, `전남`) vs `net_metering_usage.metro`는 전체명 (`전북특별자치도`) → LIKE로 매핑

---

## NL2SQL 테스트 시나리오

`tests/nl2sql_scenarios.md`: 35개 시나리오 (S, C, M, MC, N 시리즈)
- S: 단일 테이블 / 단순 (10개)
- C: 단일 테이블 / 복합 집계·비율·순위 (4개)
- M: 멀티 테이블 / 단순 JOIN (3개)
- MC: 멀티 테이블 / 복합 CTE (4개)
- N: 신규 12개 테이블 대상 (14개)

---

## 파일 구조 요약

```
kepco-nl2sql/
├── collect_data.py         # 한전 API 수집 (2022~2023)
├── collect_new_years.py    # 한전 API 수집 (2024~2026)
├── collect_retry.py        # metroCd 필수 API 재수집
├── collect_filedatasets.py # data.go.kr 파일데이터 크롤링·다운로드
├── build_db.py             # API JSON → SQLite (13개 테이블)
├── build_filedata_db.py    # 파일데이터 CSV → SQLite (12개 테이블)
├── aggregate_data.py       # 집계 테이블(agg_*) 생성
├── remove_id_column.py     # 테이블 id 컬럼 제거 유틸
├── analysis/               # 시각화 스크립트 (결과: analysis/output/)
├── insights/               # 분석 리포트 마크다운
├── tests/nl2sql_scenarios.md  # NL2SQL 테스트 35개
└── data/                   # git 제외
    ├── kepco.db            # SQLite DB (25개 테이블 + 6개 agg)
    ├── raw/                # 한전 API JSON (2022~2026)
    └── filedatasets/       # data.go.kr 파일데이터
        ├── datasets_meta.json
        └── files/          # 다운로드된 CSV 파일 (~277개)
```
