# KEPCO NL2SQL 프로젝트 가이드

이 프로젝트는 한국전력공사(KEPCO) 전력데이터 개방 포털의 OPEN API를 활용하여 자연어 질문을 API 파라미터로 변환하고, 수집된 전력 데이터를 분석하는 파이프라인입니다.

## 프로젝트 개요

- **목적**: 자연어 질문을 SQL 또는 API 쿼리로 변환하여 전력 데이터 조회를 자동화합니다.
- **주요 기술**: Python, SQLite, KEPCO OPEN API, Pandas, Matplotlib.
- **데이터 소스**: [한전 전력데이터 개방 포털](https://bigdata.kepco.co.kr).
- **아키텍처**:
    1.  **데이터 수집**: 한전 API를 통해 JSON 데이터 수집 (2022년~2026년).
    2.  **DB 구축**: 수집된 JSON 파일을 구조화된 SQLite 데이터베이스(`data/kepco.db`)로 변환.
    3.  **분석 및 시각화**: 수집된 데이터를 바탕으로 인사이트 도출 및 시각화 자료 생성.
    4.  **NL2SQL (계획)**: LLM(예: Claude)을 활용해 자연어를 SQL 쿼리 또는 API 파라미터로 변환.

## 빌드 및 실행 방법

### 1. 환경 설정
필요한 Python 패키지를 설치하고 한전 API 키를 설정합니다.
```bash
pip install -r requirements.txt
echo "KEPCO_API_KEY=발급받은_API_키" > .env
```

### 2. 데이터 수집
- **2022-2023년 데이터**: `python collect_data.py`
- **2024-2026년 데이터**: `python collect_new_years.py`
- **특정 API 재수집**: `python collect_retry.py`

### 3. 데이터베이스 구축
`data/raw/` 폴더의 JSON 파일들을 읽어 SQLite DB를 생성합니다.
```bash
python build_db.py
```

### 4. 분석 및 시각화
`analysis/` 폴더의 분석 스크립트를 실행하여 `analysis/output/`에 결과물을 생성합니다.
```bash
python analysis/plot_industry_power.py
python analysis/plot_city_industry.py
```

### 5. NL2SQL 테스트 시나리오
`tests/nl2sql_scenarios.md` 파일을 참고하여 자연어 질문 예시와 대응되는 SQL을 확인할 수 있습니다.

## 데이터베이스 스키마

SQLite DB(`data/kepco.db`)는 다음과 같은 주요 테이블을 포함합니다:
- `contract_type`: 계약종별 전력사용량 (주택용, 산업용 등).
- `industry_type`: 산업분류별 전력사용량.
- `business_type`: 업종별 전력사용량.
- `ev_charge` / `ev_charge_manage`: 전기차 충전소 및 충전기 정보.
- `renew_energy`: 신재생 에너지(태양광, 풍력 등) 현황.
- `welfare_discount`: 복지할인 대상자 통계.
- `common_code`: 지역 코드(`metroCd`, `cityCd`), 계약 코드 등 메타데이터.

## 개발 컨벤션

- **데이터 저장**: 원본 JSON 데이터는 `data/raw/{api_name}/`에 저장되며, 최종 DB는 `data/kepco.db`입니다. (두 경로 모두 Git에서 제외됩니다).
- **환경 변수**: 한전 API 키(`KEPCO_API_KEY`)는 `.env` 파일에서 관리합니다.
- **분석 스크립트**: 새로운 분석 작업은 `analysis/` 폴더 내에 작성하며, 결과물은 `analysis/output/`에 저장합니다.
- **테스트**: 예상되는 NL2SQL 동작은 `tests/nl2sql_scenarios.md`를 기준으로 합니다.
- **코드 스타일**: 표준 Python(PEP 8) 컨벤션을 따르며, 파일 경로는 `pathlib`, DB 작업은 `sqlite3`를 사용합니다.
