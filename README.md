# KEPCO NL2SQL

한국전력공사(KEPCO) 전력데이터 개방 포털 OPEN API를 활용한 **자연어 → API 쿼리 자동 생성** 시스템

---

## 프로젝트 개요

자연어 질문을 입력하면 한전 OPEN API 파라미터를 자동으로 구성하여 전력 데이터를 조회하는 NL2SQL 파이프라인을 구축합니다.

**예시**
```
입력: "2023년 서울 주택용 전력 사용량 알려줘"
출력: GET /powerUsage/contractType.do?year=2023&metroCd=11&cntrCd=100
```

---

## 진행 현황

### ✅ 완료

#### 1단계: API 명세 수집
- 한전 OPEN API 14개 전체 명세 파악
- 엔드포인트 URL, 요청 파라미터, 응답 필드 정리

| API | 엔드포인트 |
|-----|-----------|
| 계약종별 전력사용량 | `/powerUsage/contractType.do` |
| 산업분류별 전력사용량 | `/powerUsage/industryType.do` |
| 업종별 전력사용량 | `/powerUsage/businessType.do` |
| 가구평균 전력사용량 | `/powerUsage/houseAve.do` |
| 산업분류별 고객 증감 | `/change/custNum/industryType.do` |
| 요금청구방식 변동추이 | `/billingType.do` |
| 전기차 충전소 설치현황 | `/EVcharge.do` |
| 신재생 에너지 현황 | `/renewEnergy.do` |
| 복지할인대상 | `/welfareDiscount.do` |
| 전자입찰계약 정보 | `/electContract.do` |
| 전기차충전소 운영정보 | `/EVchargeManage.do` |
| 분산전원연계 정보 | `/dispersedGeneration.do` |
| 공통코드 정보 | `/commonCode.do` |
| 상태코드 정보 | (HTTP 상태코드) |

#### 2단계: 실데이터 수집 (2022~2023년)
- `collect_data.py` — 12개 API 수집 (월별/연도별)
- `collect_retry.py` — metroCd 필수 API 4개 재수집
- **200개 JSON 파일, 약 240MB** 수집 완료

#### 3단계: SQLite DB 구축
- `build_db.py` 실행으로 `data/kepco.db` 생성 (38MB)
- 13개 테이블, **총 464,685 rows**

| 테이블 | rows | 설명 |
|--------|------|------|
| contract_type | 38,472 | 계약종별 전력사용 현황 |
| industry_type | 107,860 | 산업분류별 전력사용 |
| business_type | 187,156 | 업종별 전력사용 현황 |
| billing_type | 23,397 | 청구서 유형별 발송 현황 |
| welfare_discount | 33,344 | 복지할인 현황 |
| industry_cust_change | 29,040 | 산업별 고객 변동 |
| house_avg | 2,138 | 가구당 평균 전력사용 |
| contract | 20,870 | 입찰/계약 공고 |
| ev_charge | 4,623 | EV 충전소 현황 |
| ev_charge_manage | 8,342 | EV 충전기 운영 현황 |
| dispersed_gen | 8,337 | 분산전원연계 정보 |
| renew_energy | 463 | 신재생에너지 현황 |
| common_code | 643 | 공통 코드 |

---

### 🔜 앞으로 할 일

#### 4단계: NL2SQL 파이프라인 설계
- [ ] 자연어 의도 분류기 설계 (어떤 API를 호출할지 결정)
- [ ] API별 파라미터 추출 로직 구현
- [ ] 공통코드 매핑 (예: "서울" → metroCd=11, "주택용" → cntrCd=100)

#### 5단계: LLM 연동
- [ ] Claude API 연동
- [ ] API 명세 기반 프롬프트 엔지니어링
- [ ] 자연어 → API 파라미터 변환 테스트

#### 6단계: 실행 및 결과 반환
- [ ] API 호출 실행기 구현
- [ ] 응답 데이터 자연어 요약

#### 7단계: 평가 및 개선
- [ ] 테스트 질문 셋 구성
- [ ] 정확도 측정
- [ ] 오류 케이스 개선

---

## 프로젝트 구조

```
kepco-nl2sql/
├── README.md
├── requirements.txt
├── .env                  # API 키 (git 제외)
├── .gitignore
├── collect_data.py       # 1차 데이터 수집
├── collect_retry.py      # 재수집 (metroCd 필수 API)
├── build_db.py           # SQLite DB 구축
└── data/
    ├── kepco.db          # SQLite DB (git 제외)
    └── raw/              # 수집된 JSON 파일 (git 제외)
```

---

## 환경 설정

```bash
pip install -r requirements.txt
echo "KEPCO_API_KEY=발급받은키" > .env
```

**API 키 발급**: https://bigdata.kepco.co.kr → OPEN API → 인증키 신청

---

## 참고

- 한전 전력데이터 개방 포털: https://bigdata.kepco.co.kr
- API Base URL: `https://bigdata.kepco.co.kr/openapi/v1/`
