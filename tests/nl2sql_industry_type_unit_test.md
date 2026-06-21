# NL2SQL 단위 테스트 — industry_type

**대상 테이블**: `industry_type` (KSIC 대분류별 전력사용량·요금)  
**구성**: 단일 테이블 8개(I-01~I-08) + 멀티 JOIN 4개(IJ-01~IJ-04) = 총 12개  
**데이터 범위**: 2021-01 ~ 2026-02 (274,138행)

---

## 주요 함정 요소

| 항목 | 내용 |
|------|------|
| 산업명 변형 | 동일 산업도 표기 상이 → **LIKE 필수** (예: `농업, 임업 및 어업` / `농업/ 임업 및 어업 `) |
| 지역명 | 강원특별자치도로 통일, 타 테이블 JOIN 시 `강원도` 매핑 주의 |
| 없는 컬럼 | `contract_power` 없음 → 계약전력 질문은 거절해야 함 |
| 2026년 | 1~2월 데이터만 존재 |
| bill_type 실제값 | `우편`, `이메일`, `모바일`, `카카오`, `인편`, `''` — `종이`/`e-bill` 환각 주의 |

---

## 단일 테이블 시나리오 (I)

| ID | 난이도 | 평가 축 | 자연어 질문 | 핵심 SQL 패턴 |
|----|--------|---------|------------|--------------|
| I-01 | ⭐ | BASIC + SCHEMA | 2024년 3월 제조업 전력사용량 전국 총합은? | `SUM` + `biz LIKE '%제조업%'` |
| I-02 | ⭐ | BASIC + RANK | 2024년 전력을 가장 많이 쓴 산업 TOP 5 | `GROUP BY biz ORDER BY SUM DESC LIMIT 5` |
| I-03 | ⭐⭐ | RANK | 2024년 건설업 사용량 많은 광역시도 TOP 3 | `GROUP BY metro` + LIKE + LIMIT |
| I-04 | ⭐⭐ | TIME | 2023→2024 제조업 전력사용량 증감률(YoY) | `CASE WHEN year=...` 피벗 + 증감률 계산 |
| I-05 | ⭐⭐ | TIME | 2024년 숙박 및 음식점업 계절별 사용량 비교 | `CASE WHEN month IN (...)` 계절 그룹화 |
| I-06 | ⭐⭐ | BASIC + RANK | 2024년 산업별 평균 판매단가 비싼 TOP 5 | `SUM(bill)/NULLIF(SUM(power_usage),0)` |
| I-07 | ⭐⭐⭐ | RANK | 2024년 서울 각 산업별 전력사용량 점유율(%) | `SUM(...) OVER ()` 윈도우 함수 비율 |
| I-08 | ⭐⭐⭐ | IMPOSSIBLE | 2024년 제조업 전국 계약전력 총합은? | 거절 응답 — `contract_power` 컬럼 없음 |

---

## 멀티 테이블 JOIN 시나리오 (IJ)

| ID | 난이도 | 평가 축 | 자연어 질문 | JOIN 대상 / 핵심 패턴 |
|----|--------|---------|------------|----------------------|
| IJ-01 | ⭐⭐ | JOIN + TIME | 2024년 제조업 전력 증가 TOP 5 광역시도 + 신규 고객 수 | `industry_cust_change` / 복수 CTE + YoY 비교 |
| IJ-02 | ⭐⭐ | JOIN + BASIC | 2024년 광역시도별 제조업 사용량이 가구당 평균의 몇 배? | `house_avg` / 총합 vs 평균 단위 차이 JOIN |
| IJ-03 | ⭐⭐⭐ | JOIN + RANK | 2024년 광역시도별 산업전력 대비 신재생 설비 비율 TOP 5 | `renew_energy` / kWh vs kW 단위 변환 + LEFT JOIN |
| IJ-04 | ⭐⭐⭐ | JOIN + SCHEMA | 2024년 제조업 고객 많은 TOP 5 시군구 + 전자/우편 청구서 건수 | `billing_type` / metro+city 복합키 + bill_type 환각 관찰 |

---

## 평가 축 정의

| 코드 | 설명 |
|------|------|
| BASIC | WHERE / GROUP BY 기본 집계 |
| RANK | 정렬 · TOP N |
| TIME | 연도 비교 · 계절 · 추이 |
| SCHEMA | 산업명 변형 · 컬럼 존재 여부 |
| IMPOSSIBLE | 답할 수 없는 질문 거절 |
| JOIN | 다중 테이블 조인 · CTE |

---

## JOIN 파트너 테이블

| 테이블 | JOIN 키 | 용도 |
|--------|---------|------|
| `industry_cust_change` | year, month, metro, city, biz | 신규/증설/해지 고객 수 |
| `house_avg` | year, month, metro, city | 가구당 평균 전력·요금 |
| `renew_energy` | year, metro, city | 신재생에너지 설비 용량 |
| `billing_type` | year, month, metro, city | 청구서 유형별 발송 건수 |
