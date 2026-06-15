# NL2SQL 파이프라인 진행 현황

**작성일**: 2026-06-14  
**대상 DB**: `data/kepco.db` (SQLite, 13+12+6 테이블)  
**평가 범위**: I/IJ 시리즈 12개 시나리오 (I-01~I-08, IJ-01~IJ-04)

---

## 1. 실험 목적

자연어 질문을 SQLite SQL로 자동 변환할 때, **어떤 요소가 정확도에 얼마나 기여하는지** 측정한다.

| 실험 변수 | 비교 내용 |
|-----------|-----------|
| **모델** | gpt-oss-120b · qwen3-32b · gpt-4o · claude-sonnet-4-6 |
| **테이블 선택 방식** | ① 키워드 기반 Classifier → ② 전체 스키마 직접 투입 |
| **시맨틱 레이어** | `none` (DDL만) → `examples` (예시값) → `full` (예시값+의미·규칙) |

---

## 2. 평가 시나리오 (12개)

| ID | 질문 요약 | 핵심 테이블 | 난이도 |
|----|-----------|------------|--------|
| I-01 | 2024년 3월 제조업 전력사용량 전국 총합 | industry_type | 단순 |
| I-02 | 2024년 전력 가장 많이 쓴 산업 TOP 5 | industry_type | 집계+정렬 |
| I-03 | 2024년 건설업 전력 가장 많은 광역시도 TOP 3 | industry_type | 집계+정렬 |
| I-04 | 2023→2024 제조업 전력 증감률 | industry_type | YoY 비율 |
| I-05 | 2024년 숙박음식업 계절별 전력 (봄/여름/가을/겨울) | industry_type | CASE+집계 |
| I-06 | 2024년 산업별 평균 판매단가 TOP 5 | industry_type | 가중평균 |
| I-07 | 2024년 서울 산업별 전력 점유율(%) | industry_type | 비율+JOIN |
| I-08 | 2024년 제조업 계약전력 총합 (IMPOSSIBLE) | — | 거절 판정 |
| IJ-01 | 2024년 제조업 전력 가장 많이 늘어난 광역시도 TOP 5 + 고객 증감 | industry_type + industry_cust_change | JOIN+YoY |
| IJ-02 | 2024년 광역시도별 제조업 전력 vs 가구당 전력 배수 | industry_type + house_avg | JOIN+비율 |
| IJ-03 | 2024년 광역시도별 전체 산업 전력 대비 신재생 설비 비율 | industry_type + renew_energy | JOIN+비율 |
| IJ-04 | 2024년 제조업 고객 많은 시군구 TOP 5 + 전자청구서 비율 | industry_type + billing_type | JOIN+비율 |

**I-08은 IMPOSSIBLE** — `contract_power` 컬럼이 DB에 없으므로 SQL 없이 거절해야 정답.

---

## 3. 평가 지표

| 지표 | 설명 |
|------|------|
| **EX (Execution Accuracy)** | 생성 SQL 실행 결과 = Gold SQL 결과 (IMPOSSIBLE 제외, 11개 기준) |
| **Valid Execution Rate** | 인프라 오류 제외 후 실행 성공률 |
| **IMPOSSIBLE 거절률** | I-08에서 SQL 없이 거절 응답 비율 |

**정답 판정 기준 (완화 적용)**:
- Exact match: 행·컬럼 완전 일치
- Superset match: gold 값을 gen 결과가 모두 포함하면 정답 (extra 컬럼 허용)
- NULL ↔ 0 동일 취급 (COALESCE 미적용 차이 무시)

---

## 4. 기준선 평가 결과 (Classifier 기반)

> 파이프라인: 키워드 Classifier로 테이블 선택 → 선택된 테이블 DDL만 LLM에 투입

| 모델 | EX | Valid% | IMPOSSIBLE 거절률 |
|------|----|--------|-------------------|
| openai/gpt-oss-120b | 4/11 (36.4%) | 81.8% | 0/1 (0%) |
| qwen/qwen3-32b | 4/11 (36.4%) | 72.7% | 0/1 (0%) |
| openai/gpt-4o | 2/11 (18.2%) | 63.6% | 0/1 (0%) |
| anthropic/claude-sonnet-4-6 | 3/11 (27.3%) | 81.8% | 0/1 (0%) |

### 공통 실패 패턴

| 오류 유형 | 건수 | 대표 시나리오 | 원인 |
|-----------|------|--------------|------|
| Classifier 오선택 | ~5건 | I-05, I-06, I-07 | `contract_type`·`industry_cust_change` 잘못 선택 |
| 결과 불일치 | 3~5건 | I-02, IJ-01~IJ-03 | 집계 로직 오류, YoY 계산 방식 |
| 컬럼 불일치 | 3~4건 | I-01, I-04 | 불필요한 year/month SELECT 추가 |
| IMPOSSIBLE 거절 실패 | 1건 | I-08 | 모든 모델 `contract_power` 환각 생성 |

**Classifier 주요 문제점**:
- `contract_type` 테이블에 `r"사용량"` 패턴 → 전력사용량 질문에서 잘못 선택
- `industry_type`에 `r"산업별"` 패턴 없음
- `_MULTI`의 `r"산업|고객|증감"` 패턴이 너무 넓어 `industry_cust_change` 불필요 선택

---

## 5. 파이프라인 아키텍처 변경

### Before (Classifier 방식)
```
질문 → [Classifier: 키워드 룰] → 선택된 테이블 DDL → [LLM] → SQL
```

### After (전체 스키마 방식)
```
질문 → 전체 스키마 DDL + 시맨틱 레이어 → [LLM] → SQL
```

**변경 이유**: Classifier 오선택이 실패 원인 1위 (~42%). 모델 능력보다 파이프라인 설계 문제가 크다 판단.

---

## 6. DB DDL 정비

### 문제
`metro_code`, `city_code`, `biz_code` 등 ALTER TABLE로 추가된 컬럼에 주석 없음:
```sql
-- 변경 전 (모든 테이블에서 반복)
, metro_code TEXT, city_code TEXT, biz_code TEXT)
```

### 해결
12개 테이블 DDL 재생성 — 모든 컬럼에 간단 의미 추가:
```sql
-- 변경 후
metro_code  TEXT,   -- 광역시도 코드
city_code   TEXT,   -- 시군구 코드
biz_code    TEXT    -- 산업 분류 코드
```

**재생성 대상**: billing_type, business_type, contract_type, ev_charge, ev_charge_manage, house_avg, industry_cust_change, industry_type, renew_energy, welfare_discount, contract, common_code (총 12개)

---

## 7. 시맨틱 레이어 설계

### 목적
"DDL만 있을 때 vs 시맨틱 레이어 추가 시 정확도 차이" 를 정량 측정.

### 3단계 구성

#### `none` — DDL만 (4.8k chars)
```sql
CREATE TABLE industry_type (
    -- KSIC 대분류별 전력 사용량 및 요금 통계
    year        TEXT NOT NULL,   -- 연도
    power_usage REAL,            -- 전력 사용량
    biz_code    TEXT             -- 산업 분류 코드
)
```

#### `examples` — DDL + 컬럼 예시값 (6.3k chars)
```
biz_code 예시: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U
month 예시: '01'~'12'
metro_code 예시: 11, 21, 22, ..., 41
```

#### `full` — DDL + 예시값 + 의미·단위·규칙 (8.7k chars)
```
biz_code 의미: A=농업·임업·어업, C=제조업, F=건설업, I=숙박음식 ...
power_usage 단위: kWh / bill: 원 / unit_cost: 원/kWh
unit_cost 직접 평균 금지 → ROUND(SUM(bill)/NULLIF(SUM(power_usage),0),4)
metro_code → common_code.metroCd JOIN
```

### 구현 위치
`pipeline/schema.py` — `build_full_context(semantic="none"|"examples"|"full")`

---

## 8. 평가 실행 방법

```bash
# 단일 모델, 3가지 semantic 비교
python tests/run_model_comparison.py \
  --models anthropic/claude-sonnet-4-6 \
  --semantic none examples full

# 전체 모델 비교
python tests/run_model_comparison.py \
  --models openai/gpt-oss-120b qwen/qwen3-32b anthropic/claude-sonnet-4-6 \
  --semantic none examples full

# smoke test
python tests/run_model_comparison.py --smoke --models anthropic/claude-sonnet-4-6 --semantic full
```

결과는 `tests/comparison_results.json`에 `모델명[semantic]` 키로 저장 (append 방식).

---

## 9. 다음 단계

| 순서 | 작업 | 기대 효과 |
|------|------|----------|
| 1 | **semantic 3단계 비교 평가** (현재 예정) | 시맨틱 레이어 기여도 수치화 |
| 2 | **IMPOSSIBLE 거절 프롬프트 강화** | I-08 거절률 0% → 100% |
| 3 | **고난도 시나리오 분석** (I-02, IJ-02, IJ-03) | 모든 모델 실패 패턴 파악 후 개선 |
| 4 | **few-shot 추가** | 컬럼 불일치 유형 개선 |
