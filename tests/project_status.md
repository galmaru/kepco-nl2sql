# 프로젝트 현황

## 파이프라인 모듈 구성 (2026-06-15 기준)

`pipeline/` 디렉토리:
- `normalizer.py`: metroCd/bizCd → common_code 서브쿼리 힌트 생성
- `schema.py`: DDL + 시맨틱 레이어 컨텍스트 생성 (none/examples/full 3단계)
- `generator.py`: OpenRouter gpt-oss-120b, temperature=0, 공통 SQL 규칙 12개
- `safety.py`: SELECT/WITH 전용, CTE 별칭 포함 허용 테이블 검사
- `executor.py`: 읽기 전용 SQLite, LIMIT 100, 5초 타임아웃
- `evaluator.py`: gold SQL 비교 평가 (superset/subset match, NULL↔0 동일, round 6자리)

`classifier.py`: 삭제됨 — 전체 스키마 투입 방식으로 전환

---

## 코드 변경 이력

| 날짜 | 항목 | 내용 |
|------|------|------|
| 2026-06-15 | business_type 삭제 | DB DROP + schema.py + CLAUDE.md 일괄 제거 |
| 2026-06-15 | classifier.py 삭제 | 전체 스키마 방식 전환 |
| 2026-06-15 | generator.py 정리 | 테이블별 규칙 3개 → schema.py _SEMANTIC_FULL 이동 (15→12개 규칙) |
| 2026-06-15 | pipeline.py 버그 수정 | execution 초기값 `{}` → 기본 구조체 |
| 2026-06-15 | evaluator.py 개선 | round 6자리 + `_subset_match` 추가 (gen ⊆ gold 허용) |
| 2026-06-15 | schema.py 보강 | billing_type 전자청구서 CASE WHEN 합산 예시 추가 |
| 2026-06-15 | generator.py 규칙 수정 | city JOIN CTE 내부 적용 명시, 거절 조건 완화 |
| 2026-06-15 | nl2sql_scenarios.md | I-04 gold SQL ROUND 6자리로 변경 |
| 2026-06-15 | nl2sql_scenarios.md | IJ-03 시나리오 재정의: kWh vs kW 단위 불일치 → contract_type+renew_energy 동일 단위(kW) 비율로 교체 |
| 2026-06-15 | schema.py 보강 | renew_energy: capacity SUM 금지·area_capacity MAX 예시 추가 / 조회불가 문구에 contract_type.contract_power 조회 가능 명시 |

---

## 최종 평가 결과 (2026-06-15, full 시맨틱레이어 기준)

평가 범위: I/IJ 12개 시나리오 (I-08 IMPOSSIBLE, EX 집계 제외)

### semantic=full

| 모델 | EX | valid% | IMPOSSIBLE 거절 |
|------|----|--------|----------------|
| **gpt-oss-120b** | **11/11 (100.0%)** | 100% | 1/1 (100%) |
| **gpt-4o** | **10/11 (90.9%)** | 90.9% | 1/1 (100%) |
| qwen3-32b | 7/11 (63.6%) | 81.8% | 1/1 (100%) |

### 시맨틱레이어 단계별 비교

| 모델 | none | examples | full |
|------|------|----------|------|
| gpt-oss-120b | 54.5% | 45.5% | **100.0%** |
| gpt-4o | 27.3% | 36.4% | **90.9%** |
| qwen3-32b | 36.4% | 45.5% | 63.6% |

---

## 잔여 실패 케이스 (full 기준)

| 모델 | 시나리오 | 오류 유형 | 원인 |
|------|---------|---------|------|
| gpt-4o | IJ-04 | 타임아웃 | city_code JOIN 카르테시안 곱 |
| qwen3-32b | I-04, IJ-01, IJ-02, IJ-04 | 다수 | — |

IJ-03 시나리오는 단위 불일치(kWh vs kW) 문제로 재정의 완료 → contract_type + renew_energy 조합으로 교체.

---

## 생성 문서

| 파일 | 설명 |
|------|------|
| `tests/generate_report.py` | comparison_results.json → 마크다운 리포트 생성 |
| `tests/nl2sql_eval_report.md` | 모델 비교 평가 리포트 (자동 생성) |
| `tests/accuracy_improvement_plan.md` | 정확도 개선 계획 및 완료 이력 |
| `tests/core_research_questions.md` | 핵심 연구 질문 및 결론 |
| `tests/project_status.md` | 본 파일 |
