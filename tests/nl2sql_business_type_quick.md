# NL2SQL 빠른 성능 테스트 (business_type)

`business_type` 단일 테이블 대상 **10분 이내 수행 가능한** NL2SQL 평가 시나리오.
난이도·SQL 패턴·함정 요소를 고르게 배치한 8개 시나리오.

---

## 테이블 개요

**`business_type`** — 상세 업종별(반도체·자동차·철강 등) 전력 사용량·계약전력 통계

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `year` | TEXT | 연도 ('2021'~'2026') |
| `month` | TEXT | 월 ('01'~'12') |
| `metro` | TEXT | 광역시도명 |
| `city` | TEXT | 시군구명 |
| `biz_type` | TEXT | 업종명 (38종, 예: `반도체`, `자동차`, `1차   금속`) |
| `cust_count` | INTEGER | 고객 수 |
| `power_usage` | REAL | 전력 사용량 (kWh) |
| `contract_power` | REAL | 계약전력 (kW) |

**데이터 범위**: 2021-01 ~ 2026-02 (총 471,668행)

---

## 데이터 특성 주의사항 (함정 요소)

| 함정 | 설명 |
|------|------|
| **지역명 변경** | 2023년 중반부터 `강원도` → `강원특별자치도`로 변경. 두 값 모두 존재 |
| **북한 데이터** | `황해북도` (개성공단 추정) 존재, 대부분 `power_usage=0` |
| **biz_type 공백** | 일부 업종명에 공백 다수 포함 (예: `1차   금속`, `관  공  용`) |
| **bill 컬럼 없음** | 요금·단가 관련 질문은 답할 수 없음 (`contract_type` 테이블과 구분 필요) |
| **2026년 부분 데이터** | 1~2월까지만 존재 |

---

## 평가 축

각 시나리오는 아래 축 중 하나 이상을 테스트합니다.

- **BASIC**: WHERE/GROUP BY 기본 집계
- **RANK**: 정렬·TOP N
- **TIME**: 연도 비교·계절·추이
- **SCHEMA**: 스키마 함정 (지역명 변경·북한·공백)
- **IMPOSSIBLE**: 답할 수 없는 질문 거절 능력

---

## 시나리오 (8개, 예상 실행 시간 ~8분)

### B-01. 특정 월·업종 사용량 조회 ⭐ [BASIC]

```
자연어: 2024년 3월 반도체 업종 전국 전력사용량 총합은?
```

```sql
SELECT SUM(power_usage) AS total_usage
FROM business_type
WHERE year = '2024' AND month = '03' AND biz_type = '반도체';
```

**평가 포인트**: 정확한 WHERE 필터, 단순 집계 함수 사용

---

### B-02. 업종별 사용량 TOP 5 ⭐ [BASIC + RANK]

```
자연어: 2024년 전체 기간에서 전력을 가장 많이 쓴 업종 TOP 5 알려줘
```

```sql
SELECT biz_type, SUM(power_usage) AS total_usage
FROM business_type
WHERE year = '2024'
GROUP BY biz_type
ORDER BY total_usage DESC
LIMIT 5;
```

**평가 포인트**: GROUP BY + ORDER BY + LIMIT

---

### B-03. 지역별 특정 업종 순위 ⭐⭐ [RANK + SCHEMA]

```
자연어: 2024년 자동차 업종 전력사용량이 가장 많은 시도 TOP 3
```

```sql
SELECT metro, SUM(power_usage) AS total_usage
FROM business_type
WHERE year = '2024' AND biz_type = '자동차'
GROUP BY metro
ORDER BY total_usage DESC
LIMIT 3;
```

**평가 포인트**: `강원도`/`강원특별자치도` 공존 시 중복 집계 여부 확인

---

### B-04. 전년 대비 증감률 (YoY) ⭐⭐ [TIME]

```
자연어: 2023년 대비 2024년 반도체 업종 전력사용량 증감률은?
```

```sql
SELECT
  SUM(CASE WHEN year='2023' THEN power_usage END) AS usage_2023,
  SUM(CASE WHEN year='2024' THEN power_usage END) AS usage_2024,
  ROUND(
    (SUM(CASE WHEN year='2024' THEN power_usage END)
     - SUM(CASE WHEN year='2023' THEN power_usage END)) * 100.0
    / SUM(CASE WHEN year='2023' THEN power_usage END), 2
  ) AS yoy_pct
FROM business_type
WHERE biz_type = '반도체' AND year IN ('2023', '2024');
```

**평가 포인트**: CASE WHEN 집계 또는 self-JOIN으로 연도 비교

---

### B-05. 계절별 집계 ⭐⭐ [TIME]

```
자연어: 2024년 전국 철강 업종의 계절별(봄/여름/가을/겨울) 전력사용량 비교
```

```sql
SELECT
  CASE
    WHEN month IN ('03','04','05') THEN '봄'
    WHEN month IN ('06','07','08') THEN '여름'
    WHEN month IN ('09','10','11') THEN '가을'
    ELSE '겨울'
  END AS season,
  SUM(power_usage) AS total_usage
FROM business_type
WHERE year = '2024' AND biz_type LIKE '%철강%'
GROUP BY season
ORDER BY total_usage DESC;
```

**평가 포인트**: CASE WHEN 그룹화, 계절 매핑 정확성

---

### B-06. 고객당 평균 사용량 ⭐⭐ [BASIC + RANK]

```
자연어: 2024년 업종별 고객당 평균 전력사용량이 가장 높은 업종 TOP 5
```

```sql
SELECT
  biz_type,
  SUM(cust_count) AS total_cust,
  SUM(power_usage) AS total_usage,
  ROUND(SUM(power_usage) / NULLIF(SUM(cust_count), 0), 2) AS usage_per_cust
FROM business_type
WHERE year = '2024'
GROUP BY biz_type
HAVING total_cust > 0
ORDER BY usage_per_cust DESC
LIMIT 5;
```

**평가 포인트**: 파생 지표 계산, NULLIF로 0 나눗셈 방어

---

### B-07. 업종별 점유율 + 윈도우 함수 ⭐⭐⭐ [RANK]

```
자연어: 2024년 경기도에서 업종별 전력사용량 점유율(%)과 순위를 함께 보여줘
```

```sql
SELECT
  biz_type,
  SUM(power_usage) AS total_usage,
  ROUND(SUM(power_usage) * 100.0 / SUM(SUM(power_usage)) OVER (), 2) AS share_pct,
  RANK() OVER (ORDER BY SUM(power_usage) DESC) AS rnk
FROM business_type
WHERE year = '2024' AND metro = '경기도'
GROUP BY biz_type
ORDER BY rnk;
```

**평가 포인트**: 윈도우 함수 사용, SUM OVER ()로 전체 대비 비율

---

### B-08. 답할 수 없는 질문 (거절 능력) ⭐⭐⭐ [IMPOSSIBLE]

```
자연어: 2024년 반도체 업종의 전기요금 총액은 얼마야?
```

```sql
-- 정답: business_type 테이블에는 'bill' 컬럼이 없음.
-- 모델은 "해당 테이블에 요금 정보 없음" 응답 또는 contract_type 테이블 제안을 해야 함.
-- 틀린 답 예시: SELECT SUM(bill) FROM business_type ...  (존재하지 않는 컬럼)
```

**평가 포인트**: 환각(hallucination) 방지, 스키마 한계 인식

---

## 평가 체크리스트

각 시나리오 실행 후 아래 항목 체크:

| 항목 | 설명 |
|------|------|
| ✅ **실행 성공** | SQL이 에러 없이 실행됨 |
| ✅ **결과 정확** | gold SQL 실행 결과와 동일 (컬럼 순서·별칭 무시) |
| ✅ **함정 회피** | B-03: `강원도+강원특별자치도` 합산 여부 확인 등 |
| ✅ **거절 정확** | B-08에서 환각 SQL 생성 안 함 |

---

## 점수 환산 예시

| 결과 | 점수 |
|------|------|
| 실행 성공 + 결과 정확 | 1.0 |
| 실행 성공 + 결과 일부 불일치 | 0.5 |
| 실행 실패 또는 잘못된 테이블 참조 | 0.0 |
| B-08: 올바른 거절 | 1.0 / 환각 SQL 생성 | 0.0 |

**총점 8점 만점**, 평균 정확도 = 총점 / 8

---

## 실행 방법 (수동 예시)

```bash
# gold SQL 실행 결과 미리 저장
python3 -c "
import sqlite3
conn = sqlite3.connect('data/kepco.db')
# B-01 검증
print(conn.execute(\"SELECT SUM(power_usage) FROM business_type WHERE year='2024' AND month='03' AND biz_type='반도체'\").fetchone())
"
```

모델 출력 SQL을 동일하게 실행해 결과 비교.
