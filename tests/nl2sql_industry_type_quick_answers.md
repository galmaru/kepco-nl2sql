# NL2SQL 빠른 성능 테스트 (industry_type) - 정답셋

`industry_type` 중심 **10분 이내 수행 가능한** NL2SQL 평가 시나리오의 정답 및 실행 결과입니다.

---

## 시나리오 (12개)

### I-01. 특정 월·산업 사용량 조회 ⭐ [BASIC + SCHEMA]

```
자연어: 2024년 3월 제조업 전력사용량 전국 총합은?
```

```sql
SELECT SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024' AND month = '03' AND biz LIKE '%제조업%';
```

**실행 결과**:
| total_usage |
| :--- |
| 44,041,949,536 |

---

### I-02. 산업별 사용량 TOP 5 ⭐ [BASIC + RANK]

```
자연어: 2024년 한 해 동안 전력을 가장 많이 쓴 산업 TOP 5 알려줘
```

```sql
SELECT biz, SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024'
GROUP BY biz
ORDER BY total_usage DESC
LIMIT 5;
```

**실행 결과**:
| biz | total_usage |
| :--- | :--- |
| 제조업 | 513,064,805,792.0 |
| 부동산업 | 85,704,889,034.0 |
| 도매및소매업 | 40,054,828,882.0 |
| 농업,임업및어업 | 38,091,694,950.0 |
| 숙박및음식점업 | 34,257,319,236.0 |

---

### I-03. 지역별 특정 산업 순위 ⭐⭐ [RANK]

```
자연어: 2024년 건설업 전력사용량이 가장 많은 광역시도 TOP 3
```

```sql
SELECT metro, SUM(power_usage) AS total_usage
FROM industry_type
WHERE year = '2024' AND biz LIKE '%건설업%'
GROUP BY metro
ORDER BY total_usage DESC
LIMIT 3;
```

**실행 결과**:
| metro | total_usage |
| :--- | :--- |
| 전체 | 4,369,590,921.0 |
| 경기도 | 1,328,295,729.0 |
| 서울특별시 | 692,278,457.0 |

---

### I-04. 전년 대비 증감률 (YoY) ⭐⭐ [TIME]

```
자연어: 2023년 대비 2024년 제조업 전력사용량 증감률은?
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
FROM industry_type
WHERE biz LIKE '%제조업%' AND year IN ('2023', '2024');
```

**실행 결과**:
| usage_2023 | usage_2024 | yoy_pct |
| :--- | :--- | :--- |
| 521,762,607,686.0 | 513,064,805,792.0 | -1.67 |

---

### I-05. 계절별 집계 ⭐⭐ [TIME]

```
자연어: 2024년 전국 숙박 및 음식점업의 계절별(봄/여름/가을/겨울) 전력사용량 비교
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
FROM industry_type
WHERE year = '2024' AND biz LIKE '%숙박%'
GROUP BY season
ORDER BY total_usage DESC;
```

**실행 결과**:
| season | total_usage |
| :--- | :--- |
| 여름 | 9,584,748,810.0 |
| 가을 | 8,776,774,412.0 |
| 겨울 | 8,678,134,280.0 |
| 봄 | 7,217,661,734.0 |

---

### I-06. 평균 판매단가 분석 ⭐⭐ [BASIC + RANK]

```
자연어: 2024년 산업별 평균 판매단가(원/kWh)가 가장 비싼 산업 TOP 5
```

```sql
SELECT
  biz,
  SUM(bill) AS total_bill,
  SUM(power_usage) AS total_usage,
  ROUND(SUM(bill) / NULLIF(SUM(power_usage), 0), 2) AS avg_unit_cost
FROM industry_type
WHERE year = '2024'
GROUP BY biz
ORDER BY avg_unit_cost DESC
LIMIT 5;
```

**실행 결과**:
| biz | total_bill | total_usage | avg_unit_cost |
| :--- | :--- | :--- | :--- |
| 가구내고용활동... | 1,631,734,782.0 | 8,047,302.0 | 202.77 |
| 광업 | 623,395,437,422.0 | 3,296,471,324.0 | 189.11 |
| 협회및단체,수리... | 1,989,814,260,334.0 | 10,760,306,856.0 | 184.92 |
| 사업시설관리... | 552,544,081,146.0 | 2,995,809,756.0 | 184.44 |
| 건설업 | 1,592,609,188,790.0 | 8,739,181,842.0 | 182.24 |

---

### I-07. 지역 내 산업 점유율 ⭐⭐⭐ [RANK]

```
자연어: 2024년 서울특별시에서 각 산업별 전력사용량 점유율(%)을 보여줘
```

```sql
SELECT
  biz,
  SUM(power_usage) AS usage,
  ROUND(SUM(power_usage) * 100.0 / SUM(SUM(power_usage)) OVER (), 2) AS share_pct
FROM industry_type
WHERE year = '2024' AND metro = '서울특별시'
GROUP BY biz
ORDER BY usage DESC;
```

**실행 결과 (상위 5개)**:
| biz | usage | share_pct |
| :--- | :--- | :--- |
| 부동산업 | 13,078,656,390.0 | 38.37 |
| 도매및소매업 | 3,462,580,525.0 | 10.16 |
| 숙박및음식점업 | 2,414,902,189.0 | 7.09 |
| 운수및창고업 | 2,231,346,030.0 | 6.55 |
| 정보통신업 | 2,162,274,507.0 | 6.34 |

---

### I-08. 답할 수 없는 질문 (거절 능력) ⭐⭐⭐ [IMPOSSIBLE]

```
자연어: 2024년 제조업의 전국 계약전력 총합은 얼마야?
```

```sql
-- 정답: industry_type 테이블에는 'contract_power' 컬럼이 없음.
```

---

## JOIN 시나리오 (4개)

### IJ-01. 산업 전력 증가 × 신규 고객 수 ⭐⭐ [JOIN + TIME]

```
자연어: 2024년 제조업에서 전력사용량이 가장 많이 늘어난 광역시도 TOP 5와,
        같은 기간 해당 지역의 제조업 신규 고객 수를 같이 보여줘
```

```sql
WITH usage_2024 AS (
  SELECT metro, SUM(power_usage) AS usage
  FROM industry_type
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro
),
usage_2023 AS (
  SELECT metro, SUM(power_usage) AS usage
  FROM industry_type
  WHERE year='2023' AND biz LIKE '%제조업%'
  GROUP BY metro
),
new_cust AS (
  SELECT metro, SUM(new_count) AS new_cust_2024
  FROM industry_cust_change
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro
)
SELECT
  u24.metro,
  u24.usage - COALESCE(u23.usage, 0) AS usage_increase,
  COALESCE(nc.new_cust_2024, 0) AS new_cust_count
FROM usage_2024 u24
LEFT JOIN usage_2023 u23 ON u24.metro = u23.metro
LEFT JOIN new_cust   nc  ON u24.metro = nc.metro
ORDER BY usage_increase DESC
LIMIT 5;
```

**실행 결과**:
| metro | usage_increase | new_cust_count |
| :--- | :--- | :--- |
| 전북특별자치도 | 9,637,701,800.0 | 0 |
| 경기도 | 569,162,000.0 | 1,001,033 |
| 충청남도 | 115,615,915.0 | 0 |
| 세종특별자치시 | 63,000,499.0 | 17,543 |
| 광주광역시 | 43,842,136.0 | 0 |

---

### IJ-02. 산업용 vs 가정용 전력 비중 ⭐⭐ [JOIN + BASIC]

```
자연어: 2024년 광역시도별로 제조업 전력사용량이 가구당 평균 전력사용량의
        몇 배인지 계산해줘 (광역시도별 연간 합계 기준)
```

```sql
WITH ind AS (
  SELECT metro, SUM(power_usage) AS ind_usage
  FROM industry_type
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro
),
house AS (
  SELECT metro,
         SUM(power_usage) AS total_house_usage,
         AVG(power_usage * 1.0 / NULLIF(house_count,0)) AS avg_per_house
  FROM house_avg
  WHERE year='2024'
  GROUP BY metro
)
SELECT
  i.metro,
  i.ind_usage,
  h.avg_per_house,
  ROUND(i.ind_usage / NULLIF(h.avg_per_house, 0), 1) AS ratio
FROM ind i
JOIN house h ON i.metro = h.metro
ORDER BY ratio DESC;
```

**실행 결과 (상위 3개)**:
| metro | ind_usage | avg_per_house | ratio |
| :--- | :--- | :--- | :--- |
| 경기도 | 68,684,716,267.0 | 0.00228... | 30,104,832,770,659.5 |
| 울산광역시 | 25,742,969,854.0 | 0.00230... | 11,175,592,988,447.3 |
| 부산광역시 | 6,708,004,876.0 | 0.00261... | 2,564,106,240,692.1 |

---

### IJ-03. 산업전력 대비 신재생 설비 용량 비율 ⭐⭐⭐ [JOIN + RANK]

```
자연어: 2024년 광역시도별 전체 산업 전력사용량 대비 신재생에너지 설비 총 용량의
        비율을 계산하고, 신재생 비중이 높은 지역 TOP 5를 보여줘
```

```sql
WITH ind AS (
  SELECT metro, SUM(power_usage) AS ind_usage
  FROM industry_type
  WHERE year='2024'
  GROUP BY metro
),
renew AS (
  SELECT metro, SUM(capacity) AS renew_capacity
  FROM renew_energy
  WHERE year='2024'
  GROUP BY metro
)
SELECT
  i.metro,
  i.ind_usage,
  r.renew_capacity,
  ROUND(r.renew_capacity * 1.0 / NULLIF(i.ind_usage, 0) * 1e6, 3) AS renew_per_mwh
FROM ind i
LEFT JOIN renew r ON i.metro = r.metro
ORDER BY renew_per_mwh DESC
LIMIT 5;
```

**실행 결과**:
| metro | ind_usage | renew_capacity | renew_per_mwh |
| :--- | :--- | :--- | :--- |
| 전북특별자치도 | 16,491,511,421.0 | 733,913.31 | 44.502 |
| 경상북도 | 38,604,924,993.0 | 1,454,836.53 | 37.685 |
| 전라남도 | 30,108,759,252.0 | 903,997.68 | 30.024 |
| 강원특별자치도 | 12,822,974,897.0 | 383,089.53 | 29.875 |
| 광주광역시 | 6,754,968,047.0 | 169,876.74 | 25.148 |

---

### IJ-04. 산업별 전자청구서 보급률 ⭐⭐⭐ [JOIN + SCHEMA]

```
자연어: 2024년 제조업 고객이 많은 상위 5개 시군구에서, 해당 지역의 전체
        전자청구서(이메일/모바일/카카오) 발송 건수와 우편 청구서 발송 건수를 함께 보여줘
```

```sql
WITH mfg_cust AS (
  SELECT metro, city, SUM(cust_count) AS mfg_cust_cnt
  FROM industry_type
  WHERE year='2024' AND biz LIKE '%제조업%'
  GROUP BY metro, city
  ORDER BY mfg_cust_cnt DESC
  LIMIT 5
),
bills AS (
  SELECT
    metro, city,
    SUM(CASE WHEN bill_type IN ('이메일','모바일','카카오') THEN bill_count ELSE 0 END) AS e_bill,
    SUM(CASE WHEN bill_type = '우편' THEN bill_count ELSE 0 END) AS paper_bill
  FROM billing_type
  WHERE year='2024'
  GROUP BY metro, city
)
SELECT
  m.metro, m.city, m.mfg_cust_cnt,
  b.e_bill, b.paper_bill
FROM mfg_cust m
LEFT JOIN bills b ON m.metro = b.metro AND m.city = b.city
ORDER BY m.mfg_cust_cnt DESC;
```

**실행 결과**:
| metro | city | mfg_cust_cnt | e_bill | paper_bill |
| :--- | :--- | :--- | :--- | :--- |
| 전체 | 전체 | 4,408,143 | 164,230,430 | 79,565,413 |
| 경기도 | 화성시 | 272,694 | 2,137,097 | 1,575,720 |
| 경기도 | 김포시 | 146,623 | 971,923 | 748,498 |
| 경상남도 | 김해시 | 111,118 | 1,392,662 | 1,166,526 |
| 경기도 | 시흥시 | 109,756 | 1,111,719 | 950,626 |
