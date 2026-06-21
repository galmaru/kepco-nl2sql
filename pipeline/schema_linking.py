"""스키마 링킹 레이어: 테이블 간 JOIN 관계 및 복합키 규칙."""
from __future__ import annotations

# city_code는 metro_code 범위 내에서만 고유 — 단독 사용 시 카르테시안 곱 발생
LINKING_RULES = """
-- ===== 테이블 간 JOIN 관계 (Schema Linking) =====
--
-- ⚠️ 시군구(city_code) 단독 사용 절대 금지:
--   city_code 값은 metro_code 내에서만 고유 — 단독 GROUP BY/JOIN 시 중복 집계·카르테시안 곱 발생
--
-- ❌ 금지 패턴:
--   GROUP BY city_code                          (metro_code 없이 단독)
--   JOIN t2 ON t2.city_code = t1.city_code      (metro_code 조건 없이)
--   CTE 내 SELECT city_code ... GROUP BY city_code
--
-- ✅ 필수 패턴:
--   GROUP BY metro_code, city_code
--   JOIN t2 ON t2.metro_code = t1.metro_code AND t2.city_code = t1.city_code
--   CTE 정의 시 metro_code, city_code 둘 다 SELECT + GROUP BY에 포함
--
-- [데이터 테이블 간 JOIN 가능 키]
--   industry_type ↔ billing_type         : (metro_code, city_code, year, month)
--   industry_type ↔ industry_cust_change : (metro_code, city_code, year, month, biz_code)
--   industry_type ↔ contract_type        : (metro_code, city_code, year, month)
--   industry_type ↔ welfare_discount     : (metro_code, city_code, year, month)
--   industry_type ↔ house_avg            : (metro_code, city_code, year, month)
--   contract_type ↔ renew_energy         : (metro_code) — renew_energy에 city_code 없음
--
-- [common_code JOIN]
--   광역시도명: JOIN common_code m ON m.code_type='metroCd' AND m.code=t.metro_code
--   시군구명:   JOIN common_code c ON c.code_type='cityCd' AND c.code=t.city_code AND c.upper_code=t.metro_code"""
