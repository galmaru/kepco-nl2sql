"""[3-2] 코드 정규화: 자연어 지역명·업종명 → common_code 서브쿼리 힌트."""
from __future__ import annotations

import re

# 지역명 동의어 → common_code의 code_name (metroCd)
_METRO_SYNONYMS: dict[str, str] = {
    "서울":         "서울특별시",
    "서울시":       "서울특별시",
    "서울특별시":   "서울특별시",
    "부산":         "부산광역시",
    "부산시":       "부산광역시",
    "부산광역시":   "부산광역시",
    "대구":         "대구광역시",
    "대구시":       "대구광역시",
    "대구광역시":   "대구광역시",
    "인천":         "인천광역시",
    "인천시":       "인천광역시",
    "인천광역시":   "인천광역시",
    "광주":         "광주광역시",
    "광주시":       "광주광역시",
    "광주광역시":   "광주광역시",
    "대전":         "대전광역시",
    "대전시":       "대전광역시",
    "대전광역시":   "대전광역시",
    "울산":         "울산광역시",
    "울산시":       "울산광역시",
    "울산광역시":   "울산광역시",
    "경기":         "경기도",
    "경기도":       "경기도",
    "강원":         "강원특별자치도",
    "강원도":       "강원특별자치도",
    "강원특별자치도": "강원특별자치도",
    "충북":         "충청북도",
    "충청북도":     "충청북도",
    "충남":         "충청남도",
    "충청남도":     "충청남도",
    "전북":         "전북특별자치도",
    "전라북도":     "전북특별자치도",
    "전북특별자치도": "전북특별자치도",
    "전남":         "전라남도",
    "전라남도":     "전라남도",
    "경북":         "경상북도",
    "경상북도":     "경상북도",
    "경남":         "경상남도",
    "경상남도":     "경상남도",
    "제주":         "제주특별자치도",
    "제주도":       "제주특별자치도",
    "제주특별자치도": "제주특별자치도",
    "세종":         "세종특별자치시",
    "세종시":       "세종특별자치시",
    "세종특별자치시": "세종특별자치시",
}

# 업종명 동의어 → common_code의 code_name (bizCd)
_BIZ_SYNONYMS: dict[str, str] = {
    "농업":    "농업, 임업 및 어업",
    "임업":    "농업, 임업 및 어업",
    "어업":    "농업, 임업 및 어업",
    "광업":    "광업",
    "제조업":  "제조업",
    "전기업":  "전기, 가스, 증기 및 공기 조절 공급업",
    "가스업":  "전기, 가스, 증기 및 공기 조절 공급업",
    "수도업":  "수도, 하수 및 폐기물 처리, 원료 재생업",
    "폐기물":  "수도, 하수 및 폐기물 처리, 원료 재생업",
    "건설업":  "건설업",
    "도소매":  "도매 및 소매업",
    "도매":    "도매 및 소매업",
    "소매":    "도매 및 소매업",
    "운수업":  "운수 및 창고업",
    "창고업":  "운수 및 창고업",
    "숙박업":  "숙박 및 음식점업",
    "음식점":  "숙박 및 음식점업",
    "정보통신": "정보통신업",
    "금융업":  "금융 및 보험업",
    "보험업":  "금융 및 보험업",
    "부동산":  "부동산업",
    "전문서비스": "전문, 과학 및 기술 서비스업",
    "과학기술": "전문, 과학 및 기술 서비스업",
    "사업시설": "사업시설 관리, 사업 지원 및 임대 서비스업",
    "공공행정": "공공행정, 국방 및 사회보장 행정",
    "국방":    "공공행정, 국방 및 사회보장 행정",
    "교육":    "교육 서비스업",
    "보건업":  "보건업 및 사회복지 서비스업",
    "사회복지": "보건업 및 사회복지 서비스업",
    "예술":    "예술, 스포츠 및 여가관련 서비스업",
    "스포츠":  "예술, 스포츠 및 여가관련 서비스업",
    "여가":    "예술, 스포츠 및 여가관련 서비스업",
    "협회":    "협회 및 단체, 수리 및 기타 개인 서비스업",
}


def _subquery(code_type: str, code_name: str) -> str:
    return f"(SELECT code FROM common_code WHERE code_type='{code_type}' AND code_name='{code_name}')"


def normalize(question: str) -> dict:
    """
    질문에서 지역명·업종명을 감지하여 common_code 서브쿼리 힌트 반환.

    Returns:
        {
            "metro_hints":  {"서울": "서울특별시", ...},
            "biz_hints":    {"제조업": "제조업", ...},
            "metro_subqueries": {"서울특별시": "(SELECT ...)", ...},
            "biz_subqueries":   {"제조업": "(SELECT ...)", ...},
            "hint_text": "프롬프트에 삽입할 힌트 문자열"
        }
    """
    metro_hits: dict[str, str] = {}
    biz_hits: dict[str, str] = {}

    # 긴 키워드부터 매칭 (짧은 것이 먼저 매칭되어 오검출 방지)
    for keyword in sorted(_METRO_SYNONYMS, key=len, reverse=True):
        if re.search(re.escape(keyword), question):
            canonical = _METRO_SYNONYMS[keyword]
            metro_hits[keyword] = canonical

    for keyword in sorted(_BIZ_SYNONYMS, key=len, reverse=True):
        if re.search(re.escape(keyword), question):
            canonical = _BIZ_SYNONYMS[keyword]
            biz_hits[keyword] = canonical

    metro_subqueries = {
        name: _subquery("metroCd", name)
        for name in set(metro_hits.values())
    }
    biz_subqueries = {
        name: _subquery("bizCd", name)
        for name in set(biz_hits.values())
    }

    # 프롬프트 힌트 텍스트 생성
    lines: list[str] = []
    for name, sq in metro_subqueries.items():
        lines.append(f"  {name} → metro_code = {sq}")
    for name, sq in biz_subqueries.items():
        lines.append(f"  {name} → biz_code = {sq}")

    hint_text = "[코드 정규화 힌트]\n" + "\n".join(lines) if lines else ""

    return {
        "metro_hints":      metro_hits,
        "biz_hints":        biz_hits,
        "metro_subqueries": metro_subqueries,
        "biz_subqueries":   biz_subqueries,
        "hint_text":        hint_text,
    }
