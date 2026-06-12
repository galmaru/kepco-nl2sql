"""[3-1] 테이블 선택기: 자연어 질문 → 대상 테이블 목록."""
from __future__ import annotations

import re

# 테이블별 트리거 키워드 (정규식 패턴)
_RULES: list[tuple[str, list[str]]] = [
    ("contract_type",        [r"계약종", r"주택용", r"일반용", r"산업용", r"교육용", r"농사용", r"가로등", r"심야", r"계약전력", r"전기요금", r"판매단가", r"단가", r"사용량"]),
    ("industry_type",        [r"산업분류", r"업종별", r"제조업", r"건설업", r"도소매", r"정보통신", r"금융보험", r"보건복지", r"공공행정", r"KSIC", r"bizCd"]),
    ("business_type",        [r"세분류업종", r"업종코드", r"bizType", r"반도체", r"자동차", r"섬유"]),
    ("billing_type",         [r"청구서", r"청구방식", r"모바일청구", r"이메일청구", r"우편청구", r"인편청구", r"청구 건수", r"청구건수"]),
    ("welfare_discount",     [r"복지할인", r"기초수급", r"장애", r"유공자", r"다자녀", r"대가족", r"사회복지시설", r"생명유지"]),
    ("industry_cust_change", [r"고객증감", r"신규계약", r"증설", r"해지", r"고객수 변동", r"신규 고객", r"계약 변동"]),
    ("house_avg",            [r"가구당", r"가구 평균", r"평균 사용량", r"주거용 평균"]),
    ("contract",             [r"입찰", r"계약공고", r"낙찰", r"발주", r"공고번호", r"경쟁입찰"]),
    ("ev_charge",            [r"EV", r"전기차", r"충전소", r"급속충전", r"완속충전", r"충전기 수", r"충전 인프라"]),
    ("ev_charge_manage",     [r"충전기 운영", r"충전기 상태", r"충전기 ID"]),
    ("dispersed_gen",        [r"분산전원", r"분산발전", r"배전선로", r"변전소"]),
    ("renew_energy",         [r"신재생", r"태양광", r"풍력", r"소수력", r"연료전지", r"재생에너지", r"자급률"]),
]

# 멀티테이블 시너지 패턴 (두 키워드가 같이 나오면 두 테이블 모두 선택)
_MULTI: list[tuple[str, str, str, str]] = [
    (r"주택용|전력사용", r"복지|할인", "contract_type", "welfare_discount"),
    (r"청구|모바일", r"전력|사용량", "billing_type", "contract_type"),
    (r"태양광|신재생", r"전력|사용|계약", "renew_energy", "contract_type"),
    (r"태양광|신재생", r"EV|충전", "renew_energy", "ev_charge"),
    (r"산업|고객|증감", r"전력|사용", "industry_cust_change", "contract_type"),
]


def classify(question: str) -> list[str]:
    """질문에서 대상 테이블 목록 반환 (중복 제거, 입력 순 유지)."""
    selected: list[str] = []

    for table, patterns in _RULES:
        for p in patterns:
            if re.search(p, question, re.IGNORECASE):
                if table not in selected:
                    selected.append(table)
                break

    # 멀티테이블 패턴 추가
    for pat_a, pat_b, tbl_a, tbl_b in _MULTI:
        if re.search(pat_a, question, re.IGNORECASE) and re.search(pat_b, question, re.IGNORECASE):
            for t in (tbl_a, tbl_b):
                if t not in selected:
                    selected.append(t)

    # 기본값: 아무것도 매칭 안 되면 contract_type
    if not selected:
        selected = ["contract_type"]

    return selected
