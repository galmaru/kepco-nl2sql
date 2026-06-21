"""시맨틱 빌더: 세 레이어를 조합해 LLM 프롬프트용 컨텍스트 생성.

build_full_context(semantic) 의 semantic 옵션:
  "none"     — DDL만 (컬럼명+타입+간단의미)
  "examples" — DDL + 컬럼별 예시값
  "full"     — DDL + 예시값 + 의미·단위·규칙 + 테이블 간 JOIN 관계
"""
from __future__ import annotations

from .metadata_layer import EXAMPLES, EXCLUDE_FROM_FULL, get_ddl_rows
from .schema_linking import LINKING_RULES
from .semantic_layer import COMMON_NOTE_EXAMPLES, COMMON_NOTE_FULL, FULL


def build_full_context(semantic: str = "full") -> str:
    note_map = {
        "none":     {},
        "examples": EXAMPLES,
        "full":     FULL,
    }[semantic]

    parts: list[str] = []
    for name, ddl in get_ddl_rows():
        if name in EXCLUDE_FROM_FULL or not ddl:
            continue
        note = note_map.get(name, "")
        if note:
            parts.append(f"{ddl};\n-- [시맨틱] {note}")
        else:
            parts.append(ddl + ";")

    common = {
        "none":     "",
        "examples": COMMON_NOTE_EXAMPLES,
        "full":     COMMON_NOTE_FULL + LINKING_RULES,
    }[semantic]

    return "\n\n".join(parts) + common
