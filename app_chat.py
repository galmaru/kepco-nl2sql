"""한전 데이터 어시스턴트 — 자연어 데이터 조회 챗 서비스 (Streamlit)."""
import time

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="한전 데이터 어시스턴트",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── 브랜딩 CSS ────────────────────────────────────────────
st.markdown(
    """
    <style>
      :root { --kepco-blue: #0067ac; --kepco-deep: #003d6b; --kepco-green: #00a44e; }
      .block-container { padding-top: 1.5rem; max-width: 1100px; }
      .app-header {
        background: linear-gradient(135deg, var(--kepco-deep), var(--kepco-blue));
        color: #fff; padding: 18px 24px; border-radius: 14px; margin-bottom: 8px;
      }
      .app-header h1 { margin: 0; font-size: 1.45rem; font-weight: 700; }
      .app-header p  { margin: 4px 0 0; opacity: .85; font-size: .9rem; }
      .chip button {
        border-radius: 18px !important; border: 1px solid #d6e4f0 !important;
        background: #f4f9fd !important; color: var(--kepco-deep) !important;
        font-size: .85rem !important; text-align: left !important;
      }
      .chip button:hover { border-color: var(--kepco-blue) !important; background: #e9f3fb !important; }
      .summary-card {
        background: #f4f9fd; color: #15314a; border-left: 4px solid var(--kepco-blue);
        padding: 12px 16px; border-radius: 8px; margin-bottom: 10px; font-size: 1.0rem;
      }
      .summary-card strong { color: var(--kepco-deep); }
      .stChatInput textarea { font-size: 1rem; }
      div[data-testid="stSidebarHeader"] + div { padding-top: .5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── 예시 질문 ─────────────────────────────────────────────
EXAMPLE_GROUPS = {
    "복지할인": [
        "2020~2024년 월별 복지할인 유형별 가구수 추이는?",
        "유형별 복지할인 가구수를 연도별로 비교해줘",
        "전국 복지할인 유형별 최근 현황을 알려줘",
        "경기도 의정부시의 연도별 복지할인 가구수 현황은?",
        "2024년 월별 다자녀 복지할인 가구수 추이는?",
    ],
    "청구서 발송": [
        "2024년 청구서 유형별(우편/이메일/모바일/카카오) 발송건수 현황은?",
        "2024년 광역시도별 우편 청구서 발송 비율이 높은 지역 TOP 10은?",
    ],
    "신재생에너지": [
        "2024년 발전원별 신재생에너지 설비용량 및 발전소 수는?",
        "광역시도별 태양광 신재생에너지 설비용량 현황은?",
    ],
    "계약종별": [
        "2024년 계약종별(산업용/일반용/주택용) 전력사용량 합계는?",
        "2024년 시군구별 계약종별 전력사용량 현황을 알려줘",
        "2024년 산업용 전력사용량이 가장 많은 광역시도 TOP 10은?",
        "경기도 파주시의 계약종별 월별 전력사용량 추이는?",
    ],
    "산업분류": [
        "2024년 산업분류별 월별 전력사용량 추이는?",
        "2024년 시군구별 제조업 전력사용량이 가장 많은 곳은?",
        "정보통신업 업종의 2024년 월별 전력사용량 추이는?",
        "교육 업종의 연도별 전력사용량 변화는?",
    ],
    "EV 충전소": [
        "광역시도별 EV 급속충전기와 완속충전기 수는?",
        "EV 충전소가 가장 많이 설치된 지역 TOP 5는?",
    ],
}

# ── 세션 상태 ─────────────────────────────────────────────
st.session_state.setdefault("messages", [])      # [{question, result, elapsed, model, feedback}]
st.session_state.setdefault("favorites", [])      # [질문 문자열]
st.session_state.setdefault("pending_question", None)


# ── 결과 렌더 ─────────────────────────────────────────────
def _fmt(v) -> str:
    """숫자는 천단위 콤마, 그 외는 문자열. (Decimal 포함)"""
    import numbers
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, numbers.Number):
        f = float(v)
        return f"{f:,.0f}" if f.is_integer() else f"{f:,.2f}"
    return str(v)


def _summarize(result: dict, df: pd.DataFrame | None) -> str:
    """결과를 한 줄 자연어로 요약."""
    exec_r = result.get("execution", {})
    error  = result.get("error")
    sql    = result.get("sql", "")
    if ("스키마에 없어" in (sql or "")) or ("스키마에 없어" in (error or "")):
        return "❓ 해당 데이터는 보유 범위에 없어 조회할 수 없습니다."
    if error and not exec_r.get("rows"):
        return f"⚠️ 처리 중 오류가 발생했습니다: {error}"
    if df is None or df.empty:
        return "조회 결과가 없습니다."
    n = len(df)
    if n == 1 and df.shape[1] <= 3:
        parts = [f"**{c}**: {_fmt(df.iloc[0][c])}" for c in df.columns]
        return "조회 결과 — " + " · ".join(parts)
    return f"조회 결과 **{n:,}건**을 찾았습니다. 아래 표와 차트로 확인하세요."


def _render_chart(df: pd.DataFrame, key: str) -> None:
    """결과 자동 차트."""
    import plotly.express as px
    from pipeline import chart_advisor

    s = chart_advisor.suggest_rule(df)
    x, y_lst, color, ctype = s.get("x"), s.get("y", []), s.get("color"), s.get("type", "bar")
    if not (x and y_lst and ctype != "none"):
        return
    dfp = df.copy()
    if s.get("combine_period") and "year" in dfp and "month" in dfp:
        dfp["_period"] = dfp["year"].astype(str) + "-" + dfp["month"].astype(str).str.zfill(2)
        x = "_period"
    y_col = y_lst[0] if len(y_lst) == 1 else y_lst
    fig = (px.line(dfp, x=x, y=y_col, color=color, markers=True) if ctype == "line"
           else px.bar(dfp, x=x, y=y_col, color=color, barmode="group"))
    fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=320,
                      colorway=["#0067ac", "#00a44e", "#f5a623", "#7b61ff", "#e2517a"])
    st.plotly_chart(fig, use_container_width=True, key=key)


def render_answer(msg: dict, idx: int) -> None:
    """assistant 답변 한 건 렌더."""
    result, elapsed, model = msg["result"], msg["elapsed"], msg["model"]
    exec_r = result.get("execution", {})
    rows, cols = exec_r.get("rows", []), exec_r.get("columns", [])
    df = pd.DataFrame(rows, columns=cols) if rows else None

    # 요약 카드
    st.markdown(f'<div class="summary-card">{_summarize(result, df)}</div>',
                unsafe_allow_html=True)

    if df is not None and not df.empty:
        # 단일 결과는 메트릭으로도 강조 (표 위)
        if len(df) == 1:
            mcols = st.columns(min(len(df.columns), 4))
            for col_st, c in zip(mcols, df.columns):
                col_st.metric(c, _fmt(df.iloc[0][c]))

        # 표 우선 — 항상 표를 먼저 보여줌
        st.dataframe(df, use_container_width=True, height=min(60 + len(df) * 35, 320))
        st.download_button(
            "⬇ CSV 내보내기",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name="kepco_result.csv", mime="text/csv", key=f"dl_{idx}",
        )

        # 차트는 2행 이상일 때 보조로 제공
        if len(df) > 1:
            with st.expander("📊 차트 보기", expanded=True):
                _render_chart(df, key=f"chart_{idx}")

    # 메타 + 피드백
    meta = f"⏱ {elapsed:.1f}초 · 🤖 {model.split('/')[-1]}"
    if exec_r.get("row_count"):
        meta = f"📦 {exec_r['row_count']:,}행 · " + meta
    c1, c2, c3, _ = st.columns([3, 0.6, 0.6, 4])
    c1.caption(meta)
    if c2.button("👍", key=f"up_{idx}", help="정확해요"):
        msg["feedback"] = "up"; st.toast("피드백 감사합니다 👍")
    if c3.button("👎", key=f"down_{idx}", help="부정확해요"):
        msg["feedback"] = "down"; st.toast("개선에 반영하겠습니다")

    # 상세 보기 — 처리 6단계 전체 (legacy 구성 복원)
    with st.expander("🔧 상세 보기 (처리 단계)"):
        steps  = result.get("steps", {})
        sql    = result.get("sql", "")
        safety = steps.get("safety", {})

        # ① 코드 변환
        hint = steps.get("normalizer", {}).get("hint_text", "")
        st.markdown("**① 코드 변환 (normalizer)**")
        if hint:
            st.code(hint, language="sql")
        else:
            st.caption("질문에서 지역명·업종명이 감지되지 않았습니다.")

        # ② 스키마 조합
        sem = steps.get("semantic", "")
        st.markdown(f"**② 스키마 조합 (semantic_builder)** — {steps.get('schema_length', 0):,}자 ({sem})")
        if steps.get("schema_ctx"):
            st.text_area("schema", value=steps["schema_ctx"], height=180,
                         disabled=True, label_visibility="collapsed", key=f"schema_{idx}")

        # ③ SQL 생성
        st.markdown(f"**③ SQL 생성 (generator)** — {steps.get('generator', {}).get('raw_length', 0):,}자 응답")
        t_sys, t_user, t_raw = st.tabs(["시스템 프롬프트", "유저 메시지", "LLM 응답"])
        t_sys.text_area("sys", value=steps.get("prompt_system", ""), height=160,
                        disabled=True, label_visibility="collapsed", key=f"sys_{idx}")
        t_user.text_area("user", value=steps.get("prompt_user", ""), height=160,
                         disabled=True, label_visibility="collapsed", key=f"user_{idx}")
        t_raw.text_area("raw", value=steps.get("raw", ""), height=160,
                        disabled=True, label_visibility="collapsed", key=f"raw_{idx}")

        # ③+ 자기검증 (적용 시)
        verify = steps.get("verify")
        if verify is not None:
            st.markdown("**③+ 자기검증 (verify)**")
            st.caption("수정됨 ✏️" if verify.get("changed") else "수정 없음 (원본 유지)")

        # ④ 안전성 검사
        st.markdown("**④ 안전성 검사 (safety)**")
        if safety.get("ok", True):
            st.caption("✅ SELECT/WITH 확인, 금지 키워드 없음, 테이블명 검증 통과")
        else:
            st.caption(f"❌ 차단: {safety.get('reason', '')}")

        # ⑤ SQL 실행
        st.markdown("**⑤ SQL 실행 (executor)**")
        cols_r = exec_r.get("columns", [])
        st.caption(f"반환 {exec_r.get('row_count', 0)}행 · 컬럼: {', '.join(cols_r) if cols_r else '없음'} · {elapsed:.2f}초")

        # 생성된 SQL
        if sql:
            st.markdown("**생성된 SQL**")
            st.code(sql, language="sql")


# ── 파이프라인 실행 ───────────────────────────────────────
def run_query(question: str) -> None:
    from pipeline import pipeline
    model    = st.session_state.get("cfg_model", "openai/gpt-oss-120b")
    semantic = st.session_state.get("cfg_semantic", "full")
    verify   = st.session_state.get("cfg_verify", False)
    t0 = time.time()
    result = pipeline.run(question, model=model, semantic=semantic, verify=verify)
    st.session_state.messages.append({
        "question": question, "result": result,
        "elapsed": time.time() - t0, "model": model, "feedback": None,
    })


# ── 사이드바 ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚡ 한전 데이터 어시스턴트")
    if st.button("➕ 새 대화", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

    st.divider()
    with st.expander("⚙️ 설정", expanded=False):
        st.selectbox("모델", ["openai/gpt-oss-120b", "deepseek/deepseek-v3.2",
                              "qwen/qwen3-coder-next"], key="cfg_model")
        st.radio("시맨틱 레이어", ["full", "examples", "none"], key="cfg_semantic",
                 help="full: 전체 규칙 / examples: 예시값 / none: DDL만")
        st.toggle("자기검증 사용", key="cfg_verify",
                  help="생성 SQL의 질문 조건 반영을 점검·수정")

    if st.session_state.favorites:
        st.divider()
        st.markdown("**⭐ 즐겨찾기**")
        for i, fav in enumerate(st.session_state.favorites):
            if st.button(fav, key=f"fav_{i}", use_container_width=True):
                st.session_state.pending_question = fav
                st.rerun()

    if st.session_state.messages:
        st.divider()
        st.markdown("**💬 이번 대화**")
        for m in st.session_state.messages[-8:]:
            st.caption(f"· {m['question'][:30]}")

    st.divider()
    st.caption("DB: PostgreSQL · 테이블 12개\n2021~2026년 한전 공개 데이터")


# ── 헤더 ──────────────────────────────────────────────────
st.markdown(
    '<div class="app-header"><h1>⚡ 한전 데이터 어시스턴트</h1>'
    '<p>자연어로 물어보면 사내 데이터를 조회해 답변합니다.</p></div>',
    unsafe_allow_html=True,
)

# ── 빈 상태: 환영 + 예시 칩 ───────────────────────────────
if not st.session_state.messages:
    st.markdown("#### 무엇이 궁금하신가요? 아래 예시로 시작해 보세요.")
    for category, examples in EXAMPLE_GROUPS.items():
        st.markdown(f"**{category}**")
        for i in range(0, len(examples), 3):
            cols = st.columns(3)
            for col, ex in zip(cols, examples[i:i + 3]):
                with col:
                    st.markdown('<div class="chip">', unsafe_allow_html=True)
                    if st.button(ex, key=f"ex_{ex}", use_container_width=True):
                        st.session_state.pending_question = ex
                        st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)

# ── 대화 렌더 ─────────────────────────────────────────────
for idx, msg in enumerate(st.session_state.messages):
    with st.chat_message("user"):
        st.markdown(msg["question"])
        if msg["question"] not in st.session_state.favorites:
            if st.button("⭐ 즐겨찾기", key=f"addfav_{idx}"):
                st.session_state.favorites.append(msg["question"])
                st.rerun()
    with st.chat_message("assistant", avatar="⚡"):
        render_answer(msg, idx)

# ── 입력 처리 ─────────────────────────────────────────────
prompt = st.chat_input("질문을 입력하세요 (예: 2024년 제조업 전력사용량이 가장 많은 시도는?)")
pending = st.session_state.pending_question
st.session_state.pending_question = None

question = prompt or pending
if question:
    with st.spinner("데이터를 조회하고 있습니다..."):
        run_query(question)
    st.rerun()
