"""NL2SQL 데모 웹 앱 (Streamlit)."""
import io
import time
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="한전 오픈 데이터 자연어 조회",
    page_icon="⚡",
    layout="wide",
)

# ── 예시 질문 ─────────────────────────────────────────────
EXAMPLE_GROUPS = [
    ("복지할인", [
        "2020~2024년 월별 복지할인 유형별 가구수 추이는?",
        "유형별 복지할인 가구수를 연도별로 비교해줘",
        "전국 복지할인 유형별 최근 현황을 알려줘",
        "경기도 의정부시의 연도별 복지할인 가구수 현황은?",
        "2024년 월별 다자녀 복지할인 가구수 추이는?",
    ]),
    ("청구서 발송", [
        "2024년 청구서 유형별(우편/이메일/모바일/카카오) 발송건수 현황은?",
        "2024년 광역시도별 우편 청구서 발송 비율이 높은 지역 TOP 10은?",
    ]),
    ("신재생에너지", [
        "2024년 발전원별 신재생에너지 설비용량 및 발전소 수는?",
        "광역시도별 태양광 신재생에너지 설비용량 현황은?",
    ]),
    ("계약종별", [
        "2024년 계약종별(산업용/일반용/주택용) 전력사용량 합계는?",
        "2024년 시군구별 계약종별 전력사용량 현황을 알려줘",
        "2024년 산업용 전력사용량이 가장 많은 광역시도 TOP 10은?",
        "경기도 파주시의 계약종별 월별 전력사용량 추이는?",
    ]),
    ("산업분류", [
        "2024년 산업분류별 월별 전력사용량 추이는?",
        "2024년 시군구별 제조업 전력사용량이 가장 많은 곳은?",
        "정보통신업 업종의 2024년 월별 전력사용량 추이는?",
        "교육 업종의 연도별 전력사용량 변화는?",
    ]),
    ("EV 충전소", [
        "광역시도별 EV 급속충전기와 완속충전기 수는?",
        "EV 충전소가 가장 많이 설치된 지역 TOP 5는?",
    ]),
]

# ── 헤더 ─────────────────────────────────────────────────
st.title("⚡ 한전 오픈 데이터 자연어 조회")
st.caption("자연어 질문을 SQL로 자동 변환하여 조회합니다.")
st.divider()

# ── 사이드바: 설정 ────────────────────────────────────────
with st.sidebar:
    st.header("설정")
    model = st.selectbox(
        "모델",
        ["openai/gpt-oss-120b", "deepseek/deepseek-v3.2", "qwen/qwen3-coder-next"],
        index=0,
    )
    semantic = st.radio(
        "시맨틱 레이어",
        ["full", "examples", "none"],
        index=0,
        help="full: 집계규칙·조인관계 포함 / examples: 예시값만 / none: DDL만",
    )
    st.divider()
    st.caption("한전 전력 데이터 NL2SQL 데모\n\nDB: PostgreSQL\n테이블: 12개")

# ── 세션 상태 ─────────────────────────────────────────────
if "question_val" not in st.session_state:
    st.session_state.question_val = ""
if "auto_run" not in st.session_state:
    st.session_state.auto_run = False
if "viz_mode" not in st.session_state:
    st.session_state.viz_mode = "숨김"

# ── 예시 질문 버튼 (form 밖) ───────────────────────────────
st.markdown("**예시:**")
tabs = st.tabs([category for category, _ in EXAMPLE_GROUPS])
for tab, (category, examples) in zip(tabs, EXAMPLE_GROUPS):
    with tab:
        cols = st.columns(len(examples))
        for i, (col, ex) in enumerate(zip(cols, examples)):
            if col.button(ex, key=f"ex_{category}_{i}", use_container_width=True):
                st.session_state.question_val = ex
                st.session_state.auto_run = True
                st.rerun()

# ── 입력 폼 (엔터 키로 submit) ────────────────────────────
with st.form("query_form", clear_on_submit=False):
    col_input, col_btn = st.columns([5, 1])
    with col_input:
        question = st.text_input(
            "질문을 입력하세요",
            value=st.session_state.question_val,
            placeholder="예: 2024년 제조업 전력사용량이 가장 많은 광역시도는?",
            label_visibility="collapsed",
        )
    with col_btn:
        run_btn = st.form_submit_button("조회", type="primary", use_container_width=True)

# auto_run 처리 (예시 버튼 클릭)
should_run = run_btn or st.session_state.auto_run
effective_question = question or st.session_state.question_val
if st.session_state.auto_run:
    st.session_state.auto_run = False

st.divider()

# ── 실행 ─────────────────────────────────────────────────
if should_run and effective_question:
    from pipeline import pipeline

    with st.spinner("처리 중..."):
        t0 = time.time()
        result = pipeline.run(effective_question, model=model, semantic=semantic)
        elapsed = time.time() - t0

    st.session_state.last_result  = result
    st.session_state.last_elapsed = elapsed
    st.session_state.last_model   = model

# 결과 렌더링 (다운로드 버튼 클릭 후 rerun 시에도 유지)
if "last_result" in st.session_state:
    result  = st.session_state.last_result
    elapsed = st.session_state.last_elapsed
    model   = st.session_state.last_model

    steps   = result.get("steps", {})
    sql     = result.get("sql", "")
    exec_r  = result.get("execution", {})
    error   = result.get("error")

    left, right = st.columns([1, 2])

    # ── 처리 단계 (좌) ────────────────────────────────────
    with left:
        st.subheader("처리 단계")

        # ① 코드 변환
        norm    = steps.get("normalizer", {})
        hint    = norm.get("hint_text", "")
        label1  = "✅ ① 코드 변환" if hint else "➖ ① 코드 변환 — 변환 없음"
        with st.expander(label1, expanded=bool(hint)):
            if hint:
                st.code(hint, language="sql")
            else:
                st.caption("질문에서 지역명·업종명이 감지되지 않았습니다.")

        # ② 스키마 조합
        schema_len = steps.get("schema_length", 0)
        sem_level  = steps.get("semantic", "")
        schema_ctx = steps.get("schema_ctx", "")
        level_desc = {
            "full":     "DDL + 예시값 + 집계규칙·조인관계",
            "examples": "DDL + 컬럼별 예시값",
            "none":     "DDL만 (컬럼명·타입)",
        }
        with st.expander(f"✅ ② 스키마 조합 — {schema_len:,}자 ({sem_level})", expanded=False):
            st.caption(level_desc.get(sem_level, ""))
            if schema_ctx:
                st.text_area("", value=schema_ctx, height=500, disabled=True,
                             label_visibility="collapsed", key="schema_ctx")

        # ③ SQL 생성
        gen     = steps.get("generator", {})
        raw     = steps.get("raw", "")
        gen_err = gen.get("error")
        prompt_system = steps.get("prompt_system", "")
        prompt_user   = steps.get("prompt_user", "")
        if gen_err:
            with st.expander("❌ ③ SQL 생성 — 실패", expanded=True):
                st.error(gen_err)
        else:
            with st.expander(f"✅ ③ SQL 생성 — {gen.get('raw_length', 0):,}자 응답", expanded=False):
                tab_prompt, tab_raw = st.tabs(["전체 프롬프트", "LLM 응답"])
                with tab_prompt:
                    st.markdown("**시스템 프롬프트**")
                    st.text_area("", value=prompt_system, height=200, disabled=True,
                                 label_visibility="collapsed", key="sys_prompt")
                    st.markdown("**유저 메시지 (스키마 + 힌트 + 질문)**")
                    st.text_area("", value=prompt_user, height=400, disabled=True,
                                 label_visibility="collapsed", key="user_prompt")
                with tab_raw:
                    st.text_area("", value=raw, height=250, disabled=True,
                                 label_visibility="collapsed", key="raw_resp")

        # ④ 안전성 검사
        safety     = steps.get("safety", {})
        safety_ok  = safety.get("ok", True)
        safety_rsn = safety.get("reason", "")
        if safety_ok:
            with st.expander("✅ ④ 안전성 검사 — 통과", expanded=False):
                st.success("SELECT/WITH 구문 확인, 금지 키워드 없음, 테이블명 검증 통과")
        else:
            with st.expander("❌ ④ 안전성 검사 — 차단", expanded=True):
                st.error(f"차단 사유: {safety_rsn}")

        # ⑤ SQL 실행
        row_count  = exec_r.get("row_count", 0)
        exec_err   = exec_r.get("error")
        exec_cols  = exec_r.get("columns", [])
        if exec_err:
            with st.expander(f"❌ ⑤ SQL 실행 — 오류", expanded=True):
                st.error(exec_err)
        else:
            with st.expander(f"✅ ⑤ SQL 실행 — {row_count}행 · {elapsed:.2f}초", expanded=False):
                st.write(f"**반환 컬럼:** {', '.join(exec_cols) if exec_cols else '없음'}")
                st.write(f"**소요 시간:** {elapsed:.2f}초")
                if exec_r.get("truncated"):
                    st.warning("결과가 100행을 초과하여 잘렸습니다.")

    # ── 결과 (우) ─────────────────────────────────────────
    with right:
        st.subheader("결과")

        if error and not exec_r.get("rows"):
            if "스키마에 없어" in (sql or "") or "스키마에 없어" in (error or ""):
                st.warning("조회 불가 — 해당 데이터는 스키마에 없습니다.")
            else:
                st.error(f"오류: {error}")
        else:
            rows = exec_r.get("rows", [])
            cols = exec_r.get("columns", [])

            # 메트릭 3개
            st.markdown(
                f"결과 **{row_count:,}{'+'if exec_r.get('truncated') else ''}행** &nbsp;·&nbsp; "
                f"소요 **{elapsed:.2f}초** &nbsp;·&nbsp; "
                f"모델 `{model.split('/')[-1]}`",
                unsafe_allow_html=True,
            )

            # 결과 테이블
            if rows:
                df = pd.DataFrame(rows, columns=cols)
                st.dataframe(df, use_container_width=True, height=280)
                csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "CSV 내보내기",
                    data=csv_bytes,
                    file_name="result.csv",
                    mime="text/csv",
                )

                # ── 시각화 ────────────────────────────────
                st.divider()
                import plotly.express as px
                from pipeline import chart_advisor

                viz_col, type_col = st.columns([1, 2])
                with viz_col:
                    st.markdown("**시각화**")
                with type_col:
                    viz_mode = st.radio(
                        "차트 유형",
                        ["숨김", "자동(LLM)", "자동(규칙)", "막대", "꺾은선"],
                        horizontal=True,
                        label_visibility="collapsed",
                        key="viz_mode",
                    )

                if viz_mode != "숨김":
                    if viz_mode == "자동(LLM)":
                        suggestion = chart_advisor.suggest_llm(df)
                    elif viz_mode == "자동(규칙)":
                        suggestion = chart_advisor.suggest_rule(df)
                    elif viz_mode == "막대":
                        suggestion = chart_advisor.suggest_rule(df)
                        suggestion["type"] = "bar"
                    else:  # 꺾은선
                        suggestion = chart_advisor.suggest_rule(df)
                        suggestion["type"] = "line"

                    x      = suggestion.get("x")
                    y_lst  = suggestion.get("y", [])
                    color  = suggestion.get("color")
                    ctype  = suggestion.get("type", "bar")

                    if x and y_lst and ctype != "none":
                        df_plot = df.copy()

                        # year + month → "YYYY-MM" period 컬럼으로 합치기
                        if suggestion.get("combine_period"):
                            df_plot["_period"] = (
                                df_plot["year"].astype(str) + "-"
                                + df_plot["month"].astype(str).str.zfill(2)
                            )
                            x = "_period"

                        y_col = y_lst[0] if len(y_lst) == 1 else y_lst
                        if ctype == "line":
                            fig = px.line(df_plot, x=x, y=y_col,
                                          color=color, markers=True)
                        else:
                            fig = px.bar(df_plot, x=x, y=y_col,
                                         color=color, barmode="group")
                        fig.update_layout(margin=dict(t=20, b=20))
                        st.plotly_chart(fig, use_container_width=True)
                        if suggestion.get("reason"):
                            st.caption(f"💡 {suggestion['reason']}")
                    else:
                        st.info("시각화할 수 있는 숫자 컬럼이 없습니다.")
            else:
                st.info("결과 없음")

        # 생성된 SQL
        if sql:
            with st.expander("생성된 SQL"):
                st.code(sql, language="sql")

if should_run and not effective_question:
    st.warning("질문을 입력해주세요.")
