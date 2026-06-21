import sqlite3
import pandas as pd
import plotly.express as px
import os

def main():
    db_path = "data/kepco.db"
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    
    # 산업 대분류(industry_type) 테이블에서 전국('전체') 데이터를 가져옵니다.
    query = """
    SELECT year, month, biz, power_usage
    FROM industry_type
    WHERE metro = '전체'
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()

    # 데이터 전처리: 업종명(biz) 앞뒤 공백 제거 및 정규화
    df['biz'] = df['biz'].str.strip()
    
    # '연-월' 형태의 날짜 컬럼 생성
    df['period'] = df['year'] + '-' + df['month']
    
    # 동일 업종/시점에 대해 합계 계산 (데이터 중복 방지)
    df_agg = df.groupby(['period', 'biz'])['power_usage'].sum().reset_index()
    
    # 시간 순서대로 정렬
    df_agg = df_agg.sort_values('period')

    # Plotly를 이용한 대화형 시계열 그래프 생성
    fig = px.line(
        df_agg, 
        x='period', 
        y='power_usage', 
        color='biz',
        title='2021-2026 연도별/월별 산업별 전력 사용량 추이 (전국)',
        labels={'period': '조회 연월', 'power_usage': '전력 사용량 (kWh)', 'biz': '산업 분류'},
        markers=True
    )

    # 레이아웃 설정 (가로축 촘촘하게 표시 및 범례 클릭 시 선택/해제 기능 강조)
    fig.update_layout(
        xaxis_tickangle=-45,
        hovermode='x unified',
        legend_title_text='산업 분류 (클릭 시 선택/해제)',
        width=1200,
        height=700
    )

    # 출력 디렉토리 확인 및 저장
    output_dir = "analysis/output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_path = os.path.join(output_dir, "industry_usage_interactive.html")
    fig.write_html(output_path)
    
    print(f"그래프 생성이 완료되었습니다: {output_path}")
    print("해당 HTML 파일을 브라우저에서 열어 업종별 선택/해제를 테스트해 보세요.")

if __name__ == "__main__":
    main()
