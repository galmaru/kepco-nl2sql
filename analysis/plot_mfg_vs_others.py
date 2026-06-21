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
    
    # 반도체 거점 도시의 모든 업종 데이터를 가져옵니다.
    query = """
    SELECT year, month, city, biz, power_usage
    FROM industry_type
    WHERE (
        (metro = '경기도' AND city IN ('평택시', '화성시', '용인시', '이천시')) OR
        (metro = '충청북도' AND city = '청주시')
      )
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()

    # 업종 분류: 제조업(Manufacturing) vs 타 업종(Non-Manufacturing)
    df['biz'] = df['biz'].str.strip()
    df['is_mfg'] = df['biz'].apply(lambda x: '제조업' if '제조업' in x else '타 업종')
    
    # '연-월' 형태의 날짜 컬럼 생성
    df['period'] = df['year'] + '-' + df['month']
    
    # 도시/시점/분류별 합산
    df_agg = df.groupby(['period', 'city', 'is_mfg'])['power_usage'].sum().reset_index()
    df_agg = df_agg.sort_values('period')

    # 1. 도시별 제조업 vs 타 업종 추이 비교 (패싯 그리드)
    fig = px.line(
        df_agg, 
        x='period', 
        y='power_usage', 
        color='is_mfg',
        facet_row='city',
        title='반도체 거점 도시 내 제조업 vs 타 업종 전력 사용량 패턴 비교',
        labels={'period': '조회 연월', 'power_usage': '전력 사용량 (kWh)', 'is_mfg': '업종 분류'},
        markers=True
    )

    # 레이아웃 조정 (각 도시별로 Y축 스케일 다르게 설정하여 패턴 강조)
    fig.update_yaxes(matches=None)
    fig.update_layout(xaxis_tickangle=-45, width=1200, height=1200, hovermode='x unified')

    # 출력 디렉토리 확인 및 저장
    output_dir = "analysis/output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    output_path = os.path.join(output_dir, "mfg_vs_others_pattern.html")
    fig.write_html(output_path)
    
    # 2. 전력 사용량 비중 분석 (제조업 비중)
    df_pivot = df_agg.pivot_table(index=['period', 'city'], columns='is_mfg', values='power_usage').reset_index()
    df_pivot['mfg_ratio'] = df_pivot['제조업'] / (df_pivot['제조업'] + df_pivot['타 업종']) * 100
    
    fig_ratio = px.line(
        df_pivot,
        x='period',
        y='mfg_ratio',
        color='city',
        title='반도체 거점 도시별 총 전력 사용량 대비 제조업 비중 추이 (%)',
        labels={'period': '조회 연월', 'mfg_ratio': '제조업 비중 (%)', 'city': '도시명'},
        markers=True
    )
    fig_ratio.update_layout(xaxis_tickangle=-45, width=1200, height=600)
    
    ratio_path = os.path.join(output_dir, "mfg_power_ratio_trend.html")
    fig_ratio.write_html(ratio_path)
    
    print(f"패턴 비교 분석 완료:")
    print(f"1. 제조업 vs 타 업종 시계열: {output_path}")
    print(f"2. 지역별 제조업 비중 추이: {ratio_path}")

if __name__ == "__main__":
    main()
