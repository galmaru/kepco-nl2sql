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
    
    # 삼성전자(평택, 화성, 용인)와 SK하이닉스(이천, 청주) 거점의 제조업 전력 사용량 추출
    # biz 항목의 공백 문제를 해결하기 위해 LIKE 연산자 사용
    query = """
    SELECT year, month, metro, city, SUM(power_usage) AS power_usage
    FROM industry_type
    WHERE (biz LIKE '%제조업%' OR biz LIKE '%제조업 ')
      AND (
        (metro = '경기도' AND city IN ('평택시', '화성시', '용인시', '이천시')) OR
        (metro = '충청북도' AND city = '청주시')
      )
    GROUP BY year, month, metro, city
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()

    # '연-월' 형태의 날짜 컬럼 생성
    df['period'] = df['year'] + '-' + df['month']
    df = df.sort_values('period')

    # 기업별 매핑 (분석 편의상)
    def categorize_company(city):
        if city in ['이천시', '청주시']:
            return 'SK하이닉스 거점'
        elif city in ['평택시', '화성시', '용인시']:
            return '삼성전자 거점'
        return '기타'

    df['company_focus'] = df['city'].apply(categorize_company)

    # 1. 도시별 세부 시계열 그래프
    fig_city = px.line(
        df, 
        x='period', 
        y='power_usage', 
        color='city',
        facet_col='company_focus',
        title='반도체 거점 도시별 제조업 전력 사용량 추이 (2021-2026)',
        labels={'period': '조회 연월', 'power_usage': '전력 사용량 (kWh)', 'city': '도시명'},
        markers=True
    )
    
    fig_city.update_layout(xaxis_tickangle=-45, width=1400, height=700)
    
    # 2. 기업별 합산 시계열 그래프 (패턴 비교)
    df_agg = df.groupby(['period', 'company_focus'])['power_usage'].sum().reset_index()
    fig_comp = px.line(
        df_agg, 
        x='period', 
        y='power_usage', 
        color='company_focus',
        title='삼성전자 vs SK하이닉스 거점 도시 전력 사용량 합계 비교',
        labels={'period': '조회 연월', 'power_usage': '총 전력 사용량 (kWh)', 'company_focus': '분류'},
        markers=True
    )
    fig_comp.update_layout(xaxis_tickangle=-45, width=1200, height=600)

    # 출력 디렉토리 확인 및 저장
    output_dir = "analysis/output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    path_city = os.path.join(output_dir, "semiconductor_hubs_trend.html")
    path_comp = os.path.join(output_dir, "semiconductor_company_trend.html")
    
    fig_city.write_html(path_city)
    fig_comp.write_html(path_comp)
    
    print(f"분석 그래프 생성이 완료되었습니다:")
    print(f"1. 도시별 상세: {path_city}")
    print(f"2. 기업별 합계 비교: {path_comp}")

if __name__ == "__main__":
    main()
