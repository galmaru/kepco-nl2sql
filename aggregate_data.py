import sqlite3
import pandas as pd

DB_PATH = "data/kepco.db"

def aggregate_data():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 계약종별 전력사용량 집계 (연월별 전국 합계)
    print("Aggregating contract_type...")
    df_contract = pd.read_sql("""
        SELECT year, month, contract_type, 
               SUM(cust_count) AS total_cust, 
               SUM(power_usage) AS total_usage, 
               SUM(bill) AS total_bill
        FROM contract_type
        WHERE metro != '전체'
        GROUP BY year, month, contract_type
    """, conn)
    df_contract.to_sql("agg_contract_type_monthly", conn, if_exists="replace", index=False)
    
    # 2. 업종별 전력사용량 집계 (연월별 전국 합계)
    print("Aggregating business_type...")
    df_business = pd.read_sql("""
        SELECT year, month, biz_type, 
               SUM(cust_count) AS total_cust, 
               SUM(power_usage) AS total_usage
        FROM business_type
        GROUP BY year, month, biz_type
    """, conn)
    df_business.to_sql("agg_business_type_monthly", conn, if_exists="replace", index=False)
    
    # 3. 산업분류별 전력사용량 집계 (연월별 전국 합계)
    print("Aggregating industry_type...")
    df_industry = pd.read_sql("""
        SELECT year, month, biz, 
               SUM(cust_count) AS total_cust, 
               SUM(power_usage) AS total_usage,
               SUM(bill) AS total_bill
        FROM industry_type
        WHERE metro != '전체'
        GROUP BY year, month, biz
    """, conn)
    df_industry.to_sql("agg_industry_type_monthly", conn, if_exists="replace", index=False)
    
    # 4. 복지할인 현황 집계
    print("Aggregating welfare_discount...")
    df_welfare = pd.read_sql("""
        SELECT year, month, welfare_type, 
               SUM(welfare_count) AS total_count
        FROM welfare_discount
        GROUP BY year, month, welfare_type
    """, conn)
    df_welfare.to_sql("agg_welfare_discount_monthly", conn, if_exists="replace", index=False)
    
    # 5. 청구방식 변동 집계
    print("Aggregating billing_type...")
    df_billing = pd.read_sql("""
        SELECT year, month, bill_type, 
               SUM(bill_count) AS total_count
        FROM billing_type
        GROUP BY year, month, bill_type
    """, conn)
    df_billing.to_sql("agg_billing_type_monthly", conn, if_exists="replace", index=False)

    # 6. 신재생 에너지 현황 집계 (연도별)
    print("Aggregating renew_energy...")
    df_renew = pd.read_sql("""
        SELECT year, gen_source, 
               SUM(count) AS total_plants,
               SUM(capacity) AS total_capacity
        FROM renew_energy
        GROUP BY year, gen_source
    """, conn)
    df_renew.to_sql("agg_renew_energy_yearly", conn, if_exists="replace", index=False)

    conn.close()
    print("Aggregation completed and stored in 'agg_*' tables.")

if __name__ == "__main__":
    aggregate_data()
