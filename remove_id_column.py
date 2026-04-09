import sqlite3

DB_PATH = "data/kepco.db"

def drop_id_column():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # 테이블 목록 (sqlite_sequence 등 시스템 테이블 제외)
    tables = [
        "billing_type", "business_type", "contract", "contract_type",
        "industry_cust_change", "industry_type", "dispersed_gen",
        "ev_charge", "ev_charge_manage", "house_avg",
        "renew_energy", "welfare_discount", "common_code"
    ]
    
    for table in tables:
        print(f"Processing table: {table}")
        
        # 1. 원본 테이블의 컬럼 정보 가져오기 (id 제외)
        cur.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cur.fetchall() if row[1] != 'id']
        col_string = ", ".join(columns)
        
        # 2. 기존 테이블 이름 변경
        cur.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
        
        # 3. id 컬럼이 제거된 새로운 테이블 생성 (기존 DDL 기반으로 생성하는 대신 단순화된 DDL 사용)
        # build_db.py의 DDL 구조를 참고하여 'id'만 빼고 생성
        if table == "billing_type":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, metro TEXT, city TEXT, bill_type TEXT, bill_count INTEGER)")
        elif table == "business_type":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, metro TEXT, city TEXT, biz_type TEXT, cust_count INTEGER, power_usage REAL, contract_power REAL)")
        elif table == "contract":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, purchase_type TEXT, company_id TEXT, notice_no TEXT, notice_name TEXT, presumed_price REAL, notice_date TEXT, bid_begin_datetime TEXT, bid_end_datetime TEXT, create_datetime TEXT, competition_type TEXT, vendor_award_type TEXT, bid_type TEXT, place_name TEXT, contract_req_dept TEXT, presumed_amount REAL, item_type TEXT, progress_state TEXT, delivery_location TEXT, delivery_due_date TEXT, emergency_notice_yn TEXT)")
        elif table == "contract_type":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, metro TEXT, city TEXT, contract_type TEXT, cust_count INTEGER, power_usage REAL, bill REAL, unit_cost REAL, contract_power REAL)")
        elif table == "industry_cust_change":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, metro TEXT, city TEXT, biz TEXT, new_count INTEGER, expansion_count INTEGER, cancel_count INTEGER)")
        elif table == "industry_type":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, metro TEXT, city TEXT, biz TEXT, cust_count INTEGER, power_usage REAL, bill REAL, unit_cost REAL)")
        elif table == "dispersed_gen":
            cur.execute(f"CREATE TABLE {table} (subst_code TEXT, subst_name TEXT, js_subst_power REAL, subst_power REAL, meter_no TEXT, js_meter_power REAL, meter_power REAL, dl_code TEXT, dl_name TEXT, js_dl_power REAL, dl_power REAL, vol1 REAL, vol2 REAL, vol3 REAL)")
        elif table == "ev_charge":
            cur.execute(f"CREATE TABLE {table} (metro TEXT, city TEXT, station_place TEXT, station_addr TEXT, rapid_count INTEGER, slow_count INTEGER, car_type TEXT)")
        elif table == "ev_charge_manage":
            cur.execute(f"CREATE TABLE {table} (metro TEXT, station_id TEXT, station_name TEXT, charger_id TEXT, charger_name TEXT, charger_type TEXT, charge_type TEXT, charger_status TEXT, addr TEXT, lat TEXT, lng TEXT, status_updated_at TEXT)")
        elif table == "house_avg":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, metro TEXT, city TEXT, house_count INTEGER, power_usage REAL, bill REAL)")
        elif table == "renew_energy":
            cur.execute(f"CREATE TABLE {table} (year TEXT, gen_source TEXT, metro TEXT, city TEXT, count INTEGER, capacity REAL, area_count INTEGER, area_capacity REAL)")
        elif table == "welfare_discount":
            cur.execute(f"CREATE TABLE {table} (year TEXT, month TEXT, metro TEXT, city TEXT, welfare_type TEXT, welfare_count INTEGER)")
        elif table == "common_code":
            cur.execute(f"CREATE TABLE {table} (code_type TEXT, upper_code TEXT, upper_code_name TEXT, code TEXT, code_name TEXT)")
            
        # 4. 데이터 복사
        cur.execute(f"INSERT INTO {table} ({col_string}) SELECT {col_string} FROM {table}_old")
        
        # 5. 옛날 테이블 삭제
        cur.execute(f"DROP TABLE {table}_old")
        
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    print("All 'id' columns removed successfully.")

if __name__ == "__main__":
    drop_id_column()
