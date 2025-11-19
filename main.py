import time
import twstock
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.dialects.mysql import insert  
from sqlalchemy import Table, create_engine, text, Column, Integer, String, Float, Date, MetaData, BigInteger

twstock.__update_codes()


# 爬取股票歷史資料
def create_stock_prices_table():
    create_sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stock_no VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open DECIMAL(10,2),
        high DECIMAL(10,2),
        low DECIMAL(10,2),
        close DECIMAL(10,2),
        capacity BIGINT,
        turnover BIGINT,
        `change` DECIMAL(10,2),
        `transaction` BIGINT,
        UNIQUE KEY unique_stock_date (stock_no, date)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(create_sql))


# 查詢該股票最後更新日期
def get_last_date(stock_no):
    sql = text("SELECT MAX(date) FROM stock_prices WHERE stock_no=:stock_no")
    with engine.connect() as conn:
        result = conn.execute(sql, {"stock_no": stock_no}).scalar()
    return result


# 抓 TWSE 歷史資料（全部欄位）
def fetch_twse_history_all(stock_no, start_year=2015, start_month=1, retries=3, delay=2):
    stock = twstock.Stock(stock_no)
    data = None
    
    for attempt in range(retries):
        try:
            data = stock.fetch_from(start_year, start_month)
            if data:
                break
        except Exception as e:
            print(f"{stock_no} 第 {attempt+1} 次抓取失敗: {e}")
            time.sleep(delay)
    
    if not data:
        print(f"{stock_no} 沒有抓到資料")
        return pd.DataFrame()
    
    records = [d._asdict() for d in data]
    df = pd.DataFrame(records)
    df['stock_no'] = stock_no
    df['date'] = pd.to_datetime(df['date'])
    
    cols = ['stock_no', 'date', 'open', 'high', 'low', 'close', 
            'capacity', 'turnover', 'change', 'transaction']
    df = df[cols]
    
    return df


def update_stock_price(stock_no):
    last_date = get_last_date(stock_no)
    if last_date is None:
        start_date = datetime(2015, 1, 1)
        start_year = start_date.year
        start_month = start_date.month
        print(f"{stock_no} 資料庫空，從 2015-01-01 開始抓取資料")
    else:
        start_date = last_date + timedelta(days=1)
        start_year = start_date.year
        start_month = start_date.month
        print(f"{stock_no} 從 {start_date} 開始更新資料")

    df = fetch_twse_history_all(stock_no, start_year, start_month)
    if df.empty:
        print(f"{stock_no} 沒有新資料")
        return

    # 過濾已存在的日期
    if last_date is not None:
        last_date = pd.to_datetime(last_date)
        df = df[df['date'] > last_date]

    if df.empty:
        print(f"{stock_no} 沒有新資料可更新")
        return

    # --- 資料清理與檢查 ---
    # 1. 只保留日期大於最後更新日期的
    if last_date is not None:
        df = df[df["date"] > pd.Timestamp(last_date)]

    # 2. 移除全部價格為 0（例如停牌日）
    df = df[~((df[["open", "high", "low", "close"]] == 0).all(axis=1))]

    # 3. 避免 inf / NaN
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    # 4. 移除 stock_no 或 date 為空的列
    df = df.dropna()

    # 5. 移除重複的 stock_no + date
    df = df.drop_duplicates(subset=["stock_no", "date"])

    # 若清理後沒資料就結束
    if df.empty:
        print(f"{stock_no} 清理後沒有可更新的資料")
        return

    # 建立 upsert
    table = stock_prices  # 你的 SQLAlchemy Table 物件
    upsert_stmt = insert(table).values(df.to_dict(orient='records'))
    update_cols = {c.name: c for c in table.columns if c.name not in ['id', 'stock_no', 'date']}
    upsert_stmt = upsert_stmt.on_duplicate_key_update(**update_cols)

    # 執行
    with engine.begin() as conn:
        conn.execute(upsert_stmt)

    print(f"{stock_no} 更新完成，新增/更新 {len(df)} 筆資料")


if __name__ == '__main__':
    # ======== 🔧 資料庫設定 ========
    DB_USER = "root"
    DB_PASS = "enteryourpassword"
    DB_HOST = "localhost"
    DB_PORT = "3306"
    DB_NAME = "stockdb"

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        echo=False
    )

    # 定義 Table
    metadata = MetaData()

    stock_prices = Table(
        "stock_prices",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_no", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("capacity", BigInteger),
        Column("turnover", BigInteger),
        Column("change", Float),
        Column("transaction", BigInteger),
    )

    # 查詢所有股票代號
    query = "SELECT stock_no FROM stock_codes"
    stock_codes = pd.read_sql(query, engine)

    create_stock_prices_table()

    # 查詢股價 & 儲存在資料庫
    for stock in stock_codes["stock_no"][1100:]:
        update_stock_price(stock)
        time.sleep(3)

