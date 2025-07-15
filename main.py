from src.gold_crawler import *  # Các class API crawl
# from database.database import GoldDatabase
from datetime import datetime
import pandas as pd

# Map giữa .env key và class API
apis = {
    "BTMC_DAILY": BTMCAPI,
    "SJC_DAILY": SJCAPI,
    "PNJ_DAILY": PNJAPI,
    "DOJI_DAILY": DOJIAPI,
    "PHU_QUY_DAILY": PhuQuyAPI,
}

def crawl_all_sources() -> dict:
    """
    Crawl tất cả sources, trả về dict: {source_name: dataframe}
    """
    result = {}
    for env_key, api_class in apis.items():
        try:
            print(f"🚀 Crawling: {env_key}")
            api_instance = api_class(env_key)
            response = api_instance.fetch_data()
            df = api_instance.transform(response)

            # Chuẩn hóa cột về lowercase
            # print(df.columns)
            df.columns = [col.lower() for col in df.columns]

            # Thêm metadata
            df["source"] = env_key.lower()
            df["crawl_time"] = datetime.now().isoformat()

            result[env_key] = df

        except Exception as e:
            print(f"❌ Lỗi crawl {env_key}: {str(e)}")
            result[env_key] = None  # Lưu None để biết đã fail
            continue

    return result

def main():
    # db = GoldDatabase()
    crawl_results = crawl_all_sources()

    all_dfs = {}

    for env_key, df in crawl_results.items():
        if df is not None and not df.empty:
            print(f"✔ Data từ {env_key} thu được {len(df)} rows.")
            # print(df)
            all_dfs[env_key] = df

            # # Insert riêng từng nguồn, source đã nằm trong df["source"]
            # db.insert_dataframe(df, source=env_key)
        else:
            print(f"⚠️ Không có dữ liệu hợp lệ từ {env_key} hoặc crawl lỗi.")

    # db.close()

if __name__ == "__main__":
    main()
