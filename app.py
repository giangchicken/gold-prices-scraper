# file: api/main.py
from fastapi import FastAPI
from datetime import datetime, timezone
import pytz
import pandas as pd
from src.gold_crawler import *  # Các class API crawl
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Query
import json

app = FastAPI(title="Gold Price Crawler API")

# Map giữa .env key và class API
apis = {
    "BTMC_DAILY": BTMCAPI,
    "SJC_DAILY": SJCAPI,
    "PNJ_DAILY": PNJAPI,
    "DOJI_DAILY": DOJIAPI,
    "PHU_QUY_DAILY": PhuQuyAPI,
    "WORLD_GOLD_PRICE": WORLD_GOLD_PRICE_API
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

@app.get("/crawl-all-daily")
def crawl_all():
    """
    Gọi crawl tất cả sources, trả về kết quả dạng JSON.
    """
    crawl_results = crawl_all_sources()
    output = {}

    for env_key, df in crawl_results.items():
        if df is not None and not df.empty:
            # Ép kiểu tránh numpy types
            df_safe = df.copy()
            df_safe.columns = [str(col) for col in df_safe.columns]
            # df là DataFrame kết quả từ crawl
            df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)  # convert NaN thành None

            # Chuyển datetime thành string
            for col in df_safe.select_dtypes(include=["datetime64[ns]"]).columns:
                df_safe[col] = df_safe[col].astype(str)

            # Chuyển toàn bộ dataframe về string (tạm thời để tránh lỗi)
            df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)
            df_safe = df_safe.applymap(lambda x: str(x) if not isinstance(x, (int, float, bool, type(None))) else x)


            output[env_key] = {
                "status": "success",
                "row_count": len(df_safe),
                "data": df_safe.to_dict(orient="records"),
            }
        elif df is None:
            output[env_key] = {"status": "error", "message": "Exception during crawl"}
        else:
            output[env_key] = {"status": "empty", "message": "No valid data"}

    return output

@app.get("/crawl-pnj-history")
def crawl_pnj_history(day: str,
                      month: str,
                      year: str):
    try:
        url = f"https://giavang.pnj.com.vn/history?gold_history_day={day}&gold_history_month={month}&gold_history_year={year}"

        api_instance = PNJHistoryAPI(url)
        response = api_instance.fetch_data()
        df = api_instance.transform(response)

        df_safe = df.copy()
        df_safe["source"] = "pnj_history"
        df_safe["crawl_time"] = datetime.now().isoformat()

        df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)  # convert NaN thành None

        # Chuyển datetime thành string
        for col in df_safe.select_dtypes(include=["datetime64[ns]"]).columns:
            df_safe[col] = df_safe[col].astype(str)

        # Chuyển toàn bộ dataframe về string (tạm thời để tránh lỗi)
        df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)
        df_safe = df_safe.applymap(lambda x: str(x) if not isinstance(x, (int, float, bool, type(None))) else x)

        return {
            "success": True,
            "total_rows": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/crawl-phuquy-history")
def crawl_pnj_history(date: str):
    try:
        url = f"https://phuquygroup.vn/Gold/GoldPriceLast?date={date}"

        api_instance = PhuQuyAPI(url)
        response = api_instance.fetch_data()
        df = api_instance.transform(response)

        df_safe = df.copy()
        df_safe["source"] = "pnj_history"
        df_safe["crawl_time"] = datetime.now().isoformat()

        df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)  # convert NaN thành None

        # Chuyển datetime thành string
        for col in df_safe.select_dtypes(include=["datetime64[ns]"]).columns:
            df_safe[col] = df_safe[col].astype(str)

        # Chuyển toàn bộ dataframe về string (tạm thời để tránh lỗi)
        df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)
        df_safe = df_safe.applymap(lambda x: str(x) if not isinstance(x, (int, float, bool, type(None))) else x)


        return {
            "success": True,
            "total_rows": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
@app.get("/goldprice-world/history")
def get_world_gold_price_history():

    try:
        # url = "https://data-asg.goldprice.org/GetDataHistorical/USD-XAU/0"
        url = os.getenv("WORLD_GOLD_PRICE_HIS")

        api_instance = WORLD_GOLD_PRICE_HISTORY_API(url)
        response = api_instance.fetch_data()
        df = api_instance.transform(response)

        df_safe = df.copy()
        df_safe["source"] = "pnj_history"
        df_safe["crawl_time"] = datetime.now().isoformat()

        df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)  # convert NaN thành None

        # Chuyển datetime thành string
        for col in df_safe.select_dtypes(include=["datetime64[ns]"]).columns:
            df_safe[col] = df_safe[col].astype(str)

        # Chuyển toàn bộ dataframe về string (tạm thời để tránh lỗi)
        df_safe = df_safe.astype(object).where(pd.notnull(df_safe), None)
        df_safe = df_safe.applymap(lambda x: str(x) if not isinstance(x, (int, float, bool, type(None))) else x)

        return {
            "success": True,
            "total_rows": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})