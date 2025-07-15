import os
import psycopg2
import pandas as pd
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv
import boto3
from io import StringIO

load_dotenv("./database/.env")

class GoldDatabase:
    def __init__(self):
        """
        Khởi tạo kết nối PostgreSQL và tạo bảng nếu chưa tồn tại.
        """
        if os.getenv("USE_POSTGRES") == True:
            self.conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
            self.cur = self.conn.cursor()
            self._create_table()

    def _create_table(self):
        """
        Tạo bảng lưu trữ giá vàng ở dạng bảng rộng.
        """
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_prices (
            id SERIAL PRIMARY KEY,
            source TEXT,
            crawl_time TIMESTAMP,
            buy TEXT,
            sell TEXT,
            name TEXT,
            raw_data JSONB
        )
        """)
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_source_time ON gold_prices(source, crawl_time)")
        self.conn.commit()

    def insert_dataframe(self, df: pd.DataFrame, source: str, crawl_time: Optional[str] = None):
        """
        Lưu DataFrame vào database. Yêu cầu các cột nên gồm name, buy, sell.
        """
        if crawl_time is None:
            crawl_time = datetime.now().isoformat()

        required_cols = {"name", "buy", "sell"}
        lower_cols = set(col.lower() for col in df.columns)
        if not required_cols.issubset(lower_cols):
            print("⚠️ Thiếu cột name, buy, sell. Lưu raw_data thay thế.")
            for _, row in df.iterrows():
                self.cur.execute("""
                    INSERT INTO gold_prices (source, crawl_time, raw_data)
                    VALUES (%s, %s, %s)
                """, (source, crawl_time, row.to_json()))
        else:
            for _, row in df.iterrows():
                self.cur.execute("""
                    INSERT INTO gold_prices (source, crawl_time, name, buy, sell, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    source,
                    crawl_time,
                    row.get("name") or row.get("Name"),
                    row.get("buy") or row.get("Buy"),
                    row.get("sell") or row.get("Sell"),
                    row.to_json()
                ))
        self.conn.commit()

    def query_all(self) -> pd.DataFrame:
        """
        Truy vấn toàn bộ dữ liệu.
        """
        df = pd.read_sql_query("SELECT * FROM gold_prices", self.conn)
        return df

    def query_by_source(self, source: str) -> pd.DataFrame:
        """
        Truy vấn theo nguồn dữ liệu.
        """
        query = "SELECT * FROM gold_prices WHERE source = %s"
        df = pd.read_sql_query(query, self.conn, params=(source,))
        return df

    def query_latest_by_source(self, source: str) -> pd.DataFrame:
        """
        Truy vấn dữ liệu mới nhất theo nguồn.
        """
        self.cur.execute("""
        SELECT MAX(crawl_time) FROM gold_prices WHERE source = %s
        """, (source,))
        latest_time = self.cur.fetchone()[0]
        if latest_time:
            query = """
                SELECT * FROM gold_prices WHERE source = %s AND crawl_time = %s
            """
            df = pd.read_sql_query(query, self.conn, params=(source, latest_time))
            return df
        return pd.DataFrame()

    def export_to_s3(self, df: pd.DataFrame, s3_path: str):
        """
        Xuất DataFrame lên AWS S3 dưới dạng CSV.
        """
        load_dotenv()
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        bucket_name = os.getenv("AWS_BUCKET_NAME")
        region = os.getenv("AWS_REGION")

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_path,
            Body=csv_buffer.getvalue()
        )
        print(f"✅ File uploaded to s3://{bucket_name}/{s3_path}")

    def close(self):
        """Đóng kết nối database."""
        self.cur.close()
        self.conn.close()
