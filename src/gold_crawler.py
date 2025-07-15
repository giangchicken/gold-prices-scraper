import os
import requests
import pandas as pd
import re
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import pytz

# Load biến môi trường từ .env
load_dotenv("./src/.env")

class GoldPriceAPI(ABC):
    """Lớp cơ sở cho các API giá vàng"""

    def __init__(self, api_name):
        self.api_url = os.getenv(api_name) if os.getenv(api_name) else api_name
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
            ),
            "Content-Type": "application/x-www-form-urlencoded",
        }

        if not self.api_url:
            raise ValueError(f"Không tìm thấy API '{api_name}' trong file .env")

    def fetch_data(self, payload=None):
        """Gửi request và trả về JSON nếu thành công"""
        # payload = {
        #     "method": "GetSJCGoldPriceByDate",
        #     "toDate": date,  # Định dạng dd/mm/yyyy
        # }
        if not payload:
            response = requests.get(self.api_url, headers=self.headers)
        else:
            response = requests.get(self.api_url, headers=self.headers, payload=payload)

        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Lỗi khi gọi API: {response.status_code}")

    @abstractmethod
    def transform(self, json_data):
        """Xử lý dữ liệu và trả về DataFrame"""
        pass


class BTMCAPI(GoldPriceAPI):
    """Lớp xử lý API BTMC"""

    def transform(self, response):
        records = []
        json_data = response.json()
        for item in json_data["DataList"]["Data"]:
            row_data = {}
            for key, value in item.items():
                clean_key = re.sub(r"_\d+$", "", key.lstrip("@"))  # chuẩn hóa tên cột
                row_data[clean_key] = value
            records.append(row_data)

        df = pd.DataFrame(records)
        return df


class SJCAPI(GoldPriceAPI):
    """Lớp xử lý API PNJ"""

    def transform(self, response):
        json_data = response.json()
        df = pd.DataFrame(json_data)
        return df
    
class PNJAPI(GoldPriceAPI):
    """Lớp xử lý API PNJ"""

    def transform(self, response):
        html_content = response.text

        soup = BeautifulSoup(html_content, "html.parser")
        tables = soup.find_all("table")

        # print(f"Tìm thấy {len(tables)} bảng trong file HTML.")
        target_table = tables[0] 
        rows = target_table.find_all("tr")

        data = []
        for row in rows:
            cols = row.find_all(["td", "th"])
            text_values = [col.get_text(strip=True) for col in cols]

            if len(cols) == 5:
                data.append(text_values)
            elif len(cols) == 4:
                data.append([None] + text_values)
            # else:
            #     data.append(cols)

        df = pd.DataFrame(data)

        df.columns = df.iloc[0]
        df = df[1:]

        df.columns = [str(col[0]).strip().lower() if isinstance(col, (list, tuple)) else str(col).strip().lower() for col in df.columns]
        df.columns = [col.replace('<th class="style1">', '').replace('</th>', '').strip().lower() for col in df.columns]
        
        return df

class DOJIAPI(GoldPriceAPI):
    """Lớp xử lý API DOJI"""

    def transform(self, response):
        xml_content = response.content.decode("utf-8-sig")

        root = ET.fromstring(xml_content)

        def extract_rows(parent_tag):
            result = []
            section = root.find(parent_tag)
            if section is not None:
                datetime = section.findtext("DateTime")
                for row in section.findall("Row"):
                    result.append({
                        "Name": row.attrib.get("Name"),
                        "Key": row.attrib.get("Key"),
                        "Sell": row.attrib.get("Sell"),
                        "Buy": row.attrib.get("Buy"),
                        "Time": datetime
                    })
            return result

        data = []
        for tag in ["DGPlist", "JewelryList"]:
            data.extend(extract_rows(tag))

        df = pd.DataFrame(data)
        return df
    
class PhuQuyAPI(GoldPriceAPI):
    """Lớp xử lý API PhuQuy"""

    def transform(self, response):
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        table = soup.find('div', id='priceList').find('table')
        rows = table.find_all('tr')

        headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]

        data = []
        for row in rows[1:]:  # bỏ dòng tiêu đề
            cols = [col.get_text(strip=True) for col in row.find_all('td')]
            if cols:  # tránh dòng trống
                data.append(cols)

        df = pd.DataFrame(data, columns=headers)
        return df

class PNJHistoryAPI(GoldPriceAPI):
    """Lớp xử lý API PNJ lịch sử"""

    def transform(self, response):
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')
        
        tables = soup.find_all("table")
        all_dfs = []
        for i, table in enumerate(tables[1:], start=1):  # Bỏ bảng đầu (giá hiện tại)
            thead = table.find("thead")
            title_cell = thead.find("th") if thead else None
            region = title_cell.get_text(strip=True) if title_cell else f"Unknown_{i}"

            rows = table.find("tbody").find_all("tr")
            data = []
            loai_vang = None
            for row in rows:
                cols = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cols) == 4:
                    data.append(cols)
                    loai_vang = cols[0]
                elif len(cols) == 3 and loai_vang:
                    cols.insert(0, loai_vang)
                    data.append(cols)

            if data and data[0][0].lower() == "loại vàng":
                data = data[1:]

            df = pd.DataFrame(data, columns=["loai_vang", "gia_mua", "gia_ban", "thoi_gian_cap_nhat"])
            df["region"] = region
            all_dfs.append(df)

        if not all_dfs:
            raise ValueError("Không tìm thấy bảng dữ liệu nào.")

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df["gia_mua"] = final_df["gia_mua"].str.replace(".", "", regex=False).astype(int)
        final_df["gia_ban"] = final_df["gia_ban"].str.replace(".", "", regex=False).astype(int)
        final_df["thoi_gian_cap_nhat"] = pd.to_datetime(final_df["thoi_gian_cap_nhat"],
                                                        format="%d/%m/%Y %H:%M:%S")

        return final_df

class WORLD_GOLD_PRICE_API(GoldPriceAPI):
    """Lớp xử lý API PhuQuy"""

    def transform(self, response):
        response_json = response.json()

        # Parse timestamp
        ts_millis = response_json.get("ts")
        dt = datetime.fromtimestamp(ts_millis / 1000) if ts_millis else None

        # Lấy dữ liệu item đầu tiên (USD)
        items = response_json.get("items", [])
        if not items:
            print("❌ Không có dữ liệu items.")
            exit()

        item = items[0]

        # Kết quả dưới dạng dict
        result = {
            "timestamp": ts_millis,
            "datetime": dt.isoformat() if dt else None,
            "currency": item.get("curr", "USD"),
            "xau_price": item.get("xauPrice"),
            "xag_price": item.get("xagPrice"),
            "xau_change": item.get("chgXau"),
            "xag_change": item.get("chgXag"),
            "xau_percent_change": item.get("pcXau"),
            "xag_percent_change": item.get("pcXag"),
            "xau_close": item.get("xauClose"),
            "xag_close": item.get("xagClose")
        }

        # Chuyển sang DataFrame
        df = pd.DataFrame([result])
        return df
    
class WORLD_GOLD_PRICE_HISTORY_API(GoldPriceAPI):
    """Lớp xử lý API PhuQuy"""

    def transform(self, response):
        raw_data = response.json()
        raw_data = raw_data[0].split(",")
        if not raw_data or not isinstance(raw_data, list):
            raise ValueError("Không nhận được dữ liệu đúng định dạng.")

        flat_list = raw_data[1:]  # Bỏ "USD-XAU!"

        if len(flat_list) % 2 != 0:
            raise ValueError("Dữ liệu không chia hết cho 2: thiếu timestamp hoặc giá.")

        timestamps = flat_list[::2]  # timestamp (giờ)
        prices = flat_list[1::2]     # giá trị

        # print(flat_list)
        datetimes = []
        ny_tz = pytz.timezone("America/New_York")
        for ts in timestamps:
            ts_seconds = int(ts) * 100  # convert giờ -> giây
            dt_utc = datetime.utcfromtimestamp(ts_seconds).replace(tzinfo=timezone.utc)
            dt_ny = dt_utc.astimezone(ny_tz)
            datetimes.append(dt_ny.isoformat())

        df = pd.DataFrame({
            "timestamp_hour": timestamps,
            "datetime_ny": datetimes,
            "price_usd_per_oz": prices
        })

        return df

