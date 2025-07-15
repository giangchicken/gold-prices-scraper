import requests
import json
import pandas as pd
import re
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import pytz


# URL nguồn giá vàng
url = "https://giavang.pnj.com.vn/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
}

# Gửi request
response = requests.get(url, headers=headers)
# raw_data = response.json()
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

df = pd.DataFrame(data)

df.columns = df.iloc[0]
df = df[1:]

df.columns = [str(col[0]).strip().lower() if isinstance(col, (list, tuple)) else str(col).strip().lower() for col in df.columns]
df.columns = [col.replace('<th class="style1">', '').replace('</th>', '').strip().lower() for col in df.columns]

print(df)