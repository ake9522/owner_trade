import pandas as pd
import requests
import datetime
import io
import os
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- ส่วนที่ 1: กำหนด Path แบบ Relative (ใช้ได้ทั้งในเครื่องและ GitHub) ---
target_folder = "./data_owner_trade/"

if not os.path.exists(target_folder):
    os.makedirs(target_folder)
    print(f"Created folder: {target_folder}")

# --- ส่วนที่ 2: ตั้งค่าวันที่และ Session ---
date_to = datetime.datetime.today().strftime("%Y%m%d")
# ดึงข้อมูลย้อนหลัง 5000 วันตามเดิม
date_from = (datetime.datetime.today() - datetime.timedelta(days=5000)).strftime("%Y%m%d")

url = f"https://market.sec.or.th/public/idisc/th/Viewmore/r59-2?DateType=1&DateFrom={date_from}&DateTo={date_to}"
print(f"Fetching data from: {url}")

session = requests.Session()
retry_strategy = Retry(
    total=5, # เพิ่มเป็น 5 ครั้งเพื่อความชัวร์บนระบบ Cloud
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://market.sec.or.th/"
}

# --- ส่วนที่ 3: เริ่มการดึงข้อมูล ---
try:
    response = session.get(url, headers=headers, timeout=60)
    response.encoding = 'utf-8-sig'
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        html_io = io.StringIO(str(soup))
        tables = pd.read_html(html_io)
        
        if tables:
            df = tables[0]
            full_path = os.path.join(target_folder, "owner_trade.csv")
            
            # บันทึกไฟล์
            df.to_csv(full_path, index=False, encoding='utf-8-sig')
            print(f"Success! Saved {len(df)} rows to {full_path}")
        else:
            print("No table found on the page.")
    else:
        print(f"Failed to access website. Status Code: {response.status_code}")

except Exception as e:
    print(f"Error occurred: {e}")