import pandas as pd
import requests
import datetime
import io
import os
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin

# --- ส่วนที่ 1: กำหนด Path แบบ Relative ---
# ใช้ได้ทั้ง Local และ GitHub Actions
target_folder = "./data_owner_trade/"

if not os.path.exists(target_folder):
    os.makedirs(target_folder)
    print(f"Created folder: {target_folder}")

# --- ส่วนที่ 2: ตั้งค่าวันที่ ---
date_to = datetime.datetime.today().strftime("%Y%m%d")

# ดึงข้อมูลย้อนหลัง 2000 วัน
date_from = (
    datetime.datetime.today() - datetime.timedelta(days=2000)
).strftime("%Y%m%d")

url = (
    "https://market.sec.or.th/public/idisc/th/Viewmore/r59-2"
    f"?DateType=1&DateFrom={date_from}&DateTo={date_to}"
)

print(f"Fetching data from: {url}")

# --- ตั้งค่า Session และ Retry ---
session = requests.Session()

retry_strategy = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

adapter = HTTPAdapter(max_retries=retry_strategy)

session.mount("https://", adapter)
session.mount("http://", adapter)

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://market.sec.or.th/",
    "Connection": "keep-alive",
}

# --- ส่วนที่ 3: เริ่มดึงข้อมูล ---
try:
    response = session.get(
        url,
        headers=headers,
        timeout=60,
    )

    response.raise_for_status()
    response.encoding = "utf-8-sig"

    soup = BeautifulSoup(response.text, "html.parser")

    # ค้นหาตารางทั้งหมดในหน้า
    html_io = io.StringIO(str(soup))
    tables = pd.read_html(html_io)

    if not tables:
        raise ValueError("No table found on the page.")

    # ตารางข้อมูลหลัก
    df = tables[0]

    # =========================================================
    # ดึง URL จากคอลัมน์หมายเหตุ
    # =========================================================

    # ค้นหาตำแหน่งคอลัมน์หมายเหตุจากชื่อคอลัมน์
    note_column_index = next(
        (
            index
            for index, column in enumerate(df.columns)
            if "หมายเหตุ" in str(column)
        ),
        None,
    )

    if note_column_index is None:
        raise ValueError("ไม่พบคอลัมน์หมายเหตุในตาราง SEC")

    note_column = df.columns[note_column_index]

    # เลือกตาราง HTML ที่ตรงกับตารางซึ่ง pandas อ่าน
    html_tables = soup.find_all("table")

    if not html_tables:
        raise ValueError("ไม่พบ HTML table ในหน้า SEC")

    source_table = html_tables[0]
    note_links = []

    # วนอ่านแต่ละแถวของตาราง HTML
    for row in source_table.find_all("tr"):
        cells = row.find_all("td")

        # ข้ามหัวตารางและแถวที่โครงสร้างไม่ตรง
        if len(cells) != len(df.columns):
            continue

        # ดึง <a href="..."> จากคอลัมน์หมายเหตุ
        link_tag = cells[note_column_index].find("a", href=True)

        if link_tag:
            href = link_tag["href"].strip()

            # แปลง Relative URL ให้เป็น Absolute URL
            full_url = urljoin(response.url, href)
            note_links.append(full_url)
        else:
            note_links.append(None)

    # ตรวจสอบว่าจำนวน URL ตรงกับจำนวนข้อมูล
    if len(note_links) != len(df):
        raise ValueError(
            f"จำนวนแถว URL ({len(note_links)}) "
            f"ไม่ตรงกับจำนวนข้อมูล ({len(df)})"
        )

    # แทนข้อความ Link ด้วย URL จริง
    df[note_column] = note_links

    # --- ส่วนที่ 4: บันทึก CSV ---
    full_path = os.path.join(
        target_folder,
        "owner_trade.csv",
    )

    df.to_csv(
        full_path,
        index=False,
        encoding="utf-8-sig",
    )

    print("-" * 50)
    print("Success!")
    print(f"Saved file: {full_path}")
    print(f"Total rows: {len(df)}")
    print(
        f"Total note links: "
        f"{df[note_column].notna().sum()}"
    )

except requests.exceptions.RequestException as error:
    print(f"Request error: {error}")
    raise

except Exception as error:
    print(f"Error occurred: {error}")
    raise
