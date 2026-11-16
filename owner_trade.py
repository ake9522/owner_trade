import os
import sys
import time
import datetime
from io import StringIO
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==========================================
# CONFIGURATION
# ==========================================
HEADLESS_MODE = True
WAIT_TIMEOUT = 30     # เพิ่มเวลารอสำหรับ GitHub Runner
DAYS_BACK = 2000      # จำนวนวันย้อนหลัง

# ใช้ Relative Path อิงจากตำแหน่งของไฟล์สคริปต์
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_PATH_DATA = os.path.join(BASE_DIR, "data_owner_trade")
FILE_NAME = "owner_trade.csv"
CSV_PATH = os.path.join(LOCAL_PATH_DATA, FILE_NAME)

# ==========================================
# HELPER: ENVIRONMENT SETUP
# ==========================================
def setup_environment():
    """เตรียม Folder สำหรับเก็บข้อมูล"""
    print("🔧 กำลังเตรียมโฟลเดอร์เก็บข้อมูล...")
    os.makedirs(LOCAL_PATH_DATA, exist_ok=True)
    print(f"📁 Path: {LOCAL_PATH_DATA}")

# ==========================================
# HELPER: MAKE DRIVER (รองรับทั้ง Windows และ Linux บน GitHub)
# ==========================================
def make_driver():
    """สร้าง WebDriver instance ที่รองรับทั้งเครื่อง Local และ GitHub Actions Ubuntu"""
    opts = webdriver.ChromeOptions()
    if HEADLESS_MODE:
        opts.add_argument("--headless=new")
    
    # Flags สำคัญสำหรับรันใน Linux / Docker / Cloud Runner
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    
    # หลีกเลี่ยงการถูกตรวจจับว่าเป็น bot
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
    )
    return driver

# ==========================================
# PARSE PAGE CONTENT
# ==========================================
def parse_owner_trade(html_content, base_url):
    """แกะตารางข้อมูล และแทนที่คอลัมน์หมายเหตุด้วย Full URL ของเอกสารจริง"""
    soup = BeautifulSoup(html_content, "html.parser")
    tables = pd.read_html(StringIO(str(soup)))

    if not tables:
        return None

    df = tables[0]

    # ดึงค่า href จากแท็ก <a> ในตาราง HTML
    source_table = soup.find_all("table")[0]
    note_links = []

    for row in source_table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != len(df.columns):
            continue

        # คอลัมน์หมายเหตุจะอยู่ช่องสุดท้ายตามโครงสร้างตารางของ กลต.
        link_tag = cells[-1].find("a", href=True)
        if link_tag:
            note_links.append(urljoin(base_url, link_tag["href"].strip()))
        else:
            note_links.append(None)

    # แทนที่ข้อความ Link ในคอลัมน์หมายเหตุด้วย URL เต็ม
    if len(note_links) == len(df):
        note_column = next(
            (col for col in df.columns if "หมายเหตุ" in str(col)),
            df.columns[-1]
        )
        df[note_column] = note_links
    else:
        print(f"⚠️ เตือน: จำนวนลิงก์หมายเหตุ ({len(note_links)}) ไม่ตรงกับจำนวนแถว ({len(df)})")

    return df

# ==========================================
# MAIN EXECUTION
# ==========================================
def fetch_owner_trade():
    setup_environment()

    date_to = datetime.datetime.today().strftime("%Y%m%d")
    date_from = (datetime.datetime.today() - datetime.timedelta(days=DAYS_BACK)).strftime("%Y%m%d")
    url = f"https://market.sec.or.th/public/idisc/th/Viewmore/r59-2?DateType=1&DateFrom={date_from}&DateTo={date_to}"

    driver = None
    try:
        print("🔧 กำลังสร้าง WebDriver...")
        driver = make_driver()

        print(f"🌐 กำลังเปิดเว็บ SEC: {url}")
        driver.get(url)

        print("⏳ กำลังรอตารางโหลด...")
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )

        print("⏳ กำลังรอ JavaScript เรนเดอร์ข้อมูล...")
        time.sleep(7)

        page_source = driver.page_source
        if "Block Response" in page_source:
            raise PermissionError("ระบบ SEC บล็อกการเข้าถึง (WAF Block Response)")

        print("📊 กำลังประมวลผลตารางและดึงลิงก์หมายเหตุ...")
        df = parse_owner_trade(page_source, driver.current_url)

        if df is not None and not df.empty:
            df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
            note_col = next((c for c in df.columns if "หมายเหตุ" in str(c)), df.columns[-1])

            print("-" * 40)
            print(f"💾 บันทึกไฟล์สำเร็จ: {CSV_PATH}")
            print(f"📈 จำนวนข้อมูล: {len(df):,} แถว")
            print(f"🔗 ลิงก์หมายเหตุ: {df[note_col].notna().sum():,} ลิงก์")
            print("-" * 40)
        else:
            raise ValueError("⚠️ ไม่พบข้อมูลในตาราง")

    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")
        sys.exit(1)  # ส่ง Exit code 1 เพื่อให้ GitHub Actions แจ้งเตือนว่ามี Error
    finally:
        if driver:
            driver.quit()
            print("🔒 ปิดการทำงาน WebDriver เรียบร้อย")

if __name__ == "__main__":
    fetch_owner_trade()
