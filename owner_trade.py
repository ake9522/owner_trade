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
WAIT_TIMEOUT = 45     # เวลาสูงสุดที่จะรอตาราง (วินาที)

# สำหรับ Auto-run รายวัน ดึงย้อนหลัง 30-60 วันก็เพียงพอ
# ถ้าต้องการดึงย้อนหลังยาวๆ สามารถเปลี่ยนเป็น 2000 ได้
DAYS_BACK = 60        

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_PATH_DATA = os.path.join(BASE_DIR, "data_owner_trade")
FILE_NAME = "owner_trade.csv"
CSV_PATH = os.path.join(LOCAL_PATH_DATA, FILE_NAME)

def setup_environment():
    os.makedirs(LOCAL_PATH_DATA, exist_ok=True)
    print(f"📁 โฟลเดอร์ข้อมูล: {LOCAL_PATH_DATA}")

def make_driver():
    opts = webdriver.ChromeOptions()
    if HEADLESS_MODE:
        opts.add_argument("--headless=new")
    
    # ⚡ สำคัญ: ไม่ต้องรอ sub-resources โหลดครบ รอแค่ DOM พร้อม ช่วยลด Timeout ได้มาก
    opts.page_load_strategy = "eager"
    
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    
    # ปิดการโหลดรูปภาพเพื่อความเร็ว
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    
    # ป้องกันการตรวจจับ Bot
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=opts)
    # เพิ่ม Page Load Timeout เป็น 180 วินาที
    driver.set_page_load_timeout(180)
    
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"}
    )
    return driver

def parse_owner_trade(html_content, base_url):
    soup = BeautifulSoup(html_content, "html.parser")
    tables = pd.read_html(StringIO(str(soup)))

    if not tables:
        return None

    df = tables[0]
    source_table = soup.find_all("table")[0]
    note_links = []

    for row in source_table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != len(df.columns):
            continue

        link_tag = cells[-1].find("a", href=True)
        if link_tag:
            note_links.append(urljoin(base_url, link_tag["href"].strip()))
        else:
            note_links.append(None)

    if len(note_links) == len(df):
        note_column = next(
            (col for col in df.columns if "หมายเหตุ" in str(col)),
            df.columns[-1]
        )
        df[note_column] = note_links
    else:
        print(f"⚠️ จำนวนลิงก์ ({len(note_links)}) ไม่ตรงกับจำนวนแถว ({len(df)})")

    return df

def save_and_merge_data(new_df):
    """รวมข้อมูลใหม่เข้ากับไฟล์ CSV เดิมที่มีอยู่ในระบบ และตัดแถวที่ซ้ำออก"""
    note_col = next((c for c in new_df.columns if "หมายเหตุ" in str(c)), new_df.columns[-1])
    
    if os.path.exists(CSV_PATH):
        try:
            print("🔄 พบไฟล์ข้อมูลเดิม กำลังนำข้อมูลใหม่ไป Merge...")
            old_df = pd.read_csv(CSV_PATH)
            # รวมข้อมูลเดิม + ข้อมูลใหม่
            combined_df = pd.concat([new_df, old_df], ignore_index=True)
            # ตัดแถวที่ซ้ำ โดยเช็คจากคอลัมน์หมายเหตุ (URL รายการ) หรือทุกคอลัมน์
            combined_df.drop_duplicates(subset=[note_col], keep="first", inplace=True)
            final_df = combined_df
        except Exception as e:
            print(f"⚠️ ไม่สามารถอ่านไฟล์เดิมได้ ({e}) จะบันทึกเฉพาะข้อมูลใหม่")
            final_df = new_df
    else:
        final_df = new_df

    final_df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    
    print("-" * 50)
    print(f"💾 บันทึกสำเร็จ: {CSV_PATH}")
    print(f"📥 ข้อมูลใหม่ที่ดึงมาได้: {len(new_df):,} แถว")
    print(f"📈 รวมข้อมูลทั้งหมดในไฟล์: {len(final_df):,} แถว")
    print(f"🔗 ลิงก์หมายเหตุที่มีอยู่: {final_df[note_col].notna().sum():,} รายการ")
    print("-" * 50)

def fetch_owner_trade():
    setup_environment()

    date_to = datetime.datetime.today().strftime("%Y%m%d")
    date_from = (datetime.datetime.today() - datetime.timedelta(days=DAYS_BACK)).strftime("%Y%m%d")
    url = f"https://market.sec.or.th/public/idisc/th/Viewmore/r59-2?DateType=1&DateFrom={date_from}&DateTo={date_to}"

    driver = None
    try:
        print("🔧 กำลังสร้าง WebDriver...")
        driver = make_driver()

        print(f"🌐 กำลังเปิดเว็บ SEC ({DAYS_BACK} วันย้อนหลัง): {url}")
        driver.get(url)

        print("⏳ กำลังรอตารางโหลด...")
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )

        print("⏳ หน่วงเวลาให้ JavaScript เรนเดอร์ข้อมูล...")
        time.sleep(5)

        page_source = driver.page_source
        if "Block Response" in page_source:
            raise PermissionError("ระบบ SEC บล็อกการเข้าถึง (WAF Block Response)")

        print("📊 กำลังประมวลผลตาราง...")
        df = parse_owner_trade(page_source, driver.current_url)

        if df is not None and not df.empty:
            save_and_merge_data(df)
        else:
            raise ValueError("⚠️ ไม่พบข้อมูลในตาราง")

    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")
        sys.exit(1)
    finally:
        if driver:
            driver.quit()
            print("🔒 ปิดการทำงาน WebDriver เรียบร้อย")

if __name__ == "__main__":
    fetch_owner_trade()
