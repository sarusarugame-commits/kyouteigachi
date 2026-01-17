import requests
from bs4 import BeautifulSoup
import time
import re
import unicodedata
import random
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace(" ", "").strip()

def extract_float(text):
    if not text: return 0.0
    match = re.search(r"(\d+\.?\d*)", clean_text(text))
    return float(match.group(1)) if match else 0.0

def get_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def get_soup(session, url):
    try:
        res = session.get(url, timeout=10)
        res.encoding = res.apparent_encoding
        return BeautifulSoup(res.text, 'html.parser') if res.status_code == 200 else None
    except: return None

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    soup_before = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_list = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_before or not soup_list: return None

    row = {'date': date_str, 'jcd': jcd, 'rno': rno}
    try:
        # 風速
        weather = soup_before.select(".weather1_bodyUnitLabelData")
        row['wind'] = next((extract_float(e.text) for e in weather if "m" in e.text and "cm" not in e.text), 0.0)
        
        for i in range(1, 7):
            # 展示・勝率・ST
            row[f'ex{i}'] = extract_float(soup_before.select_one(f".is-boatColor{i}").find_parent("tbody").select("td")[4].text)
            tbody = soup_list.select_one(f".is-boatColor{i}").find_parent("tbody")
            tds = tbody.select("td")
            row[f'wr{i}'] = extract_float(tds[3].text)
            row[f'f{i}'] = int(extract_float(tds[2].text))
            st_match = re.search(r"ST(\d\.\d{2})", clean_text(tbody.text))
            row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17
            row[f'mo{i}'] = extract_float(tds[5].text) or 30.0
    except: return None
    return row

def scrape_result(session, jcd, rno, date_str):
    """
    【新機能】レース結果と二連単の配当金を取得する
    """
    url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup = get_soup(session, url)
    if not soup or "データがありません" in soup.text: return None

    try:
        # 二連単のテーブルを探す
        tables = soup.select(".is-w750 table")
        for table in tables:
            if "二連単" in table.text:
                rows = table.select("tr")
                for r in rows:
                    if "二連単" in r.text:
                        tds = r.select("td")
                        result_combo = clean_text(tds[1].text).replace("-", "-") # 例: "1-3"
                        payout = int(clean_text(tds[2].text).replace("¥", "")) # 例: 450
                        return {"combo": result_combo, "payout": payout}
    except: pass
    return None
