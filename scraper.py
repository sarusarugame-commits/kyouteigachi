import requests
from bs4 import BeautifulSoup
import time
import re
import unicodedata
import random
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ★バージョン確認用署名★
print("🛠️ LOADED: Scraper Version strict_debug_v2 (Japanese Error Mode)")

# 設定
MAX_RETRIES = 3
RETRY_DELAY = 2
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def get_session():
    session = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retries)
    session.mount("https://", adapter)
    return session

def get_soup(session, url):
    for i in range(MAX_RETRIES):
        try:
            headers = {'User-Agent': random.choice(UA_LIST)}
            res = session.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            if res.status_code == 200:
                if "データがありません" in res.text or "開催中止" in res.text:
                    return None
                return BeautifulSoup(res.text, 'html.parser')
            time.sleep(random.uniform(1, 2))
        except Exception:
            time.sleep(RETRY_DELAY)
    return None

def extract_payout(soup, key_text):
    try:
        tables = soup.select("table")
        for tbl in tables:
            if key_text in tbl.text:
                rows = tbl.select("tr")
                for tr in rows:
                    if key_text in tr.text:
                        tds = tr.select("td")
                        for td in tds:
                            txt = clean_text(td.text)
                            if txt.isdigit() and len(txt) >= 2 and "-" not in txt:
                                return int(txt)
    except: pass
    return 0

def scrape_race_data(session, jcd, rno, date_str):
    """デバッグモード: エラーがあれば即座に例外を発生させる"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # ページ取得
    soup_before = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_before: raise FileNotFoundError(f"【エラー】直前情報ページなし: {jcd}場 {rno}R")

    soup_list = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: raise FileNotFoundError(f"【エラー】出走表ページなし: {jcd}場 {rno}R")

    soup_res = get_soup(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    row = {'date': date_str, 'jcd': jcd, 'rno': rno}

    # ① 風速
    wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
    if wind_elem is None: raise ValueError(f"【エラー】風速データなし (weather1_bodyUnitLabelData)")
    row['wind'] = float(clean_text(wind_elem.text).replace("m", "").strip())

    # ② 各艇データ
    for i in range(1, 7):
        # 展示
        boat_cell = soup_before.select_one(f".is-boatColor{i}")
        if boat_cell is None: raise ValueError(f"【エラー】{i}号艇の展示行なし (.is-boatColor{i})")
        tds = boat_cell.find_parent("tbody").select("td")
        if len(tds) <= 4: raise IndexError(f"【エラー】{i}号艇の展示列不足 len={len(tds)}")
        ex_val = clean_text(tds[4].text)
        if not ex_val: raise ValueError(f"【エラー】{i}号艇の展示タイム空")
        row[f'ex{i}'] = float(ex_val)

        # 番組表
        list_elem = soup_list.select_one(f".is-boatColor{i}")
        if list_elem is None: raise ValueError(f"【エラー】{i}号艇の番組行なし")
        list_tbody = list_elem.find_parent("tbody")
        tds_list = list_tbody.select("td")
        
        # 勝率
        wr_match = re.search(r"(\d\.\d{2})", clean_text(tds_list[3].text))
        if not wr_match: raise ValueError(f"【エラー】{i}号艇の勝率なし")
        row[f'wr{i}'] = float(wr_match.group(1))
        
        f_match = re.search(r"F(\d+)", clean_text(tds_list[2].text))
        row[f'f{i}'] = int(f_match.group(1)) if f_match else 0
        
        st_match = re.search(r"ST(\d\.\d{2})", list_tbody.text.replace("\n", "").replace(" ", ""))
        if not st_match: raise ValueError(f"【エラー】{i}号艇のSTなし")
        row[f'st{i}'] = float(st_match.group(1))
        
        mo_text = clean_text(tds_list[5].text)
        mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
        if not mo_match and len(tds_list) > 6:
            mo_text = clean_text(tds_list[6].text)
            mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
        if not mo_match: raise ValueError(f"【エラー】{i}号艇のモーターなし")
        row[f'mo{i}'] = float(mo_match.group(1))

    # ③ オッズ（予測時は0でOK）
    if soup_res:
        row['tansho'] = extract_payout(soup_res, "単勝")
        row['nirentan'] = extract_payout(soup_res, "2連単")
        row['sanrentan'] = extract_payout(soup_res, "3連単")
    else:
        row['tansho'] = 0
        row['nirentan'] = 0
        row['sanrentan'] = 0

    return row
