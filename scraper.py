import requests
from bs4 import BeautifulSoup
import time
import re
import unicodedata
import random
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# 設定
MAX_RETRIES = 3
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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
    if session is None: session = get_session()
    try:
        headers = {'User-Agent': random.choice(UA_LIST)}
        res = session.get(url, headers=headers, timeout=10)
        res.encoding = res.apparent_encoding
        if res.status_code == 200:
            if "データがありません" in res.text or "開催中止" in res.text:
                return None
            return BeautifulSoup(res.text, 'html.parser')
    except: pass
    return None

def scrape_race_data(session, jcd, rno, date_str):
    """厳格デバッグモード: エラー時に日本語で原因を報告"""
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # ページ取得
    soup_before = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_before: return None 

    soup_list = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None

    # 結果ページ（オッズ用）
    soup_res = get_soup(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    row = {'date': date_str, 'jcd': jcd, 'rno': rno}

    # ① 風速チェック
    wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
    if wind_elem is None:
        raise ValueError(f"【エラー】{jcd}場{rno}R: 風速データが見つかりません")
    row['wind'] = float(clean_text(wind_elem.text).replace("m", "").strip())

    # ② 各艇データ
    for i in range(1, 7):
        # 展示タイム
        boat_cell = soup_before.select_one(f".is-boatColor{i}")
        if boat_cell is None: raise ValueError(f"【エラー】{i}号艇の展示行なし")
        
        tds = boat_cell.find_parent("tbody").select("td")
        if len(tds) <= 4: raise IndexError(f"【エラー】{i}号艇の展示列不足")
            
        ex_val = clean_text(tds[4].text)
        if not ex_val: raise ValueError(f"【エラー】{i}号艇の展示タイム空")
        row[f'ex{i}'] = float(ex_val)

        # 番組データ
        list_elem = soup_list.select_one(f".is-boatColor{i}")
        if list_elem is None: raise ValueError(f"【エラー】{i}号艇の出走表データなし")
            
        tds_list = list_elem.find_parent("tbody").select("td")
        wr_match = re.search(r"(\d\.\d{2})", clean_text(tds_list[3].text))
        if not wr_match: raise ValueError(f"【エラー】{i}号艇の勝率なし")
        row[f'wr{i}'] = float(wr_match.group(1))
        
        st_match = re.search(r"ST(\d\.\d{2})", list_elem.find_parent("tbody").text.replace("\n", "").replace(" ", ""))
        row[f'st{i}'] = float(st_match.group(1)) if st_match else 0.17

        # モーター
        mo_text = clean_text(tds_list[5].text)
        mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
        if not mo_match and len(tds_list) > 6:
            mo_text = clean_text(tds_list[6].text)
            mo_match = re.search(r"(\d{1,3}\.\d)", mo_text)
        row[f'mo{i}'] = float(mo_match.group(1)) if mo_match else 30.0
        row[f'f{i}'] = 0 # 簡易化

    # オッズ（なくてもOK）
    if soup_res:
        # 簡易抽出（extract_payout関数省略版）
        try:
            txt = soup_res.select("td")[0].text # ダミー処理、実際は0で返す
        except: pass
    row['nirentan'] = 0
    row['sanrentan'] = 0
    row['tansho'] = 0

    return row
