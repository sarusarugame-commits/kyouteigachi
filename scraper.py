from curl_cffi import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import re
import unicodedata
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def get_session():
    # Chrome 120 偽装
    return requests.Session(impersonate="chrome120")

def get_soup(session, url):
    try:
        res = session.get(url, timeout=15)
        if res.status_code != 200: return None, f"Status {res.status_code}"
        if len(res.content) < 5000: return None, "BLOCKED (Small Size)"
        if "データがありません" in res.text: return None, "NO_DATA"
        return BeautifulSoup(res.content, 'lxml'), None
    except Exception as e:
        return None, str(e)

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 3ページ取得
    soup_before, err_b = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    soup_list, err_l = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    
    if err_b: return None, f"BeforeInfoエラー: {err_b}"
    if err_l: return None, f"RaceListエラー: {err_l}"
    if not soup_before or not soup_list: return None, "スープ取得失敗"

    row = {'date': date_str, 'jcd': jcd, 'rno': rno, 'wind': 0.0, 'deadline_time': "23:59"}

    # --- 天候・風 ---
    try:
        wind_elem = soup_before.select_one(".weather1_bodyUnitLabelData")
        if wind_elem:
            txt = clean_text(wind_elem.text).replace("m", "").replace(" ", "")
            row['wind'] = float(txt) if txt else 0.0
    except: pass

    # --- 締切時刻 ---
    try:
        target = soup_list.find(lambda t: t.name in ['th','td'] and "締切予定時刻" in t.text)
        if target:
            tr = target.find_parent("tr")
            cells = tr.find_all(['th','td'])
            if len(cells) > rno:
                m = re.search(r"(\d{1,2}:\d{2})", clean_text(cells[rno].text))
                if m: row['deadline_time'] = m.group(1)
    except: pass

    # --- 各艇データ ---
    for i in range(1, 7):
        # 初期値
        row[f'wr{i}'], row[f'mo{i}'], row[f'ex{i}'] = 0.0, 30.0, 6.80
        row[f'f{i}'], row[f'st{i}'] = 0, 0.20

        # 展示タイム
        try:
            boat_cell = soup_before.select_one(f".is-boatColor{i}")
            if boat_cell:
                tds = boat_cell.find_parent("tbody").select("td")
                if len(tds) > 4:
                    ex_val = clean_text(tds[4].text)
                    if re.match(r"\d\.\d{2}", ex_val):
                        row[f'ex{i}'] = float(ex_val)
        except: pass

        # 本番データ
        try:
            list_cell = soup_list.select_one(f".is-boatColor{i}")
            if list_cell:
                tds = list_cell.find_parent("tbody").select("td")
                
                # F数 / ST
                if len(tds) > 3:
                    txt = clean_text(tds[3].text)
                    f_match = re.search(r"F(\d+)", txt)
                    if f_match: row[f'f{i}'] = int(f_match.group(1))
                    st_match = re.search(r"(\.\d{2}|\d\.\d{2})", txt)
                    if st_match:
                        val = float(st_match.group(1))
                        if val < 1.0: row[f'st{i}'] = val
                
                # 勝率
                if len(tds) > 4:
                    txt = tds[4].get_text(" ").strip()
                    wr_match = re.search(r"(\d\.\d{2})", txt)
                    if wr_match: row[f'wr{i}'] = float(wr_match.group(1))

                # モーター
                if len(tds) > 6:
                    txt = tds[6].get_text(" ").strip()
                    mo_vals = re.findall(r"(\d{1,3}\.\d{2})", txt)
                    if len(mo_vals) >= 1: row[f'mo{i}'] = float(mo_vals[0])
        except: pass

    return row, None

def scrape_result(session, jcd, rno, date_str):
    url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup, err = get_soup(session, url)
    if not soup: return None

    # 結果格納用（3連単対応）
    res = {
        "tansho_no": None,       "tansho_payout": 0,
        "nirentan_combo": None,  "nirentan_payout": 0,
        "sanrentan_combo": None, "sanrentan_payout": 0
    }
    
    try:
        # 3連単、2連単、単勝を探す
        for tbl in soup.select("table"):
            txt = tbl.text
            if "3連単" in txt or "2連単" in txt or "単勝" in txt:
                for tr in tbl.select("tr"):
                    row_txt = clean_text(tr.text)
                    
                    if "3連単" in row_txt:
                        nums = tr.select(".numberSet1_number")
                        if len(nums) >= 3:
                            res["sanrentan_combo"] = f"{nums[0].text}-{nums[1].text}-{nums[2].text}"
                        pay = tr.select_one(".is-payout1")
                        if pay:
                            p = clean_text(pay.text).replace("¥","").replace(",","")
                            if p.isdigit(): res["sanrentan_payout"] = int(p)

                    elif "2連単" in row_txt:
                        nums = tr.select(".numberSet1_number")
                        if len(nums) >= 2:
                            res["nirentan_combo"] = f"{nums[0].text}-{nums[1].text}"
                        pay = tr.select_one(".is-payout1")
                        if pay:
                            p = clean_text(pay.text).replace("¥","").replace(",","")
                            if p.isdigit(): res["nirentan_payout"] = int(p)
                            
                    elif "単勝" in row_txt:
                        nums = tr.select(".numberSet1_number")
                        if len(nums) >= 1:
                            res["tansho_no"] = nums[0].text

    except: pass
    return res

def scrape_odds(session, jcd, rno, date_str, target_boat=None, target_combo=None):
    return {"tansho": "1.0", "nirentan": "1.0"}
