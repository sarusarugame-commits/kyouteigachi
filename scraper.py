from curl_cffi import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import re
import unicodedata
import warnings

# ログ汚染対策
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

def clean_text(text):
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", " ").replace("\r", "").strip()

def get_session():
    # Chrome 120 の指紋を模倣（これで5KBブロックを突破）
    return requests.Session(impersonate="chrome120")

def get_soup(session, url):
    try:
        res = session.get(url, timeout=15)
        if res.status_code != 200:
            return None
        return BeautifulSoup(res.content, 'lxml')
    except:
        return None

def extract_win_rate(text):
    matches = re.findall(r"(\d\.\d{2})", text)
    for m in matches:
        val = float(m)
        if 1.5 <= val <= 9.99: return val
    return 0.0

def scrape_race_data(session, jcd, rno, date_str):
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    url_before = f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    url_list = f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}"

    soup_before = get_soup(session, url_before)
    soup_list = get_soup(session, url_list)
    
    if not soup_before or not soup_list: return None
    if "データがありません" in soup_before.text: return None

    row = {'date': date_str, 'jcd': jcd, 'rno': rno}
    row['deadline_time'] = "23:59"

    # 締切時刻
    try:
        target = soup_list.find(lambda t: t.name in ['th','td'] and "締切予定時刻" in t.text)
        if target:
            cells = target.find_parent("tr").find_all(['th','td'])
            if len(cells) > rno:
                m = re.search(r"(\d{1,2}:\d{2})", clean_text(cells[rno].text))
                if m: row['deadline_time'] = m.group(1)
    except: pass

    # 風速
    try:
        w_node = soup_before.select_one(".weather1_bodyUnitLabelData")
        m = re.search(r"(\d+)m", clean_text(w_node.text)) if w_node else None
        row['wind'] = float(m.group(1)) if m else 0.0
    except: row['wind'] = 0.0

    # 各艇データ
    for i in range(1, 7):
        try:
            # 展示タイム
            node_b = soup_before.select_one(f"td.is-boatColor{i}")
            tr_b = node_b.find_parent("tr")
            ex_val = clean_text(tr_b.select("td")[4].text)
            row[f'ex{i}'] = float(re.search(r"(\d\.\d{2})", ex_val).group(1))
        except: row[f'ex{i}'] = 6.80

        try:
            # 勝率・ST・モーター
            node_l = soup_list.select_one(f"td.is-boatColor{i}")
            tbody = node_l.find_parent("tbody")
            full_txt = clean_text(tbody.text)
            
            row[f'wr{i}'] = extract_win_rate(full_txt)
            
            m_st = re.search(r"ST(\.\d{2}|\d\.\d{2})", full_txt.replace(" ", ""))
            if m_st:
                val = m_st.group(1)
                row[f'st{i}'] = float(val) if val.startswith("0") or val.startswith(".") else 0.20
            else: row[f'st{i}'] = 0.20
            if row[f'st{i}'] < 0: row[f'st{i}'] = 0.20 # フライング補正

            m_mo = re.findall(r"(\d{2}\.\d)", full_txt)
            valid_mo = [float(x) for x in m_mo if float(x) > 10.0]
            row[f'mo{i}'] = valid_mo[0] if valid_mo else 30.0
        except:
            row[f'wr{i}'], row[f'st{i}'], row[f'mo{i}'] = 0.0, 0.20, 30.0

    return row

def scrape_result(session, jcd, rno, date_str):
    url = f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}"
    soup = get_soup(session, url)
    if not soup or "データがありません" in soup.text: return None
    res = {"nirentan_combo": None, "nirentan_payout": 0}
    try:
        for row in soup.select("tr"):
            txt = clean_text(row.text)
            if "2連単" in txt:
                nums = row.select(".numberSet1_number")
                if len(nums) >= 2:
                    res["nirentan_combo"] = f"{nums[0].text}-{nums[1].text}"
                pay = row.select_one(".is-payout1")
                if pay: res["nirentan_payout"] = int(pay.text.replace("¥","").replace(",",""))
    except: pass
    return res

def scrape_odds(session, jcd, rno, date_str, target_boat=None, target_combo=None):
    return {"tansho": "1.0", "nirentan": "1.0"}
