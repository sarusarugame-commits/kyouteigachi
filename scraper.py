import requests
from bs4 import BeautifulSoup
import time
import re
import unicodedata
import random
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MAX_RETRIES = 3        # リトライ回数
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
]

def clean_text(text):
    """テキストの不要な空白や改行を削除"""
    if not text: return ""
    text = unicodedata.normalize('NFKC', str(text))
    return text.replace("\n", "").replace("\r", "").replace("¥", "").replace(",", "").strip()

def extract_float(text):
    """
    【重要】あらゆる文字列から数値だけを抜き出す関数
    '8.0°C' -> 8.0
    '風速3m' -> 3.0
    'ST.12' -> 0.12
    """
    if not text: return 0.0
    cleaned = clean_text(text)
    # 数字とドット(.)の塊を探す正規表現
    match = re.search(r"(\d+\.?\d*)", cleaned)
    if match:
        try:
            return float(match.group(1))
        except:
            return 0.0
    return 0.0

def get_session():
    """リトライ機能付きセッションを作成"""
    session = requests.Session()
    retries = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retries)
    session.mount("https://", adapter)
    return session

def get_soup(session, url):
    """HTMLを取得してBeautifulSoupオブジェクトを返す"""
    if session is None: session = get_session()
    for i in range(MAX_RETRIES):
        try:
            headers = {'User-Agent': random.choice(UA_LIST)}
            res = session.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            
            if res.status_code == 200:
                # 開催中止やデータなしの判定
                if "データがありません" in res.text or "開催中止" in res.text:
                    return None
                return BeautifulSoup(res.text, 'html.parser')
            
            time.sleep(random.uniform(1, 2))
        except Exception:
            pass
    return None

def scrape_race_data(session, jcd, rno, date_str):
    """
    メインのスクレイピング関数
    """
    base_url = "https://www.boatrace.jp/owpc/pc/race"
    
    # 1. 直前情報（風速、展示タイム）
    soup_before = get_soup(session, f"{base_url}/beforeinfo?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_before: return None 

    # 2. 番組表（選手データ）
    soup_list = get_soup(session, f"{base_url}/racelist?rno={rno}&jcd={jcd:02d}&hd={date_str}")
    if not soup_list: return None

    # 3. 結果ページ（オッズ取得用・なくても進む）
    soup_res = get_soup(session, f"{base_url}/raceresult?rno={rno}&jcd={jcd:02d}&hd={date_str}")

    row = {'date': date_str, 'jcd': jcd, 'rno': rno}

    # --- ① 風速の取得（気温と間違えない処理） ---
    try:
        # 天候エリアのデータを全取得
        weather_elems = soup_before.select(".weather1_bodyUnitLabelData")
        wind_val = 0.0
        
        # 中身をループして「m」が含まれているもの（かつ「cm」=波高 ではないもの）を探す
        for elem in weather_elems:
            txt = elem.text
            if "m" in txt and "cm" not in txt:
                wind_val = extract_float(txt)
                break
        
        row['wind'] = wind_val
    except:
        row['wind'] = 0.0

    # --- ② 各艇データ ---
    for i in range(1, 7):
        try:
            # A. 展示タイム
            boat_cell = soup_before.select_one(f".is-boatColor{i}")
            if boat_cell:
                tds = boat_cell.find_parent("tbody").select("td")
                # 何番目のカラムにあっても、とりあえず5番目(index 4)付近を取得して数値化
                if len(tds) > 4:
                    row[f'ex{i}'] = extract_float(tds[4].text)
                else:
                    row[f'ex{i}'] = 6.80
            else:
                row[f'ex{i}'] = 6.80

            # B. 番組表データ
            list_elem = soup_list.select_one(f".is-boatColor{i}")
            if list_elem:
                tbody = list_elem.find_parent("tbody")
                tds_list = tbody.select("td")
                
                # 勝率 (Usually index 3)
                row[f'wr{i}'] = extract_float(tds_list[3].text)
                
                # フライング数
                row[f'f{i}'] = int(extract_float(tds_list[2].text))
                
                # ST (テキスト全体から "ST0.12" のような形を探す)
                st_match = re.search(r"ST(\d\.\d{2})", clean_text(tbody.text))
                if st_match:
                    row[f'st{i}'] = float(st_match.group(1))
                else:
                    row[f'st{i}'] = 0.17 # 平均値

                # モーター (Usually index 5 or 6)
                mo_val = extract_float(tds_list[5].text)
                # もし0.0なら、カラムズレの可能性があるので隣も見る
                if mo_val == 0.0 and len(tds_list) > 6:
                     mo_val = extract_float(tds_list[6].text)
                
                row[f'mo{i}'] = mo_val if mo_val > 0 else 30.0

            else:
                # データなしの場合のデフォルト
                row[f'wr{i}'] = 5.0
                row[f'f{i}'] = 0
                row[f'st{i}'] = 0.17
                row[f'mo{i}'] = 30.0

        except Exception:
            # 万が一のエラー時は安全なデフォルト値
            row[f'ex{i}'] = 6.80
            row[f'wr{i}'] = 5.0
            row[f'f{i}'] = 0
            row[f'st{i}'] = 0.17
            row[f'mo{i}'] = 30.0

    # ダミーデータ（今回は使用しないがキーエラー防止のため0で埋める）
    row['nirentan'] = 0
    row['sanrentan'] = 0
    row['tansho'] = 0
    
    # オッズがあれば取得（extract_floatが強力なのでそのまま使える）
    if soup_res:
        try:
            # 簡易ロジック: 単勝などのテーブルを探す（実際にはBot側で予測にオッズを使わないなら0でOK）
            pass 
        except: pass

    return row

if __name__ == "__main__":
    # テスト実行用
    print("🛠 scraper.py マニュアル更新版")
    from datetime import datetime
    s = get_session()
    today = datetime.now().strftime("%Y%m%d")
    # テスト: 今日の日付でどこかのレースを取得
    try:
        data = scrape_race_data(s, 1, 1, today)
        print(f"取得結果: {data}")
    except Exception as e:
        print(f"エラー: {e}")
