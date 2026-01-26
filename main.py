import os
import datetime
import time
import requests
import sqlite3
import concurrent.futures
import traceback
import threading
from collections import defaultdict

# 自作モジュール
from scraper import scrape_race_data, scrape_odds, scrape_result
from predict_boat import predict_race

# ==========================================
# ⚙️ 設定エリア
# ==========================================
DB_FILE = "race_data.db"
BET_AMOUNT = 1000
PLACE_NAMES = {
    1: "桐生", 2: "戸田", 3: "江戸川", 4: "平和島", 5: "多摩川", 6: "浜名湖",
    7: "蒲郡", 8: "常滑", 9: "津", 10: "三国", 11: "びわこ", 12: "住之江",
    13: "尼崎", 14: "鳴門", 15: "丸亀", 16: "児島", 17: "宮島", 18: "徳山",
    19: "下関", 20: "若松", 21: "芦屋", 22: "福岡", 23: "唐津", 24: "大村"
}
JST = datetime.timezone(datetime.timedelta(hours=9), 'JST')

def send_discord(content):
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url: return
    try: requests.post(url, json={"content": content}, timeout=10)
    except: pass

def get_db_connection():
    conn = sqlite3.connect(DB_FILE, timeout=60, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute('''CREATE TABLE IF NOT EXISTS history (
        race_id TEXT PRIMARY KEY, date TEXT, time TEXT, place TEXT, race_no INTEGER,
        predict_combo TEXT, predict_prob REAL, gemini_comment TEXT,
        result_combo TEXT, is_win INTEGER, payout INTEGER, profit INTEGER, status TEXT,
        best_boat TEXT, odds_tansho TEXT, odds_nirentan TEXT, result_tansho TEXT
    )''')
    conn.close()

# ==========================================
# 📊 結果報告スレッド
# ==========================================
def report_worker():
    print("📋 [Report] 監視開始")
    while True:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT * FROM history WHERE status='PENDING'")
            pending = c.fetchall()
            
            # レース単位でまとめる
            races = defaultdict(list)
            for p in pending:
                base_id = "_".join(p['race_id'].split('_')[:3])
                races[base_id].append(p)
            
            sess = requests.Session()
            for base_id, items in races.items():
                _, jcd, rno = base_id.split('_')
                date_str = items[0]['date']
                
                res = scrape_result(sess, int(jcd), int(rno), date_str)
                if not res: continue # まだ結果出てない

                total_profit = 0
                lines = []
                
                for item in items:
                    hit = False
                    payout = 0
                    
                    if "単" in item['predict_combo']: # 2連単や3連単
                        if item['predict_combo'] == res['nirentan_combo']: # 今回は2連単のみ対応
                            hit = True
                            payout = res['nirentan_payout'] * (BET_AMOUNT/100)
                    # ※3連単への対応が必要ならここで分岐を追加

                    profit = int(payout - BET_AMOUNT)
                    total_profit += profit
                    
                    # DB更新
                    c.execute("UPDATE history SET status='FINISHED', is_win=?, payout=?, profit=? WHERE race_id=?", 
                              (1 if hit else 0, int(payout), profit, item['race_id']))
                    
                    icon = "🎯" if hit else "💀"
                    lines.append(f"{icon} {item['predict_combo']} ({profit:+d}円)")

                # 通知
                place = PLACE_NAMES.get(int(jcd), "場")
                msg = (f"🏁 **{place}{rno}R 結果**\n" + "\n".join(lines) + f"\n💰 計: {total_profit:+d}円")
                send_discord(msg)
                time.sleep(1)
            
            conn.close()
        except Exception as e:
            print(f"Report Error: {e}")
        time.sleep(300)

# ==========================================
# ⚡️ メイン処理
# ==========================================
def process_race(jcd, rno, today):
    sess = requests.Session()
    raw = scrape_race_data(sess, jcd, rno, today)
    if not raw: return [] # データなし
    
    # 締切チェック (現在時刻より未来か？)
    now = datetime.datetime.now(JST)
    if raw['deadline_time'] != "23:59":
        hm = raw['deadline_time'].split(':')
        deadline = now.replace(hour=int(hm[0]), minute=int(hm[1]), second=0)
        if deadline < now: return [] # 締切過ぎた
    
    # 予測実行
    preds = predict_race(raw)
    if not preds: return []
    
    # オッズ取得
    results = []
    for p in preds:
        combo = p['combo']
        best_b = p['best_boat']
        odds = scrape_odds(sess, jcd, rno, today, target_boat=str(best_b), target_combo=combo)
        
        p['odds'] = odds
        p['jcd'] = jcd
        p['rno'] = rno
        p['deadline'] = raw['deadline_time']
        results.append(p)
        
    return results

def main():
    print("🚀 最強AI Bot 起動")
    init_db()
    
    t = threading.Thread(target=report_worker, daemon=True)
    t.start()
    
    start_ts = time.time()
    
    while True:
        now = datetime.datetime.now(JST)
        if time.time() - start_ts > 21000: break # GitHub Actionsのタイムアウト対策
        
        today = now.strftime('%Y%m%d')
        print(f"⚡ Scan: {now.strftime('%H:%M:%S')}")
        
        # 既読チェック用
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT race_id FROM history")
        done_ids = {r[0] for r in c.fetchall()}
        conn.close()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            futures = []
            for jcd in range(1, 25):
                for rno in range(1, 13):
                    rid = f"{today}_{jcd}_{rno}"
                    # 簡易チェック: 既にこのレースの全チケット処理済みならスキップしたいが
                    # 組み合わせごとにIDが違うので、とりあえず予測させる
                    futures.append(ex.submit(process_race, jcd, rno, today))
            
            for f in concurrent.futures.as_completed(futures):
                try:
                    preds = f.result()
                    if not preds: continue
                    
                    conn = get_db_connection()
                    c = conn.cursor()
                    
                    new_bets = []
                    for p in preds:
                        race_id = f"{today}_{p['jcd']}_{p['rno']}_{p['combo']}"
                        if race_id in done_ids: continue
                        
                        # DB登録
                        c.execute("""
                            INSERT INTO history (race_id, date, time, place, race_no, predict_combo, predict_prob, status, best_boat)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
                        """, (race_id, today, now.strftime('%H:%M'), PLACE_NAMES.get(p['jcd']), p['rno'], p['combo'], p['prob'], str(p['best_boat'])))
                        
                        new_bets.append(p)
                        done_ids.add(race_id)
                    
                    if new_bets:
                        place = PLACE_NAMES.get(new_bets[0]['jcd'])
                        rno = new_bets[0]['rno']
                        dl = new_bets[0]['deadline']
                        
                        lines = [f"🔥 **{place}{rno}R** (締切 {dl})"]
                        for b in new_bets:
                            lines.append(f"🎫 [{b['type']}] **{b['combo']}** (期待値:{b['profit']}円)")
                        
                        send_discord("\n".join(lines))
                        print(f"✅ 通知: {place}{rno}R")
                        
                    conn.close()
                except Exception as e:
                    print(f"Error: {e}")
                    
        time.sleep(180) # 3分待機

if __name__ == "__main__":
    main()
