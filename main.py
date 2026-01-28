import os
import datetime
import time
import sqlite3
import concurrent.futures
import threading
import sys
import requests as std_requests
import json
import pandas as pd

# 自作モジュール
from scraper import scrape_race_data, get_session
from predict_boat import predict_race

DB_FILE = "race_data.db"
PLACE_NAMES = {i: n for i, n in enumerate(["","桐生","戸田","江戸川","平和島","多摩川","浜名湖","蒲郡","常滑","津","三国","びわこ","住之江","尼崎","鳴門","丸亀","児島","宮島","徳山","下関","若松","芦屋","福岡","唐津","大村"])}
JST = datetime.timezone(datetime.timedelta(hours=9), 'JST')

sys.stdout.reconfigure(encoding='utf-8')

def log(msg):
    print(msg, flush=True)

# ★修正ポイント：エラーを握りつぶさず、詳細を表示する
def send_discord(content):
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        log("❌ Discord Error: 環境変数 DISCORD_WEBHOOK_URL が設定されていません！")
        return

    # URLの形式チェック（誤ってスペースが入っていないかなど）
    if not url.startswith("http"):
        log(f"❌ Discord Error: URLの形式がおかしいです -> {url[:10]}...")
        return

    try:
        # 実際に送信
        resp = std_requests.post(url, json={"content": content}, timeout=10)
        
        # ステータスコードチェック
        if 200 <= resp.status_code < 300:
            log(f"✅ Discord送信成功: {resp.status_code}")
        else:
            # 400 Bad Request, 401 Unauthorized, 404 Not Found など
            log(f"💀 Discord送信失敗: Code {resp.status_code}")
            log(f"   Response: {resp.text}") # エラー内容（「Invalid Webhook Token」など）を表示
            
    except Exception as e:
        log(f"💀 Discord接続エラー: {e}")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("CREATE TABLE IF NOT EXISTS history (race_id TEXT PRIMARY KEY, date TEXT, place TEXT, race_no INTEGER, predict_combo TEXT, status TEXT, profit INTEGER)")
    conn.close()

def report_worker(stop_event):
    while not stop_event.is_set():
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            pending = conn.execute("SELECT * FROM history WHERE status='PENDING'").fetchall()
            sess = get_session()
            for p in pending:
                try: jcd = int(p['race_id'].split('_')[1])
                except: continue
                
                from scraper import scrape_result
                res = scrape_result(sess, jcd, p['race_no'], p['date'])
                if not res: continue

                hit = False
                payout = 0
                combo = p['predict_combo']
                result_str = "未確定"
                
                # 3連単 or 2連単 判定
                if str(combo).count("-") == 2:
                    if res.get('sanrentan_combo'):
                        result_str = res['sanrentan_combo']
                        if res['sanrentan_combo'] == combo:
                            hit = True
                            payout = res.get('sanrentan_payout', 0) * 10
                else:
                    if res.get('nirentan_combo'):
                        result_str = res['nirentan_combo']
                        if res['nirentan_combo'] == combo:
                            hit = True
                            payout = res.get('nirentan_payout', 0) * 10
                
                if result_str != "未確定":
                    profit = int(payout - 1000)
                    conn.execute("UPDATE history SET status='FINISHED', profit=? WHERE race_id=?", (profit, p['race_id']))
                    conn.commit()
                    
                    if hit:
                        msg = f"🎯 **{p['place']}{p['race_no']}R** 的中！！\n買い目: **{combo}**\n払戻: {int(payout):,}円\n収支: +{profit:,}円"
                        log(f"🎯 {p['place']}{p['race_no']}R 的中！ {combo} (+{profit}円)")
                        send_discord(msg)
                    else:
                        log(f"💀 {p['place']}{p['race_no']}R ハズレ... 予想:{combo} 結果:{result_str}")
            conn.close()
        except Exception as e:
            log(f"Report Error: {e}")
        
        for _ in range(10):
            if stop_event.is_set(): break
            time.sleep(60)

def process_race(jcd, rno, today):
    sess = get_session()
    place = PLACE_NAMES[jcd]
    try:
        raw, error = scrape_race_data(sess, jcd, rno, today)
    except Exception as e:
        log(f"❌ {place}{rno}R: エラー {e}")
        return

    if error: return
    if not raw or raw.get('wr1', 0) == 0: return

    # ログ出力（データ確認用）
    log(f"✅ {place}{rno}R 取得完了 ------------------------------")
    headers = [
        'date', 'jcd', 'rno', 'wind', 'res1', 'rank1', 'rank2', 'rank3',
        'tansho', 'nirentan', 'sanrentan', 'sanrenpuku', 'payout',
        'wr1', 'mo1', 'ex1', 'f1', 'st1',
        'wr2', 'mo2', 'ex2', 'f2', 'st2',
        'wr3', 'mo3', 'ex3', 'f3', 'st3',
        'wr4', 'mo4', 'ex4', 'f4', 'st4',
        'wr5', 'mo5', 'ex5', 'f5', 'st5',
        'wr6', 'mo6', 'ex6', 'f6', 'st6'
    ]
    values = [str(raw.get(k, '')) for k in headers]
    # log(f"   DATA: {','.join(values)}") # データログが多すぎる場合はコメントアウト推奨
    log("----------------------------------------------------------")

    try: preds = predict_race(raw)
    except: return
    if not preds: return

    conn = sqlite3.connect(DB_FILE)
    for p in preds:
        combo = p['combo']
        race_id = f"{today}_{jcd}_{rno}_{combo}"
        exists = conn.execute("SELECT 1 FROM history WHERE race_id=?", (race_id,)).fetchone()
        
        if not exists:
            ptype = p.get('type', '不明')
            profit = p.get('profit', 0)
            prob = p.get('prob', 0)
            roi = p.get('roi', 0)
            reason = p.get('reason', 'AI解説なし')
            
            log(f"🔥 [HIT] {place}{rno}R -> {combo} (期待値:{profit}円/確率:{prob}%)")
            odds_url = f"https://www.boatrace.jp/owpc/pc/race/odds3t?rno={rno}&jcd={jcd:02d}&hd={today}"

            msg = (
                f"🔥 **{place}{rno}R** AI激熱予想\n"
                f"🎯 買い目: **{combo}** ({ptype})\n"
                f"💰 期待値: **+{profit}円**\n"
                f"📊 自信度: **{prob}%** (回収率:{roi}%)\n"
                f"📝 **AI解説**: {reason}\n"
                f"🔗 [オッズ確認・投票]({odds_url})"
            )
            
            conn.execute("INSERT INTO history VALUES (?,?,?,?,?,?,?)", (race_id, today, place, rno, combo, 'PENDING', 0))
            conn.commit()
            
            # ここで送信処理を呼び出す
            send_discord(msg)
            
    conn.close()

def main():
    log("🚀 最強AI Bot (デバッグモード: 通知エラー全表示) 起動")
    
    # 起動時に一度だけテスト送信を行う（これでURLが死んでるか即わかる）
    log("🧪 起動時 Discord接続テスト...")
    send_discord("🚀 Botが起動しました。このメッセージが見えていますか？")

    init_db()
    stop_event = threading.Event()
    t = threading.Thread(target=report_worker, args=(stop_event,), daemon=True)
    t.start()
    
    start_time = time.time()
    MAX_RUNTIME = 5.8 * 3600

    while True:
        now = datetime.datetime.now(JST)
        
        if now.hour == 23 and now.minute >= 55:
            log(f"🌙 {now.strftime('%H:%M')} ミッドナイト終了。")
            break
        
        if time.time() - start_time > MAX_RUNTIME:
            log("🔄 稼働時間上限。")
            break

        today = now.strftime('%Y%m%d')
        log(f"⚡ Scan Start: {now.strftime('%H:%M:%S')}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            for jcd in range(1, 25):
                for rno in range(1, 13):
                    ex.submit(process_race, jcd, rno, today)
        
        log("💤 休憩中...")
        time.sleep(300)

    stop_event.set()
    log("👋 Bot停止")

if __name__ == "__main__":
    main()
