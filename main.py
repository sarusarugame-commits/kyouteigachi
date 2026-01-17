import os
import json
import datetime
import time
import pandas as pd
import numpy as np
import lightgbm as lgb
import google.generativeai as genai
import zipfile
from discordwebhook import Discord
from scraper import scrape_race_data, scrape_result

# 設定
BET_AMOUNT = 1000 # 1点1000円で計算
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
model_gemini = genai.GenerativeModel('gemini-1.5-flash')
discord = Discord(url=os.environ["DISCORD_WEBHOOK_URL"])
MODEL_FILE = 'boat_model_nirentan.txt'
COMBOS = [f"{f}-{s}" for f in range(1, 7) for s in range(1, 7) if f != s]
PLACE_NAMES = {1:"桐生",2:"戸田",3:"江戸川",4:"平和島",5:"多摩川",6:"浜名湖",7:"蒲郡",8:"常滑",9:"津",10:"三国",11:"びわこ",12:"住之江",13:"尼崎",14:"鳴門",15:"丸亀",16:"児島",17:"宮島",18:"徳山",19:"下関",20:"若松",21:"芦屋",22:"福岡",23:"唐津",24:"大村"}

def load_status():
    if not os.path.exists('status.json'): return {"notified": [], "total_balance": 0}
    with open('status.json', 'r') as f: return json.load(f)

def save_status(status):
    with open('status.json', 'w') as f: json.dump(status, f, indent=4)

def main():
    print("🚀 Bot起動: 予想＆収支集計モード")
    session = requests.Session()
    status = load_status()
    today = datetime.datetime.now().strftime('%Y%m%d')

    # --- 1. 結果の確認フェーズ ---
    print("📊 前回までの結果を確認中...")
    for item in status["notified"]:
        if item.get("checked"): continue # すでに確認済みなら飛ばす
        
        res = scrape_result(session, item["jcd"], item["rno"], item["date"])
        if res:
            is_win = (res["combo"] == item["combo"])
            payout = res["payout"] if is_win else 0
            profit = payout - BET_AMOUNT
            status["total_balance"] += profit
            item["checked"] = True # 確認完了フラグ
            
            # 結果通知
            place = PLACE_NAMES.get(item["jcd"], "不明")
            result_msg = (
                f"{'🎊 **的中！**' if is_win else '💀 不的中'}\n"
                f"場所: {place} {item['rno']}R\n"
                f"予測: {item['combo']} → 結果: {res['combo']}\n"
                f"収支: {'+' if profit > 0 else ''}{profit}円\n"
                f"💰 通算収支: {status['total_balance']}円"
            )
            discord.post(content=result_msg)
            save_status(status)

    # --- 2. 新しいレースの予想フェーズ ---
    # (モデルの読み込み処理は省略せず、前回のコードを維持してください)
    # ※ここに前回の bst = lgb.Booster... などの予測ロジックが入ります
    # ※通知する際に status["notified"].append({"id": race_id, "jcd": jcd, "rno": rno, "date": today, "combo": combo, "checked": False})
    # として保存するのがコツです。

    save_status(status)
    print("✅ 巡回完了")

if __name__ == "__main__":
    main()
