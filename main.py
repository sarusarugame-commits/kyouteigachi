import os
import json
import datetime
import time
import pandas as pd
import numpy as np
import lightgbm as lgb
import google.generativeai as genai
import zipfile
import requests  # ← これが抜けていたので修正しました
from discordwebhook import Discord

# スクレイピング機能の読み込み
from scraper import scrape_race_data, scrape_result

# ==========================================
# ⚙️ 設定エリア
# ==========================================
BET_AMOUNT = 1000  # 的中計算用の仮想投資額
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
model_gemini = genai.GenerativeModel('gemini-1.5-flash')
discord = Discord(url=os.environ["DISCORD_WEBHOOK_URL"])

MODEL_FILE = 'boat_model_nirentan.txt'
ZIP_MODEL = 'model.zip'
COMBOS = [f"{f}-{s}" for f in range(1, 7) for s in range(1, 7) if f != s]
PLACE_NAMES = {
    1: "桐生", 2: "戸田", 3: "江戸川", 4: "平和島", 5: "多摩川", 6: "浜名湖",
    7: "蒲郡", 8: "常滑", 9: "津", 10: "三国", 11: "びわこ", 12: "住之江",
    13: "尼崎", 14: "鳴門", 15: "丸亀", 16: "児島", 17: "宮島", 18: "徳山",
    19: "下関", 20: "若松", 21: "芦屋", 22: "福岡", 23: "唐津", 24: "大村"
}

def load_status():
    if not os.path.exists('status.json'):
        return {"notified": [], "total_balance": 0}
    with open('status.json', 'r') as f:
        return json.load(f)

def save_status(status):
    with open('status.json', 'w') as f:
        json.dump(status, f, indent=4)

def engineer_features(df):
    for i in range(1, 7):
        df[f'power_idx_{i}'] = df[f'wr{i}'] * (1.0 / (df[f'st{i}'] + 0.01))
    for i in range(1, 6):
        df[f'st_gap_{i}_{i+1}'] = df[f'st{i+1}'] - df[f'st{i}']
        df[f'wr_gap_{i}_{i+1}'] = df[f'wr{i}'] - df[f'wr{i+1}']
    avg_wr = df[[f'wr{i}' for i in range(1, 7)]].mean(axis=1)
    df['wr_1_vs_avg'] = df['wr1'] / (avg_wr + 0.001)
    df['jcd'] = df['jcd'].astype('category')
    return df

def main():
    print("🚀 Bot起動: 予想＆収支集計モード")
    session = requests.Session()
    status = load_status()
    today = datetime.datetime.now().strftime('%Y%m%d')

    # --- 1. モデルの準備 ---
    if not os.path.exists(MODEL_FILE):
        if os.path.exists(ZIP_MODEL):
            with zipfile.ZipFile(ZIP_MODEL, 'r') as f: f.extractall()
        elif os.path.exists('model_part_1'):
            with open(ZIP_MODEL, 'wb') as f_out:
                for i in range(1, 10):
                    p = f'model_part_{i}'
                    if os.path.exists(p):
                        with open(p, 'rb') as f_in: f_out.write(f_in.read())
            with zipfile.ZipFile(ZIP_MODEL, 'r') as f: f.extractall()

    try:
        bst = lgb.Booster(model_file=MODEL_FILE)
    except Exception as e:
        print(f"❌ モデル読み込み失敗: {e}")
        return

    # --- 2. 結果の確認・収支計算 ---
    print("📊 結果を確認中...")
    for item in status["notified"]:
        if item.get("checked"): continue
        
        # 過去3日以内のレースだけ確認（それより前は諦める）
        res = scrape_result(session, item["jcd"], item["rno"], item["date"])
        if res:
            is_win = (res["combo"] == item["combo"])
            payout = res["payout"] if is_win else 0
            profit = payout - BET_AMOUNT
            status["total_balance"] += profit
            item["checked"] = True
            
            place = PLACE_NAMES.get(item["jcd"], f"{item['jcd']}場")
            result_msg = (
                f"{'🎊 **的中！**' if is_win else '💀 不的中'}\n"
                f"レース: {place} {item['rno']}R ({item['date']})\n"
                f"予測: {item['combo']} → 結果: {res['combo']}\n"
                f"収支: {'+' if profit > 0 else ''}{profit}円\n"
                f"💰 現在の通算収支: {status['total_balance']}円"
            )
            discord.post(content=result_msg)
            save_status(status)

    # --- 3. 新しいレースの予想 ---
    print("🔍 新しいレースをパトロール中...")
    for jcd in range(1, 25):
        for rno in range(1, 13):
            race_id = f"{today}_{str(jcd).zfill(2)}_{rno}"
            if any(n['id'] == race_id for n in status["notified"]): continue

            try:
                raw_data = scrape_race_data(session, jcd, rno, today)
                if raw_data is None: continue

                df = pd.DataFrame([raw_data])
                df = engineer_features(df)
                
                features = ['jcd', 'rno', 'wind', 'wr_1_vs_avg']
                for i in range(1, 7): features.extend([f'wr{i}', f'st{i}', f'ex{i}', f'power_idx_{i}'])
                for i in range(1, 6): features.extend([f'st_gap_{i}_{i+1}', f'wr_gap_{i}_{i+1}'])
                
                probs = bst.predict(df[features])[0]
                best_idx = np.argmax(probs)
                combo = COMBOS[best_idx]
                prob = probs[best_idx]
                
                if prob > 0.4:
                    place_name = PLACE_NAMES.get(jcd, f"{jcd}場")
                    prompt = f"{place_name}{rno}R、的中率{prob:.2%}で「{combo}」と予測。推奨できるか一言で。"
                    try:
                        res_gemini = model_gemini.generate_content(prompt).text
                    except:
                        res_gemini = "Gemini API応答なし"

                    vote_url = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={rno}&jcd={jcd:02d}&hd={today}"
                    live_url = f"https://www.boatrace.jp/owpc/pc/race/live?jcd={jcd:02d}&rno={rno}"

                    discord.post(content=(
                        f"🚀 **勝負レース発見！**\n🏁 **{place_name} {rno}R**\n🔥 推奨: **{combo}**\n"
                        f"📊 AI確率: {prob:.2%}\n🤖 Gemini: {res_gemini}\n\n"
                        f"🗳 [出走表]({vote_url}) | 📺 [ライブ]({live_url})"
                    ))
                    
                    status["notified"].append({
                        "id": race_id, "jcd": jcd, "rno": rno, 
                        "date": today, "combo": combo, "checked": False
                    })
                    save_status(status)
                
                time.sleep(0.5)
            except Exception as e:
                print(f"⚠️ Error {race_id}: {e}")

    save_status(status)
    print("✅ 全行程終了")

if __name__ == "__main__":
    main()
