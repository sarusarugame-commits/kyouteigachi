import os
import json
import datetime
import pandas as pd
import numpy as np
import lightgbm as lgb
import google.generativeai as genai
import zipfile
from discordwebhook import Discord

# ★スクレイピング機能の読み込み
from scraper import scrape_race_data

# ==========================================
# ⚙️ 設定エリア
# ==========================================
BET_AMOUNT = 1000
genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model_gemini = genai.GenerativeModel('gemini-1.5-flash')
discord = Discord(url=os.environ["DISCORD_WEBHOOK_URL"])

MODEL_FILE = 'boat_model_nirentan.txt'
ZIP_MODEL = 'model.zip'
COMBOS = [f"{f}-{s}" for f in range(1, 7) for s in range(1, 7) if f != s]

def load_status():
    if not os.path.exists('status.json'): return {"notified": []}
    with open('status.json', 'r') as f: return json.load(f)

def save_status(status):
    with open('status.json', 'w') as f: json.dump(status, f, indent=4)

def engineer_features(df):
    """
    特徴量作成関数
    """
    # 1. 数値計算
    for i in range(1, 7):
        df[f'power_idx_{i}'] = df[f'wr{i}'] * (1.0 / (df[f'st{i}'] + 0.01))
    
    for i in range(1, 6):
        df[f'st_gap_{i}_{i+1}'] = df[f'st{i+1}'] - df[f'st{i}']
        df[f'wr_gap_{i}_{i+1}'] = df[f'wr{i}'] - df[f'wr{i+1}']
    
    avg_wr = df[[f'wr{i}' for i in range(1, 7)]].mean(axis=1)
    df['wr_1_vs_avg'] = df['wr1'] / (avg_wr + 0.001)

    # 2. 【重要】会場コード(jcd)を 'category' 型に変換
    df['jcd'] = df['jcd'].astype('category')

    return df

def main():
    print("🚀 Bot起動: デバッグモード (1会場・5レース限定)")
    
    # 1. モデルの解凍・結合
    if not os.path.exists(MODEL_FILE):
        if os.path.exists(ZIP_MODEL):
            print("📦 モデルを解凍中...")
            with zipfile.ZipFile(ZIP_MODEL, 'r') as f: f.extractall()
        elif os.path.exists('model_part_1'):
            print("🧩 分割モデルを結合中...")
            with open(ZIP_MODEL, 'wb') as f_out:
                for i in range(1, 10):
                    p = f'model_part_{i}'
                    if os.path.exists(p):
                        with open(p, 'rb') as f_in: f_out.write(f_in.read())
            with zipfile.ZipFile(ZIP_MODEL, 'r') as f: f.extractall()

    # 2. モデル読み込み
    try:
        bst = lgb.Booster(model_file=MODEL_FILE)
    except Exception as e:
        print(f"❌ モデル読み込み失敗: {e}")
        return

    status = load_status()
    today = datetime.datetime.now().strftime('%Y%m%d')
    
    # ★デバッグ用設定：1会場(range(1, 2))、5レース(range(1, 6))のみ
    for jcd in range(1, 2):
        for rno in range(1, 6):
            race_id = f"{today}_{str(jcd).zfill(2)}_{rno}"
            
            # 通知済みならスキップ
            if any(n['id'] == race_id for n in status["notified"]):
                continue

            try:
                # 1. スクレイピング
                print(f"🔍 Checking {race_id}...")
                raw_data = scrape_race_data(None, jcd, rno, today)
                
                # データがない場合はスキップ
                if raw_data is None:
                    continue

                # 2. 予測データの作成
                df = pd.DataFrame([raw_data])
                df = engineer_features(df)
                
                features = ['jcd', 'rno', 'wind', 'wr_1_vs_avg']
                for i in range(1, 7): features.extend([f'wr{i}', f'st{i}', f'ex{i}', f'power_idx_{i}'])
                for i in range(1, 6): features.extend([f'st_gap_{i}_{i+1}', f'wr_gap_{i}_{i+1}'])
                
                # 3. AI予測
                probs = bst.predict(df[features])[0]
                best_idx = np.argmax(probs)
                combo = COMBOS[best_idx]
                prob = probs[best_idx]
                
                # 4. 判定と通知 (デバッグなので確率低くてもログに出す)
                print(f"   👉 予測: {combo} (確率:{prob:.2%})")

                if prob > 0.4:
                    print(f"✨ 有力候補発見！")
                    prompt = f"競艇予測AIです。{jcd}場{rno}R、的中率{prob:.2%}で「{combo}」を有力と判断しました。推奨できますか？一言で回答して。"
                    try:
                        res = model_gemini.generate_content(prompt).text
                    except:
                        res = "Gemini API応答なし"

                    discord.post(content=f"🐛 **デバッグ通知**\n場所: {jcd}場 {rno}R\n推奨: **{combo}**\nAI確率: {prob:.2%}\nGemini: {res}")
                    
                    status["notified"].append({"id": race_id, "combo": combo})
                    save_status(status)

            except Exception as e:
                print(f"⚠️ Error {race_id}: {e}")

if __name__ == "__main__":
    main()
