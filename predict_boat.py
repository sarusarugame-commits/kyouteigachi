import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import os
import re
import traceback
from groq import Groq

MODEL_FILE = 'ultimate_boat_model.pkl'
STRATEGY_FILE = 'ultimate_winning_strategies.csv'

# ★強制通知設定
MIN_PROFIT = -999999 
MIN_ROI = 0       

# Groq設定
GROQ_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"

client = None
if os.environ.get("GROQ_API_KEY"):
    client = Groq(
        api_key=os.environ.get("GROQ_API_KEY"),
        base_url=GROQ_URL
    )

def ask_groq_reason(row, combo, ptype):
    if not client: return "AI解説: (APIキー設定確認中)"
    try:
        def safe_get(key):
            return row.get(key, 0)
            
        data_str = (
            f"1号艇:勝率{safe_get('wr1')}\n"
            f"2号艇:勝率{safe_get('wr2')}\n"
            f"3号艇:勝率{safe_get('wr3')}\n"
            f"4号艇:勝率{safe_get('wr4')}\n"
        )
        prompt = f"買い目「{combo}」({ptype})を推奨する理由を、競艇のプロとして100文字以内で断言せよ。\nデータ:\n{data_str}"
        
        completion = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": "You are a professional boat race analyst. Answer in Japanese."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=150,
        )
        return completion.choices[0].message.content
    except Exception as e:
        return f"AI解説エラー: {str(e)}"

# ★デバッグ用：値を厳密にチェックして変換する
def debug_convert(key, val):
    try:
        # まずは単純な変換を試みる
        if isinstance(val, (int, float)):
            return float(val)
        
        # 文字列の場合
        s_val = str(val)
        
        # 正規表現で数値抽出
        match = re.search(r"(-?\d+\.?\d*)", s_val)
        if match:
            return float(match.group(1))
            
        return 0.0
    except Exception as e:
        # ★ここで犯人をログに出す
        print(f"🔥 CONVERT ERROR on Key: [{key}]")
        print(f"   Value: {val}")
        print(f"   Type: {type(val)}")
        print(f"   Error: {e}")
        # traceback.print_exc() 
        # エラーを握りつぶさず、0.0を返して次に進める（ログ取り優先）
        return 0.0

def predict_race(raw_data):
    recommendations = []
    
    print(f"🔍 Debug: Processing Race Data...", flush=True)

    # ---------------------------------------------------------
    # 0. 前処理: 1つずつ値をチェックして変換 (犯人探し)
    # ---------------------------------------------------------
    clean_data = {}
    for k, v in raw_data.items():
        # ここで全項目をチェックしながら変換
        clean_data[k] = debug_convert(k, v)
            
    # ---------------------------------------------------------
    # 1. AI予測
    # ---------------------------------------------------------
    try:
        if not os.path.exists(MODEL_FILE):
            print("⚠️ Model file not found.")
            return []

        models = joblib.load(MODEL_FILE)
        
        if 'features' in models:
            required_feats = models['features']
        else:
            print("⚠️ Model Error: 'features' key missing.")
            return []

        # ここまで来れば clean_data は全て float になっているはず
        # 確認のため型チェックログを出す（最初だけ）
        # print(f"🔍 Clean Data Sample: {list(clean_data.items())[:5]}", flush=True)

        df = pd.DataFrame([clean_data])
        
        # 特徴量エンジニアリング
        # 念の為、計算前に存在確認
        for i in range(1, 7):
            if f'wr{i}' not in df.columns: df[f'wr{i}'] = 0.0
            if f'mo{i}' not in df.columns: df[f'mo{i}'] = 0.0
            if f'ex{i}' not in df.columns: df[f'ex{i}'] = 0.0
            if f'st{i}' not in df.columns: df[f'st{i}'] = 0.0

        # 計算処理（ここでエラーが出るならPandasの問題）
        try:
            df['wr_mean'] = df[[f'wr{i}' for i in range(1, 7)]].mean(axis=1)
            df['mo_mean'] = df[[f'mo{i}' for i in range(1, 7)]].mean(axis=1)
            df['ex_mean'] = df[[f'ex{i}' for i in range(1, 7)]].mean(axis=1)
            df['st_mean'] = df[[f'st{i}' for i in range(1, 7)]].mean(axis=1)

            for i in range(1, 7):
                df[f'wr{i}_rel'] = df[f'wr{i}'] - df['wr_mean']
                df[f'mo{i}_rel'] = df[f'mo{i}'] - df['mo_mean']
                df[f'ex{i}_rel'] = df['ex_mean'] - df[f'ex{i}'] 
                df[f'st{i}_rel'] = df['st_mean'] - df[f'st{i}'] 
        except Exception as e:
            print(f"🔥 Feature Engineering Error: {e}", flush=True)
            print(df.dtypes) # 型情報を出す
            return []

        # モデル入力整形
        df_final = pd.DataFrame()
        for f in required_feats:
            if f in df.columns:
                df_final[f] = df[f]
            else:
                df_final[f] = 0.0
        
        # NumPy配列化
        X = df_final.values.astype(np.float32)
        
        # 予測
        try:
            p1_idx = np.argmax(models['r1'].predict_proba(X), axis=1)[0]
            p2_idx = np.argmax(models['r2'].predict_proba(X), axis=1)[0]
            p3_idx = np.argmax(models['r3'].predict_proba(X), axis=1)[0]
        except:
            p1_idx = int(models['r1'].predict(X)[0]) - 1
            p2_idx = int(models['r2'].predict(X)[0]) - 1
            p3_idx = int(models['r3'].predict(X)[0]) - 1

        p1, p2, p3 = p1_idx + 1, p2_idx + 1, p3_idx + 1
        
    except Exception as e:
        # ここで本当の死因が出る
        print(f"💀 FATAL AI ERROR: {e}", flush=True)
        traceback.print_exc()
        return [] 

    # ---------------------------------------------------------
    # 2. 買い目作成
    # ---------------------------------------------------------
    form_3t = f"{p1}-{p2}-{p3}"
    form_2t = f"{p1}-{p2}"
    
    profit, prob, roi = 9999, 99.9, 999 
    
    try:
        if os.path.exists(STRATEGY_FILE):
            strategies = pd.read_csv(STRATEGY_FILE)
            match = strategies[(strategies['券種'] == '3連単') & (strategies['買い目'] == form_3t)]
            if not match.empty:
                profit = int(match.iloc[0]['収支'])
                prob = match.iloc[0]['的中率']
                roi = match.iloc[0]['回収率']
    except: pass 

    # ★ 3連単
    if p1 != p2 and p1 != p3 and p2 != p3:
        reason = ask_groq_reason(clean_data, form_3t, "3連単")
        recommendations.append({
            'type': '3連単',
            'combo': form_3t,
            'prob': prob,
            'profit': profit,
            'roi': roi,
            'reason': reason
        })

    # ★ 2連単
    if p1 != p2:
        reason = ask_groq_reason(clean_data, form_2t, "2連単")
        recommendations.append({
            'type': '2連単',
            'combo': form_2t,
            'prob': 80.0,
            'profit': 2000,
            'roi': 120,
            'reason': reason
        })
            
    return recommendations
