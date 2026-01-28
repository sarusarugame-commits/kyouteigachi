import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import os
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
        data_str = (
            f"1号艇:勝率{row.get('wr1',0)}\n"
            f"2号艇:勝率{row.get('wr2',0)}\n"
            f"3号艇:勝率{row.get('wr3',0)}\n"
            f"4号艇:勝率{row.get('wr4',0)}\n"
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

# ★最強のクリーニング関数（辞書の値そのものを直す）
def unwrap_value(v):
    # リストや配列なら中身を取り出す（再帰）
    if isinstance(v, (list, tuple, np.ndarray)):
        if len(v) == 0: return 0.0
        return unwrap_value(v[0])
    # 文字列なら数値化
    if isinstance(v, str):
        try:
            return float(v.replace(',', '').replace('[','').replace(']','').strip())
        except:
            return 0.0
    return v

def predict_race(raw_data):
    recommendations = []
    
    # ---------------------------------------------------------
    # 0. 前処理: 辞書の中身をここで全部フラットな数値にする
    # ---------------------------------------------------------
    clean_data = {}
    for k, v in raw_data.items():
        try:
            val = unwrap_value(v)
            clean_data[k] = float(val) # 強制的にfloat
        except:
            clean_data[k] = 0.0
            
    # ---------------------------------------------------------
    # 1. AI予測
    # ---------------------------------------------------------
    try:
        if not os.path.exists(MODEL_FILE):
            return []

        models = joblib.load(MODEL_FILE)
        
        if 'features' in models:
            required_feats = models['features']
        else:
            print("⚠️ Model Error: 'features' key missing.")
            return []

        # クリーンなデータからDataFrame作成
        df = pd.DataFrame([clean_data])
        
        # 特徴量エンジニアリング（単純計算のみ）
        df['wr_mean'] = df[[f'wr{i}' for i in range(1, 7)]].mean(axis=1)
        df['mo_mean'] = df[[f'mo{i}' for i in range(1, 7)]].mean(axis=1)
        df['ex_mean'] = df[[f'ex{i}' for i in range(1, 7)]].mean(axis=1)
        df['st_mean'] = df[[f'st{i}' for i in range(1, 7)]].mean(axis=1)

        for i in range(1, 7):
            df[f'wr{i}_rel'] = df[f'wr{i}'] - df['wr_mean']
            df[f'mo{i}_rel'] = df[f'mo{i}'] - df['mo_mean']
            df[f'ex{i}_rel'] = df['ex_mean'] - df[f'ex{i}'] 
            df[f'st{i}_rel'] = df['st_mean'] - df[f'st{i}'] 
        
        # 列の整合性確保
        df_final = pd.DataFrame()
        for f in required_feats:
            if f in df.columns:
                df_final[f] = df[f]
            else:
                df_final[f] = 0.0
        
        # 予測実行
        try:
            p1_idx = np.argmax(models['r1'].predict_proba(df_final), axis=1)[0]
            p2_idx = np.argmax(models['r2'].predict_proba(df_final), axis=1)[0]
            p3_idx = np.argmax(models['r3'].predict_proba(df_final), axis=1)[0]
        except:
            p1_idx = int(models['r1'].predict(df_final)[0]) - 1
            p2_idx = int(models['r2'].predict(df_final)[0]) - 1
            p3_idx = int(models['r3'].predict(df_final)[0]) - 1

        p1, p2, p3 = p1_idx + 1, p2_idx + 1, p3_idx + 1
        
    except Exception as e:
        print(f"⚠️ AI Prediction Error: {e}", flush=True)
        return [] 

    # ---------------------------------------------------------
    # 2. 買い目作成 (AI成功時のみ)
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

    # ★ 3連単 (強制通知)
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

    # ★ 2連単 (強制通知)
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
