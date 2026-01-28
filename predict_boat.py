import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import os
from groq import Groq

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MODEL_FILE = 'ultimate_boat_model.pkl'
STRATEGY_FILE = 'ultimate_winning_strategies.csv'

# 厳選フィルタ
MIN_PROFIT = 0      # 利益0円以上ならOK
MIN_ROI = 100       # 回収率100%以上ならOK      

# モデル特徴量
BASE_FEATURES = [
    'wind',
    'wr1', 'mo1', 'ex1', 'st1',
    'wr2', 'mo2', 'ex2', 'st2',
    'wr3', 'mo3', 'ex3', 'st3',
    'wr4', 'mo4', 'ex4', 'st4',
    'wr5', 'mo5', 'ex5', 'st5',
    'wr6', 'mo6', 'ex6', 'st6'
]

# Groqクライアント (Base URLを明示)
client = None
if os.environ.get("GROQ_API_KEY"):
    client = Groq(
        api_key=os.environ.get("GROQ_API_KEY"),
        base_url="https://api.groq.com/openai/v1"  # ★ここを追加
    )

def ask_groq_reason(row, combo, ptype):
    """
    指定された最新モデル (Llama-4 Scout) で解説を生成
    """
    if not client:
        return "AI解説: APIキー未設定のため解説スキップ"

    try:
        data_str = (
            f"風速:{row['wind']}m\n"
            f"1号艇:勝率{row['wr1']} モータ{row['mo1']} ST{row['st1']}\n"
            f"2号艇:勝率{row['wr2']} モータ{row['mo2']} ST{row['st2']}\n"
            f"3号艇:勝率{row['wr3']} モータ{row['mo3']} ST{row['st3']}\n"
            f"4号艇:勝率{row['wr4']} モータ{row['mo4']} ST{row['st4']}\n"
            f"5号艇:勝率{row['wr5']} モータ{row['mo5']} ST{row['st5']}\n"
            f"6号艇:勝率{row['wr6']} モータ{row['mo6']} ST{row['st6']}\n"
        )

        prompt = (
            f"あなたはプロの競艇予想家です。以下のレースデータに基づき、"
            f"なぜ買い目「{combo}」({ptype})が推奨できるのか、"
            f"展開（逃げ、まくり、差しなど）やモーター気配に触れて、"
            f"100文字以内で「激熱な理由」を断言してください。\n\n"
            f"[データ]\n{data_str}"
        )

        # ★ご指定のモデル
        completion = client.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[
                {"role": "system", "content": "You are a professional boat race analyst. Answer in Japanese."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=150,
        )
        return completion.choices[0].message.content
    except Exception as e:
        return f"AI解説生成エラー: {e}"

def engineer_features(df):
    """特徴量エンジニアリング"""
    cols_to_convert = BASE_FEATURES
    for col in cols_to_convert:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    df['wr_mean'] = df[[f'wr{i}' for i in range(1, 7)]].mean(axis=1)
    df['mo_mean'] = df[[f'mo{i}' for i in range(1, 7)]].mean(axis=1)
    df['ex_mean'] = df[[f'ex{i}' for i in range(1, 7)]].mean(axis=1)
    df['st_mean'] = df[[f'st{i}' for i in range(1, 7)]].mean(axis=1)

    new_feats = []
    for i in range(1, 7):
        df[f'wr{i}_rel'] = df[f'wr{i}'] - df['wr_mean']
        df[f'mo{i}_rel'] = df[f'mo{i}'] - df['mo_mean']
        df[f'ex{i}_rel'] = df['ex_mean'] - df[f'ex{i}'] 
        df[f'st{i}_rel'] = df['st_mean'] - df[f'st{i}'] 
        new_feats.extend([f'wr{i}_rel', f'mo{i}_rel', f'ex{i}_rel', f'st{i}_rel'])
    
    return df[BASE_FEATURES + new_feats]

def predict_race(raw_data):
    if not os.path.exists(MODEL_FILE) or not os.path.exists(STRATEGY_FILE):
        return []

    try:
        df = pd.DataFrame([raw_data])
        df_features = engineer_features(df)
        models = joblib.load(MODEL_FILE)
        
        try:
            p1_idx = np.argmax(models['r1'].predict_proba(df_features), axis=1)[0]
            p2_idx = np.argmax(models['r2'].predict_proba(df_features), axis=1)[0]
            p3_idx = np.argmax(models['r3'].predict_proba(df_features), axis=1)[0]
        except:
            p1_idx = int(models['r1'].predict(df_features)[0]) - 1
            p2_idx = int(models['r2'].predict(df_features)[0]) - 1
            p3_idx = int(models['r3'].predict(df_features)[0]) - 1

        p1, p2, p3 = p1_idx + 1, p2_idx + 1, p3_idx + 1
        
        form_3t = f"{p1}-{p2}-{p3}"
        form_2t = f"{p1}-{p2}"
        
        strategies = pd.read_csv(STRATEGY_FILE)
        valid_strategies = strategies[
            (strategies['回収率'] >= MIN_ROI) & 
            (strategies['収支'] >= MIN_PROFIT)
        ]
        
        recommendations = []

        # 3連単判定
        target_strat_3t = valid_strategies[(valid_strategies['券種'] == '3連単') & (valid_strategies['買い目'] == form_3t)]
        if not target_strat_3t.empty:
            if p1 != p2 and p1 != p3 and p2 != p3:
                row = target_strat_3t.iloc[0]
                reason = ask_groq_reason(raw_data, form_3t, "3連単")
                recommendations.append({
                    'type': '3連単',
                    'combo': form_3t,
                    'prob': row['的中率'],
                    'profit': int(row['収支']),
                    'roi': row['回収率'],
                    'reason': reason
                })

        # 2連単判定
        target_strat_2t = valid_strategies[(valid_strategies['券種'] == '2連単') & (valid_strategies['買い目'] == form_2t)]
        if not target_strat_2t.empty:
            if p1 != p2:
                row = target_strat_2t.iloc[0]
                reason = ask_groq_reason(raw_data, form_2t, "2連単")
                recommendations.append({
                    'type': '2連単',
                    'combo': form_2t,
                    'prob': row['的中率'],
                    'profit': int(row['収支']),
                    'roi': row['回収率'],
                    'reason': reason
                })
                
        return recommendations

    except Exception as e:
        return []
