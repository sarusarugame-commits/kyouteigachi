import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import os

# ==========================================
# ⚙️ 設定エリア
# ==========================================
MODEL_FILE = 'ultimate_boat_model.pkl'
STRATEGY_FILE = 'ultimate_winning_strategies.csv'

# 厳選フィルタ（上振れ排除設定）
MIN_PROFIT = 1000   # 収支1000円以上のみ対象
MIN_ROI = 110       # 回収率110%以上のみ対象

# モデル特徴量（学習時と完全に一致させること）
BASE_FEATURES = [
    'wind',
    'wr1', 'mo1', 'ex1', 'st1',
    'wr2', 'mo2', 'ex2', 'st2',
    'wr3', 'mo3', 'ex3', 'st3',
    'wr4', 'mo4', 'ex4', 'st4',
    'wr5', 'mo5', 'ex5', 'st5',
    'wr6', 'mo6', 'ex6', 'st6'
]

def engineer_features(df):
    """AIモデル用の特徴量（相対評価など）を作成"""
    # 数値型に変換（念のため）
    cols_to_convert = BASE_FEATURES
    for col in cols_to_convert:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # 平均値
    df['wr_mean'] = df[[f'wr{i}' for i in range(1, 7)]].mean(axis=1)
    df['mo_mean'] = df[[f'mo{i}' for i in range(1, 7)]].mean(axis=1)
    df['ex_mean'] = df[[f'ex{i}' for i in range(1, 7)]].mean(axis=1)
    df['st_mean'] = df[[f'st{i}' for i in range(1, 7)]].mean(axis=1)

    new_feats = []
    for i in range(1, 7):
        # 偏差（相対評価）
        df[f'wr{i}_rel'] = df[f'wr{i}'] - df['wr_mean']
        df[f'mo{i}_rel'] = df[f'mo{i}'] - df['mo_mean']
        df[f'ex{i}_rel'] = df['ex_mean'] - df[f'ex{i}'] # タイムは小さい方が良い(逆転)
        df[f'st{i}_rel'] = df['st_mean'] - df[f'st{i}'] # STも小さい方が良い(逆転)
        new_feats.extend([f'wr{i}_rel', f'mo{i}_rel', f'ex{i}_rel', f'st{i}_rel'])
    
    # 学習時と同じカラム順序で返す
    return df[BASE_FEATURES + new_feats]

def predict_race(raw_data):
    """
    raw_data: scraper.scrape_race_dataで取得した辞書データ
    戻り値: 推奨買い目のリスト（なければ空リスト）
    """
    if not os.path.exists(MODEL_FILE) or not os.path.exists(STRATEGY_FILE):
        # ファイルがない場合はログを出して終了
        # print("⚠️ Model or Strategy file not found.", flush=True)
        return []

    try:
        # 1. データフレーム化
        # raw_dataは辞書なのでリストに入れてDataFrame化
        df = pd.DataFrame([raw_data])
        
        # 2. 特徴量エンジニアリング
        df_features = engineer_features(df)
        
        # 3. モデルロード
        models = joblib.load(MODEL_FILE)
        
        # 4. 予測実行
        # models['r1'], ['r2'], ['r3'] がそれぞれ 1着,2着,3着予測モデルと想定
        # LightGBMのpredictはクラス確率を返すため、argmaxで最も確率の高いクラス(艇番号)を取得
        # ※艇番号は1~6だが、クラスindexは0~5の場合が多いので +1 する補正を入れる
        
        # クラス分類(LGBMClassifier)の場合: predict_probaの最大値インデックス
        try:
            p1_idx = np.argmax(models['r1'].predict_proba(df_features), axis=1)[0]
            p2_idx = np.argmax(models['r2'].predict_proba(df_features), axis=1)[0]
            p3_idx = np.argmax(models['r3'].predict_proba(df_features), axis=1)[0]
        except:
            # もしpredictが直接クラスを返す場合(LGBMRegressorなど)
            p1_idx = int(models['r1'].predict(df_features)[0]) - 1
            p2_idx = int(models['r2'].predict(df_features)[0]) - 1
            p3_idx = int(models['r3'].predict(df_features)[0]) - 1

        # index(0-5) -> 艇番号(1-6)
        p1 = p1_idx + 1
        p2 = p2_idx + 1
        p3 = p3_idx + 1
        
        # 買い目フォーマット作成
        form_3t = f"{p1}-{p2}-{p3}"
        form_2t = f"{p1}-{p2}"
        
        # 5. 戦略ファイル読み込み
        strategies = pd.read_csv(STRATEGY_FILE)
        
        # フィルタリング (ROI 110%以上, 収支1000円以上)
        valid_strategies = strategies[
            (strategies['回収率'] >= MIN_ROI) & 
            (strategies['収支'] >= MIN_PROFIT)
        ]
        
        recommendations = []

        # --- 3連単の判定 ---
        # AIの予想した買い目(form_3t)が、過去に儲かった買い目リストにあるか？
        target_strat_3t = valid_strategies[
            (valid_strategies['券種'] == '3連単') & 
            (valid_strategies['買い目'] == form_3t)
        ]
        
        if not target_strat_3t.empty:
            # 重複除外 (1-1-2などはあり得ない)
            if p1 != p2 and p1 != p3 and p2 != p3:
                row = target_strat_3t.iloc[0]
                recommendations.append({
                    'type': '3連単',
                    'combo': form_3t,
                    'prob': row['的中率'], # %表記のまま渡すか調整
                    'profit': int(row['収支']), # 期待収支額
                    'roi': row['回収率']
                })

        # --- 2連単の判定 ---
        target_strat_2t = valid_strategies[
            (valid_strategies['券種'] == '2連単') & 
            (valid_strategies['買い目'] == form_2t)
        ]
        
        if not target_strat_2t.empty:
            if p1 != p2:
                row = target_strat_2t.iloc[0]
                recommendations.append({
                    'type': '2連単',
                    'combo': form_2t,
                    'prob': row['的中率'],
                    'profit': int(row['収支']),
                    'roi': row['回収率']
                })
                
        return recommendations

    except Exception as e:
        # print(f"❌ Predict Error: {e}", flush=True)
        return []
