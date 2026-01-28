import os
import sys
from groq import Groq

print("="*60)
print("🤖 Groq 通信・モデル診断")
print("="*60)

# 1. APIキー確認
api_key = os.environ.get("GROQ_API_KEY")
if not api_key:
    print("❌ エラー: GROQ_API_KEY が設定されていません。")
    sys.exit(1)
print("✅ APIキー: 設定あり")

# 2. クライアント作成（公式推奨・最も標準的な設定）
print("\n--- 接続テスト ---")
try:
    client = Groq(api_key=api_key)
    print("✅ クライアント初期化: 成功")
except Exception as e:
    print(f"❌ クライアント初期化エラー: {e}")
    sys.exit(1)

# 3. 指定モデルでの生成テスト
# あなたが指定したモデル
TARGET_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"

print(f"\n--- モデル生成テスト: {TARGET_MODEL} ---")
try:
    completion = client.chat.completions.create(
        model=TARGET_MODEL,
        messages=[
            {"role": "user", "content": "Hello, are you working?"}
        ],
        max_tokens=20
    )
    print("🎉 成功！ Groqからの応答:")
    print(f"   >> {completion.choices[0].message.content}")

except Exception as e:
    print(f"💀 生成エラー発生: {e}")
    print("\n🔍 エラー分析:")
    err_msg = str(e)
    if "404" in err_msg or "model_not_found" in err_msg:
        print("   👉 原因: 「モデル名」が存在しません。Groqでサポートされていない名前です。")
        print("      (Llama-4はまだGroqで公開されていない可能性があります)")
    elif "401" in err_msg:
        print("   👉 原因: APIキーが無効です。")
    elif "Connection" in err_msg:
        print("   👉 原因: 通信エラー。GitHub ActionsのIPがブロックされているか、URL設定ミスです。")
    else:
        print("   👉 原因: 不明なエラー。上記ログを確認してください。")

print("="*60)
