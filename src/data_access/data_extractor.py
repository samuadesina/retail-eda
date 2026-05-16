import os

def save_to_csv(df, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)
    print(f"✅ Saved {len(df)} rows to {filepath}")