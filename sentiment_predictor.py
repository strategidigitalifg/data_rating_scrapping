# ===============================================================
#   IFG Review Sentiment Predictor (GitHub Version)
#   - Membaca Google Sheet "Data Review"
#   - Jika kolom "Sentiment" kosong → prediksi pakai IndoBERT model
#   - Output 0 = neutral, 1 = negative
#   - Update kembali ke Google Sheets hanya untuk baris yang kosong
# ===============================================================

import os
import json
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from transformers import BertTokenizerFast, BertForSequenceClassification
import torch

print("🚀 Memulai proses prediksi sentiment...")

# === 1️⃣ Load kredensial (otomatis) ===
GDRIVE_CREDENTIAL_JSON = os.getenv("GDRIVE_CREDENTIAL_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "14j2qAsi18KRXKmwqrj6q8Uqjo0Q0xE6eCmoe7kCFUg4")

if not GDRIVE_CREDENTIAL_JSON:
    if os.path.exists("ifg-credentials.json"):
        with open("ifg-credentials.json", "r") as f:
            GDRIVE_CREDENTIAL_JSON = f.read()
    else:
        raise SystemExit("❌ GDRIVE_CREDENTIAL_JSON tidak ditemukan.")

info = json.loads(GDRIVE_CREDENTIAL_JSON)

# === 2️⃣ Setup Google Sheet ===
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(info, scopes=scopes)
gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# === 3️⃣ Load model IndoBERT ===
print("📦 Memuat model IndoBERT...")

MODEL_DIR = "./"  # model di root repo
tokenizer = BertTokenizerFast.from_pretrained(MODEL_DIR)
model = BertForSequenceClassification.from_pretrained(MODEL_DIR)

device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)
model.eval()

# === 4️⃣ Fungsi prediksi ===
def predict_sentiment(text):
    if not isinstance(text, str) or text.strip() == "":
        return ""

    inputs = tokenizer(
        text,
        padding=True,
        truncation=True,
        max_length=256,
        return_tensors="pt"
    )

    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        logits = model(**inputs).logits
        pred = torch.argmax(logits, dim=1).item()
        return int(pred)  # 0 = neutral, 1 = negative

# === 5️⃣ Baca sheet Data Review ===
print("📄 Membaca sheet 'Data Review'...")

ws = spreadsheet.worksheet("Data Review")
values = ws.get_all_values()

header = values[0]
rows = values[1:]
df = pd.DataFrame(rows, columns=header)

# Normalisasi kolom
df.columns = df.columns.str.strip()
if "Sentiment" not in df.columns:
    raise SystemExit("❌ Kolom 'Sentiment' tidak ditemukan di Data Review.")

# === 6️⃣ Pilih baris yang Sentiment kosong ===
df["Sentiment"] = df["Sentiment"].astype(str)
mask = (df["Sentiment"].str.strip() == "") | (df["Sentiment"].isin(["nan", "None"]))

df_missing = df[mask]

print(f"🧠 Jumlah data yang perlu diprediksi: {len(df_missing)}")

if df_missing.empty:
    print("ℹ️ Tidak ada baris yang perlu diprediksi.")
    exit()

# === 7️⃣ Prediksi satu per satu ===
predictions = []
for idx, row in df_missing.iterrows():
    detail = row.get("Detail", "")
    pred = predict_sentiment(detail)
    predictions.append(pred)
    df.loc[idx, "Sentiment"] = pred

print("✅ Prediksi selesai.")

# === 8️⃣ Update kembali ke Google Sheets ===
print("📤 Mengupdate Google Sheet...")

updated_values = [df.columns.tolist()] + df.values.tolist()
ws.update(updated_values, value_input_option="USER_ENTERED")

print("🎉 Sentiment update selesai pada",
      datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "WIB")
