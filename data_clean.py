# ===============================================================
#   IFG Review Data Merger + Text Cleaning (Final GitHub Version)
#   Menggabungkan review Google Play & App Store ke Google Sheets
#   - Hapus emoji & special characters
#   - Lowercase semua teks
#   - Cleaning typo dari CSV (df_typo)
#   - Normalisasi waktu ke WIB
#   - Hapus duplikat berdasarkan reviewId
# ===============================================================

import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import re

print("🚀 Memulai proses merge data dan cleaning...")

# === 1️⃣ Load kredensial (otomatis dari environment atau file lokal) ===
GDRIVE_CREDENTIAL_JSON = os.getenv("GDRIVE_CREDENTIAL_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "14j2qAsi18KRXKmwqrj6q8Uqjo0Q0xE6eCmoe7kCFUg4")

if not GDRIVE_CREDENTIAL_JSON:
    if os.path.exists("/content/ifg-credentials.json"):
        print("🔑 Memuat kredensial dari file lokal '/content/ifg-credentials.json'...")
        with open("/content/ifg-credentials.json", "r") as f:
            GDRIVE_CREDENTIAL_JSON = f.read()
    elif os.path.exists("ifg-credentials.json"):
        print("🔑 Memuat kredensial dari file lokal 'ifg-credentials.json'...")
        with open("ifg-credentials.json", "r") as f:
            GDRIVE_CREDENTIAL_JSON = f.read()
    else:
        raise SystemExit("❌ Tidak ditemukan GDRIVE_CREDENTIAL_JSON di environment maupun file lokal.")

try:
    info = json.loads(GDRIVE_CREDENTIAL_JSON)
except json.JSONDecodeError as e:
    raise SystemExit(f"❌ Format JSON di GDRIVE_CREDENTIAL_JSON tidak valid: {e}")

# === 2️⃣ Setup koneksi ke Google Sheets ===
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(info, scopes=scopes)
gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)

# === 3️⃣ Fungsi: Hapus Emoji ===
def remove_emojis(text):
    if pd.isna(text) or text is None:
        return ""
    emoji_pattern = re.compile(
        "["                             
        "\U0001F600-\U0001F64F"         
        "\U0001F300-\U0001F5FF"         
        "\U0001F680-\U0001F6FF"         
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE
    )
    return emoji_pattern.sub(r'', str(text))

# === 4️⃣ Fungsi: Hapus special character & lowercase ===
def clean_special_chars(text):
    if pd.isna(text) or text is None:
        return ""
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', str(text))
    return text.lower().strip()

# === 5️⃣ Load typo cleaning ===
# File bisa di root repo (GitHub Actions) atau di /content (Colab)
if os.path.exists("typo_cleaning.csv"):
    typo_path = "typo_cleaning.csv"
elif os.path.exists("/content/typo_cleaning.csv"):
    typo_path = "/content/typo_cleaning.csv"
else:
    typo_path = None

if typo_path:
    df_typo = pd.read_csv(typo_path, sep=';', encoding='utf-8', on_bad_lines='skip')
    typo_dict = dict(zip(df_typo["word"].str.lower(), df_typo["clean"].str.lower()))
    print(f"🧹 Loaded {len(typo_dict)} typo mappings dari {typo_path}")
else:
    print("⚠️ File typo_cleaning.csv tidak ditemukan, lanjut tanpa typo cleaning.")
    typo_dict = {}

# === 6️⃣ Fungsi: Cleaning lengkap ===
def full_clean_text(text):
    text = remove_emojis(text)
    text = clean_special_chars(text)
    words = text.split()
    words = [typo_dict.get(w, w) for w in words]
    return " ".join(words)

# === 7️⃣ Baca data dari sheet ===
def read_sheet(sheet_name):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df["Apps"] = sheet_name
        for col in ["title", "Detail"]:
            if col in df.columns:
                df[col] = df[col].apply(full_clean_text)
        print(f"✅ {sheet_name} dibaca: {len(df)} baris (cleaned & lowercased).")
        return df
    except Exception as e:
        print(f"⚠️ Gagal membaca {sheet_name}: {e}")
        return pd.DataFrame()

# === 8️⃣ Normalisasi datetime ke WIB ===
def normalize_to_wib(series):
    tz_wib = pytz.timezone("Asia/Jakarta")
    out = []
    for val in series:
        if pd.isna(val) or str(val).strip() == "":
            out.append("")
            continue
        try:
            dt = pd.to_datetime(val, errors="coerce")
            if pd.isna(dt):
                out.append("")
                continue
            if dt.tzinfo is None:
                dt = dt.tz_localize("UTC")
            dt_wib = dt.tz_convert(tz_wib)
            out.append(dt_wib.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            out.append("")
    return out

# === 9️⃣ Urutan kolom ===
DESIRED_ORDER = [
    "reviewId", "Date", "Rating", "Username", "appVersion", "title", "Detail",
    "repliedAt", "replyContent", "reviewCreatedVersion", "thumbsUpCount",
    "userImage", "Apps"
]
DESIRED_ORDER_LOWER = [c.lower() for c in DESIRED_ORDER]

# === 🔟 Append tanpa duplikat ===
def append_no_duplicates_to_data_review(new_df, key_column="reviewid"):
    sheet_name = "Data Review"
    new_df.columns = new_df.columns.str.strip().str.lower()
    try:
        ws_data = spreadsheet.worksheet(sheet_name)
        values = ws_data.get_all_values()
        if len(values) > 1:
            header = [h.strip().lower() for h in values[0]]
            rows = values[1:]
            df_existing = pd.DataFrame(rows, columns=header)
        else:
            df_existing = pd.DataFrame(columns=DESIRED_ORDER_LOWER)
            ws_data.update([DESIRED_ORDER])
            print("📋 Header di 'Data Review' diperbarui.")
    except gspread.WorksheetNotFound:
        ws_data = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
        ws_data.update([DESIRED_ORDER])
        df_existing = pd.DataFrame(columns=DESIRED_ORDER_LOWER)
        print("🆕 Sheet 'Data Review' baru dibuat dengan header yang benar.")

    df_existing.columns = df_existing.columns.str.strip().str.lower()

    if key_column not in new_df.columns:
        print(f"❗ Kolom '{key_column}' tidak ditemukan.")
        return

    if key_column not in df_existing.columns:
        df_existing[key_column] = ""

    df_existing[key_column] = df_existing[key_column].astype(str)
    new_df[key_column] = new_df[key_column].astype(str)

    if df_existing.empty:
        new_unique = new_df.copy()
    else:
        new_unique = new_df[~new_df[key_column].isin(df_existing[key_column])]

    new_unique = new_unique[~new_unique[key_column].eq("")]

    if new_unique.empty:
        print("ℹ️ Tidak ada data baru untuk ditambahkan.")
        return

    print(f"📤 Menambahkan {len(new_unique)} data baru ke 'Data Review'...")
    ws_data.append_rows(new_unique.astype(str).values.tolist(), value_input_option="USER_ENTERED")
    print(f"✅ {len(new_unique)} baris baru berhasil ditambahkan.")

# === 11️⃣ Eksekusi utama ===
df_play = read_sheet("Google Play")
df_app = read_sheet("Apps Store")

if df_play.empty and df_app.empty:
    print("❌ Tidak ada data untuk digabungkan.")
else:
    df_all = pd.concat([df_play, df_app], ignore_index=True)

    # Normalisasi datetime ke WIB
    for col in ["Date", "repliedAt"]:
        if col.lower() in df_all.columns:
            df_all[col] = normalize_to_wib(df_all[col])

    df_all.columns = df_all.columns.str.strip().str.lower()
    for col in DESIRED_ORDER_LOWER:
        if col not in df_all.columns:
            df_all[col] = ""

    df_all = df_all[DESIRED_ORDER_LOWER]
    df_all.loc[df_all["apps"].str.lower() == "google play", "title"] = ""

    df_all = df_all.map(lambda x: "" if (pd.isna(x) or x is None or str(x).lower() in ["nan", "none", "nat"]) else x)

    df_all["reviewid"] = df_all["reviewid"].astype(str)

    if "date" in df_all.columns:
        df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
        df_all = df_all.sort_values("date", ascending=False, na_position="last")
        df_all["date"] = df_all["date"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

    print(f"📊 Total data gabungan: {len(df_all)} baris.")
    append_no_duplicates_to_data_review(df_all)

print(f"✅ Merge & cleaning selesai pada {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} WIB")
