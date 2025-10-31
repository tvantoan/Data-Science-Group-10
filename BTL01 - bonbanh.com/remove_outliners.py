import numpy as np
import pandas as pd

# --------------------------
# ⚙️ Cấu hình ban đầu
# --------------------------
INPUT_FILE = "cars_ready.csv"
OUTPUT_FILE = "cars_cleaned.csv"
METHOD = "iqr"  # "iqr" hoặc "zscore"

# --------------------------
# 1️⃣ Đọc dữ liệu
# --------------------------
df = pd.read_csv(INPUT_FILE)
print(f"📂 Đọc thành công {len(df)} dòng từ {INPUT_FILE}")

# --------------------------
# 2️⃣ Chọn các cột numeric để loại bỏ outlier
# --------------------------
numeric_cols = [
    "Giá thành",
    "Năm sản xuất",
    "Số Km đã đi",
    "Tuổi thọ",
    "Dung tích động cơ",
]

df_numeric = df[numeric_cols]

# --------------------------
# 3️⃣ Hàm loại bỏ ngoại lai
# --------------------------


def remove_outliers_iqr(data, factor=1.5):
    """Loại bỏ ngoại lai bằng IQR."""
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1
    mask = ~((data < (Q1 - factor * IQR)) | (data > (Q3 + factor * IQR))).any(axis=1)
    return mask


def remove_outliers_zscore(data, threshold=3):
    """Loại bỏ ngoại lai bằng Z-Score."""
    z = (data - data.mean()) / data.std(ddof=0)
    mask = (np.abs(z) < threshold).all(axis=1)
    return mask


# --------------------------
# 4️⃣ Áp dụng phương pháp
# --------------------------
if METHOD == "iqr":
    mask = remove_outliers_iqr(df_numeric)
    print("📊 Dùng phương pháp IQR (factor=1.5)")
elif METHOD == "zscore":
    mask = remove_outliers_zscore(df_numeric)
    print("📊 Dùng phương pháp Z-Score (threshold=3)")
else:
    raise ValueError("❌ METHOD phải là 'iqr' hoặc 'zscore'")

removed = len(df) - mask.sum()
print(f"🧹 Đã loại bỏ {removed} dòng ngoại lai ({removed/len(df)*100:.2f}%)")

# --------------------------
# 5️⃣ Lưu dữ liệu sạch
# --------------------------
df_cleaned = df[mask]
df_cleaned.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Đã lưu dữ liệu sạch: {OUTPUT_FILE} ({len(df_cleaned)} dòng còn lại)")

# --------------------------
# 6️⃣ Báo cáo tóm tắt
# --------------------------
print("\n📈 Thống kê trước và sau khi làm sạch:")
summary_before = df_numeric.describe().T[["mean", "min", "max"]]
summary_after = df_cleaned[numeric_cols].describe().T[["mean", "min", "max"]]

report = pd.concat([summary_before, summary_after], axis=1)
report.columns = [
    "Mean (Before)",
    "Min (Before)",
    "Max (Before)",
    "Mean (After)",
    "Min (After)",
    "Max (After)",
]

print(report)
