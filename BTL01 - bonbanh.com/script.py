import pandas as pd

# Đọc file
df = pd.read_csv("./data_cleaned.csv")

numeric_cols = ["Năm sản xuất", "Số Km đã đi", "Tuổi thọ", "Dung tích động cơ"]

# Tạo DataFrame boolean
zero_mask = df[numeric_cols] == 0

# Stack để lấy (row, column) của các giá trị bằng 0
zero_positions = zero_mask.stack()[lambda x: x].index.tolist()

print(f"Số lượng giá trị 0: {len(zero_positions)}\n")
print("Dòng, Cột có giá trị = 0:")
for row_idx, col_name in zero_positions:
    print(f"  Dòng {row_idx}, Cột {col_name}")
