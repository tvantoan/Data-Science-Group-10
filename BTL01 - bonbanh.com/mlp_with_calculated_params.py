import os
import time

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
    root_mean_squared_error,
)
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

DATA_FILE = "data_cleaned.csv"
MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)
MODEL_FILE = os.path.join(MODEL_DIR, "mlp_car_price_pipeline.pkl")
TRAIN_LOG = os.path.join(MODEL_DIR, "training_log.txt")

RANDOM_STATE = 42

df = pd.read_csv(DATA_FILE)

categorical_cols = [
    "Hãng xe",
    "Dòng xe",
    "Xuất xứ",
    "Kiểu dáng",
    "Hộp số",
    "Loại động cơ",
]

numeric_cols = [
    "Năm sản xuất",
    "Số Km đã đi",
    "Số ngày bài đã đăng",
    "Dung tích động cơ",
]

X = df[categorical_cols + numeric_cols].copy()
y = df["Giá thành"].astype(float).copy()

numeric_transformer = Pipeline(
    steps=[("imputer", SimpleImputer(strategy="mean")), ("scaler", StandardScaler())]
)

categorical_transformer = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value="__missing__")),
        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
    ]
)

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, numeric_cols),
        ("cat", categorical_transformer, categorical_cols),
    ],
    remainder="drop",
    n_jobs=-1,
)

mlp = MLPRegressor(
    random_state=RANDOM_STATE,
    solver="adam",
    learning_rate_init=0.0001,
    hidden_layer_sizes=(128, 64),
    alpha=0.01,
    activation="tanh",
    early_stopping=True,
    validation_fraction=0.15,
    n_iter_no_change=40,
    tol=1e-5,
    max_iter=2000,
    verbose=False,
)

pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("regressor", mlp)])

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=RANDOM_STATE
)

test_df = X_test.copy()
test_df["Giá thực tế"] = y_test.values
test_df.to_csv("test_set.csv", index=False, encoding="utf-8")

y_train = np.log1p(y_train)
y_test = np.log1p(y_test)

t0 = time.time()
pipeline.fit(X_train, y_train)
t1 = time.time()
y_train_pred_log = pipeline.predict(X_train)
y_train_pred_real = np.expm1(y_train_pred_log)
y_train_real = np.expm1(y_train.values)

mae_train = mean_absolute_error(y_train_real, y_train_pred_real)
mse_train = mean_squared_error(y_train_real, y_train_pred_real)
r2_train = r2_score(y_train_real, y_train_pred_real)
rmse_train = root_mean_squared_error(y_train_real, y_train_pred_real)
mape_train = mean_absolute_percentage_error(y_train_real, y_train_pred_real)


y_test_pred_log = pipeline.predict(X_test)
y_test_log = y_test.values

y_pred_real = np.expm1(y_test_pred_log)
y_test_real = np.expm1(y_test_log)

mae_test = mean_absolute_error(y_test_real, y_pred_real)
mse_test = mean_squared_error(y_test_real, y_pred_real)
r2_test = r2_score(y_test_real, y_pred_real)
rmse_test = root_mean_squared_error(y_test_real, y_pred_real)
mape_test = mean_absolute_percentage_error(y_test_real, y_pred_real)

joblib.dump({"pipeline": pipeline}, MODEL_FILE)

with open(TRAIN_LOG, "w", encoding="utf-8") as f:
    f.write("=== Train metrics ===\n")
    f.write(f"MAE : {mae_train:,.2f}\n")
    f.write(f"MSE: {mse_train:,.2f}\n")
    f.write(f"R2: {r2_train:.4f}\n")
    f.write(f"RMSE: {rmse_train:,.2f}\n")
    f.write(f"MAPE: {mape_train:.4f}\n\n")

    f.write("=== Test metrics ===\n")
    f.write(f"MAE : {mae_test:,.2f}\n")
    f.write(f"MSE: {mse_test:,.2f}\n")
    f.write(f"R2: {r2_test:.4f}\n")
    f.write(f"RMSE: {rmse_test:,.2f}\n")
    f.write(f"MAPE: {mape_test:.4f}\n")
    f.write(f"\nTrain time: {t1 - t0:.1f}s\n")

compare = pd.DataFrame({"Giá thực tế": y_test_real, "Giá dự đoán": y_pred_real})

plt.figure(figsize=(8, 8))
plt.scatter(y_test_real, y_pred_real, alpha=0.5)
plt.plot(
    [y_test_real.min(), y_test_real.max()],
    [y_test_real.min(), y_test_real.max()],
    "r--",
    lw=2,
)
plt.xlabel("Giá thực tế")
plt.ylabel("Giá dự đoán")
plt.title("So sánh giá thực tế và giá dự đoán")
plt.grid(True)
plt.savefig("real_vs_predicted.png")
plt.close()

errors = y_pred_real - y_test_real
plt.figure(figsize=(10, 6))
plt.hist(errors, bins=50, edgecolor="black")
plt.title("Phân phối lỗi dự đoán (dự đoán - thực tế)")
plt.xlabel("Lỗi")
plt.ylabel("Số lượng")
plt.grid(True)
plt.savefig("prediction_errors.png")
plt.close()

plt.figure(figsize=(10, 6))
plt.hist(y_test_real, bins=50, alpha=0.5, label="Giá thực tế")
plt.hist(y_pred_real, bins=50, alpha=0.5, label="Giá dự đoán")
plt.title("Phân phối giá thực tế vs giá dự đoán")
plt.xlabel("Giá xe")
plt.ylabel("Số lượng")
plt.legend()
plt.savefig("price_distribution.png")
plt.close()
