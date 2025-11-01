# mlp.py
"""
Script huấn luyện MLP để dự đoán giá xe (dữ liệu đã log cho Số Km và Giá thành).
- OneHot cho categorical (handle_unknown='ignore')
- Imputer + StandardScaler cho numeric
- Pipeline + ColumnTransformer
- RandomizedSearchCV để tìm cấu hình MLP tốt hơn
- Khi dự đoán, chuyển ngược log bằng np.expm1
"""

import os
import time

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

DATA_FILE = "data_cleaned.csv"
MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)

MODEL_FILE = os.path.join(MODEL_DIR, "mlp_car_price_pipeline.pkl")
SAMPLE_PRED_CSV = os.path.join(MODEL_DIR, "sample_predictions.csv")
TRAIN_LOG = os.path.join(MODEL_DIR, "training_log.txt")

RANDOM_STATE = 42


df = pd.read_csv(DATA_FILE)
print(f"Đã đọc {len(df)} mẫu từ '{DATA_FILE}'")

categorical_cols = [
    "Hãng xe",
    "Dòng xe",
    "Xuất xứ",
    "Kiểu dáng",
    "Hộp số",
    "Loại động cơ",
]
numeric_cols = ["Năm sản xuất", "Số Km đã đi", "Tuổi thọ", "Dung tích động cơ"]

missing_cols = set(categorical_cols + numeric_cols + ["Giá thành"]) - set(df.columns)
if missing_cols:
    raise ValueError(f"Thiếu cột trong dữ liệu: {missing_cols}")

X = df[categorical_cols + numeric_cols].copy()
y = df["Giá thành"].astype(float).copy()

numeric_transformer = Pipeline(
    steps=[
        ("imputer", SimpleImputer(strategy="mean")),
        ("scaler", StandardScaler()),
    ]
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
    early_stopping=True,
    validation_fraction=0.15,
    n_iter_no_change=40,
    tol=1e-5,
    max_iter=2000,
    verbose=False,
)

pipeline = Pipeline(
    steps=[
        ("preprocessor", preprocessor),
        ("regressor", mlp),
    ]
)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=RANDOM_STATE
)

print(f"Train: {len(X_train)} | Test: {len(X_test)}")

param_distributions = {
    "regressor__hidden_layer_sizes": [
        (128, 64),
        (256, 128),
        (256, 128, 64),
        (512, 256),
        (128, 64, 32),
    ],
    "regressor__alpha": [1e-5, 1e-4, 1e-3, 1e-2],
    "regressor__learning_rate_init": [1e-3, 5e-4, 1e-4],
    "regressor__activation": ["relu", "tanh"],
    "regressor__solver": ["adam"],
}

search = RandomizedSearchCV(
    pipeline,
    param_distributions=param_distributions,
    n_iter=12,
    scoring="neg_mean_absolute_error",
    n_jobs=-1,
    cv=3,
    random_state=RANDOM_STATE,
    verbose=2,
    return_train_score=True,
)


t0 = time.time()
print("Bắt đầu RandomizedSearchCV để tìm hyperparams tốt...")
search.fit(X_train, y_train)
t1 = time.time()
print(f"RandomizedSearchCV hoàn tất trong {t1 - t0:.1f}s")

best = search.best_estimator_
print("Best params:", search.best_params_)
print("Best CV score (neg MAE):", search.best_score_)


with open(TRAIN_LOG, "w", encoding="utf-8") as f:
    f.write(f"Best params: {search.best_params_}\n")
    f.write(f"Best CV score (neg MAE): {search.best_score_}\n")
    f.write("\nCV results (top 10):\n")
    cvres = search.cv_results_

    best_idx = np.argsort(cvres["rank_test_score"])[:10]
    for i in best_idx:
        f.write(
            f"rank={cvres['rank_test_score'][i]} mean_test={cvres['mean_test_score'][i]:.6f} params={cvres['params'][i]}\n"
        )
print(f"Đã lưu log huấn luyện tại: {TRAIN_LOG}")


y_pred_log = best.predict(X_test)
y_test_log = y_test.values


y_pred_real = np.expm1(y_pred_log)
y_test_real = np.expm1(y_test_log)

mae = mean_absolute_error(y_test_real, y_pred_real)
mse = mean_squared_error(y_test_real, y_pred_real)
r2 = r2_score(y_test_real, y_pred_real)

print("\n--- KẾT QUẢ TRÊN TEST SET (sau expm1) ---")
print(f"MAE: {mae:,.2f}")
print(f"MSE: {mse:,.2f}")
print(f"R2:  {r2:.4f}")


compare = pd.DataFrame(
    {
        "Giá thực tế": y_test_real[:50],
        "Giá dự đoán": y_pred_real[:50],
    }
).reset_index(drop=True)
compare.to_csv(SAMPLE_PRED_CSV, index=False)
print(f"Đã lưu mẫu dự đoán tại: {SAMPLE_PRED_CSV}")


joblib.dump(
    {"pipeline": best, "search_cv": search},
    MODEL_FILE,
)
print(f"Đã lưu model pipeline tại: {MODEL_FILE}")


def predict_prices_df(df_input, model_pipeline=best):
    """
    Nhận DataFrame có cùng cột X (categorical_cols + numeric_cols),
    trả về DataFrame có thêm 'Giá dự đoán (log)' và 'Giá dự đoán (real)'.
    """
    X_in = df_input[categorical_cols + numeric_cols].copy()
    y_log_pred = model_pipeline.predict(X_in)
    y_real_pred = np.expm1(y_log_pred)
    out = df_input.copy()
    out["Giá dự đoán (log)"] = y_log_pred
    out["Giá dự đoán (real)"] = y_real_pred
    return out


print("\nSo sánh 10 mẫu test đầu (giá thực):")
print(compare.head(10).to_string(index=False))
