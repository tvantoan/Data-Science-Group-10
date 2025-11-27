import joblib
import numpy as np
import pandas as pd

MODEL_PATH = "mlp_car_price_pipeline.pkl"
test_data = pd.read_csv("test_set.csv")

model_dict = joblib.load(MODEL_PATH)
model_pipeline = model_dict["pipeline"]

predicted_prices = np.expm1(model_pipeline.predict(test_data))

output_df = test_data.copy()
output_df["Giá dự đoán"] = predicted_prices

output_df.to_csv("predicted_car_prices.csv", index=False)
