import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import mysql.connector
from prophet import Prophet
import json

# DB 연결
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="0011",
    database="dashboard_db"
)

# 데이터 가져오기
query = "SELECT date, amount FROM products ORDER BY date ASC"
df = pd.read_sql(query, conn)

# Prophet 예측
df = df.rename(columns={"date": "ds", "amount": "y"})
model = Prophet()
model.fit(df)

future = model.make_future_dataframe(periods=7)
forecast = model.predict(future)

output = forecast[['ds', 'yhat']].tail(7)
result = {
    "forecast": [
        {
            "date": row["ds"].strftime("%Y-%m-%d"),
            "predicted": round(row["yhat"])
        }
        for _, row in output.iterrows()
    ]
}

# ⚠️ 오직 이 줄만 출력되도록!
print(json.dumps(result, ensure_ascii=False))

conn.close()
