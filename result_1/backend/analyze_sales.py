# -*- coding: utf-8 -*-
import pandas as pd
from prophet import Prophet
import re
import matplotlib
matplotlib.use('Agg')
import io
import sys
import json
import os
import requests
import mysql.connector
import chardet

# MySQL 연결
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='0000',
        database='dashboard_db'
    )

# 컬럼 추정 함수
def guess_column(candidates, options):
    for col in candidates:
        for opt in options:
            if re.search(opt, col, re.IGNORECASE):
                return col
    return None

COLUMN_ALIASES = {
    '판매일': '날짜',
    'date': '날짜',
    'Date': '날짜',
    '판매수량': '수량',
    'quantity': '수량',
    '매출': '매출액',
    'sales': '매출액',
    'amount': '매출액',
    'Amount': '매출액'
}

def normalize_keys(row):
    new_row = {}
    for key, value in row.items():
        normalized_key = COLUMN_ALIASES.get(key.strip(), key.strip())
        new_row[normalized_key] = value
    return new_row

# 환율 가져오기
def fetch_exchange_rate(base='USD', target='KRW'):
    test_mode = os.environ.get('TEST_MODE', 'false').lower() == 'true'
    if test_mode:
        fake_rate = 1300.0
        print("🔪 테스트 모드 확율 적용 ({fake_rate})", file=sys.stderr)
        return fake_rate
    api_key = "63fdc587845af979431c25fe"
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base}"
    try:
        res = requests.get(url, timeout=5)
        data = res.json()
        return data.get('conversion_rates', {}).get(target)
    except Exception as e:
        print(f"환율 API 오류: {e}", file=sys.stderr)
        return None
    
# DB 저장
def insert_forecast_to_db(forecast_dict, project_id=None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for date_str, amount in forecast_dict.items():
            raw_data = {
                "name": "예측데이터",
                "amount": amount,
                "date": date_str
            }
            print(f"📥 저장 대상: {raw_data}", file=sys.stderr)
            cursor.execute(
                "INSERT INTO products (project_id, raw_data) VALUES (%s, %s)",
                (project_id, json.dumps(raw_data, ensure_ascii=False))
            )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"DB 저장 오류: {e}", file=sys.stderr)

# 분석 함수
def analyze_combined_dataframe(df, project_id=None):
    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
    
    df['매출액'] = pd.to_numeric(df['매출액'], errors='coerce')
    print("📊 Prophet 입력용 데이터:", file=sys.stderr)
    print(df_parsed[['날짜', '매출액']].head(10), file=sys.stderr)

    df = df.dropna(subset=['날짜', '매출액'])
    if df.empty or len(df) < 10:
        raise ValueError("유효한 분석 데이터가 부족합니다.")

    total_sales = df['매출액'].sum()
    avg_sales = df['매출액'].mean()
    max_sales = df['매출액'].max()
    min_sales = df['매출액'].min()

    df_prophet = df[['날짜', '매출액']].rename(columns={'날짜': 'ds', '매출액': 'y'})
    df_prophet = df_prophet.groupby('ds', as_index=False).agg({'y': 'sum'}).sort_values('ds')
    df_prophet = df_prophet[(df_prophet['y'] >= 0) & (df_prophet['y'] <= 1e9)]  # 예측값 유효성 필터링
    if df_prophet.empty or len(df_prophet) < 10:
        raise ValueError("예측할 수 있는 유효한 데이터가 존재하지 않거나 너무 적습니다.")

    model = Prophet()
    model.fit(df_prophet)

    future = pd.date_range(start=df_prophet['ds'].max() + pd.Timedelta(days=1), periods=7)
    forecast = model.predict(pd.DataFrame({'ds': future}))

    forecast_dict = {
        str(row['ds'].date()): int(round(row['yhat'])) if pd.notna(row['yhat']) else 0
        for _, row in forecast[['ds', 'yhat']].iterrows()
    }
    insert_forecast_to_db(forecast_dict, project_id)

    return {
        'total_sales': int(total_sales),
        'avg_sales': int(avg_sales),
        'max_sales': int(max_sales),
        'min_sales': int(min_sales),
        'forecast_next_7_days': forecast_dict
    }

# 실행 진입점
if __name__ == '__main__':
    args = sys.argv[1:]
    project_id = None

    if '--project' in args:
        idx = args.index('--project')
        if idx + 1 < len(args):
            project_id = int(args[idx + 1])
            del args[idx:idx + 2]

    if project_id is not None:
        try:
            conn = get_db_connection()
            df = pd.read_sql("SELECT raw_data FROM products WHERE project_id = %s ORDER BY uploaded_at ASC", conn, params=(project_id,))
            conn.close()

            raw_parsed = [json.loads(item['raw_data']) for _, item in df.iterrows()]

            # ✅ 중복 및 파생 컬럼 제거
            cleaned_rows = []
            for row in raw_parsed:
                row = {k.strip().replace('\ufeff', ''): v for k, v in row.items() if k}
                for bad_key in ['filename', 'index', '_merge']:
                    row.pop(bad_key, None)
                row = {k: v for k, v in row.items() if not re.match(r"^_\\d+$|^Unnamed", k)}
                row = normalize_keys(row)
                cleaned_rows.append(row)

            df_parsed = pd.DataFrame(cleaned_rows)
            print(f"✅ 정제 후 컬럼: {df_parsed.columns.tolist()}", file=sys.stderr)
            print(f"✅ 샘플 5행:\n{df_parsed.head()}", file=sys.stderr)

            columns = df_parsed.columns.tolist()
            date_col = guess_column(columns, ["날짜", "date", "판매일", "Date"])
            amount_col = guess_column(columns, ["매출", "매출액", "금액", "sales", "amount", "Amount"])
            currency_col = guess_column(columns, ["통화", "currency", "Currency"])

            if not date_col or not amount_col:
                raise ValueError("날짜 또는 매출 컬럼을 찾을 수 없습니다.")
            
            rate = fetch_exchange_rate()
            if rate and currency_col in df_parsed.columns:
                df_parsed[currency_col] = df_parsed[currency_col].astype(str).str.upper()
                usd_mask = df_parsed[currency_col] == 'USD'
                df_parsed.loc[usd_mask, amount_col] = df_parsed.loc[usd_mask, amount_col].astype(float) * rate

            # 날짜 처리 및 보정
            if pd.api.types.is_numeric_dtype(df_parsed[date_col]):
                # 엑셀 숫자형 날짜 → 날짜로 변환
                df_parsed['날짜'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df_parsed[date_col], unit='D')
            else:
                df_parsed['날짜'] = pd.to_datetime(df_parsed[date_col], errors='coerce')
                if df_parsed['날짜'].isna().all() or (
                    df_parsed['날짜'].dt.year.nunique() == 1 and df_parsed['날짜'].dt.year.mode().iloc[0] == 1970
                ):
                    df_parsed['날짜'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df_parsed[date_col], unit='D')

            # 매출액 정제 (쉼표 제거)
            df_parsed[amount_col] = df_parsed[amount_col].astype(str).str.replace(",", "").str.strip()
            df_parsed['매출액'] = pd.to_numeric(df_parsed[amount_col], errors='coerce')
            if df_parsed['매출액'].isna().all():
                raise ValueError("매출액 데이터가 없습니다.")

            df_parsed = df_parsed.dropna(subset=['날짜', '매출액'])
            result = analyze_combined_dataframe(df_parsed, project_id)
            print(json.dumps(result, ensure_ascii=False), file=sys.stdout)
        except Exception as e:
            print(json.dumps({'error': f'분석 실패: {str(e)}'}, ensure_ascii=False), file=sys.stdout)
