# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re
import sys
import json
import os
import mysql.connector
from prophet import Prophet
import chardet

COLUMN_ALIASES = {
    '판매일': '날짜', 'date': '날짜', 'Date': '날짜',
    '판매수량': '수량', 'quantity': '수량',
    '매출': '매출액', 'sales': '매출액',
    'amount': '매출액', 'Amount': '매출액', 'Sales': '매출액'
}

def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='0000',
        database='dashboard_db'
    )

def guess_column(candidates, options):
    for col in candidates:
        for opt in options:
            if opt.lower() in col.lower():
                return col
    return None

def normalize_column_names(df):
    new_columns = []
    seen = set()
    for col in df.columns:
        base_col = col.strip().replace('\ufeff', '')
        mapped = COLUMN_ALIASES.get(base_col, base_col)
        if mapped in seen:
            new_columns.append(base_col)
        else:
            new_columns.append(mapped)
            seen.add(mapped)
    df.columns = new_columns
    return df

def remove_duplicate_columns(df):
    return df.loc[:, ~df.columns.duplicated()]

def decode_maybe(val):
    try:
        if isinstance(val, bytes):
            encoding = chardet.detect(val)['encoding'] or 'utf-8'
            return val.decode(encoding)
        return val
    except Exception:
        return val

def apply_exchange_rate(df):
    for col in df.columns:
        if col.strip().lower() in ["currency", "통화"]:
            df[col] = df[col].astype(str).str.upper()
            usd_mask = df[col] == "USD"
            if "Exchange Rate" in df.columns:
                df.loc[usd_mask, "매출액"] = (
                    pd.to_numeric(df.loc[usd_mask, "매출액"], errors="coerce") *
                    pd.to_numeric(df.loc[usd_mask, "Exchange Rate"], errors="coerce")
                )
            else:
                df.loc[usd_mask, "매출액"] = pd.to_numeric(df.loc[usd_mask, "매출액"], errors="coerce") * 1300
            print(f"✅ 환율 적용: {usd_mask.sum()}건 USD → KRW 변환", file=sys.stderr)
            break
    return df

def convert_mixed_date(val):
    try:
        if isinstance(val, (int, float)):
            if val > 30000:
                return pd.to_datetime('1899-12-30') + pd.to_timedelta(val, unit='D')
            else:
                return pd.to_datetime(val, errors='coerce')
        elif isinstance(val, str):
            v = val.strip()
            try:
                num = float(v)
                if num > 30000:
                    return pd.to_datetime('1899-12-30') + pd.to_timedelta(num, unit='D')
                else:
                    return pd.to_datetime(num, errors='coerce')
            except ValueError:
                cleaned = v.replace('.', '-').replace('/', '-')
                return pd.to_datetime(cleaned, errors='coerce')
        else:
            return pd.NaT
    except Exception:
        return pd.NaT

def analyze_combined_dataframe(df, project_id=None):
    print(f"📦 분석 시작 - 총 데이터 행 수: {len(df)}", file=sys.stderr)

    df = normalize_column_names(df)
    df = remove_duplicate_columns(df)
    for col in df.columns:
        df[col] = df[col].apply(decode_maybe)
    df = apply_exchange_rate(df)

    date_col = guess_column(df.columns, ["날짜", "date", "판매일", "Date"])
    amount_col = guess_column(df.columns, ["매출", "매출액", "금액", "sales", "amount", "Amount"])

    print("🧩 컬럼명 리스트:", df.columns.tolist(), file=sys.stderr)
    print("🧩 date_col:", date_col, "/ amount_col:", amount_col, file=sys.stderr)

    for col in df.columns:
        if col != date_col and col.lower() in ["판매일", "date", "날짜"]:
            df.drop(columns=[col], inplace=True)

    df['날짜'] = df[date_col].apply(convert_mixed_date)
    df[amount_col] = df[amount_col].astype(str).str.replace(",", "").str.strip()
    df['매출액'] = pd.to_numeric(df[amount_col], errors='coerce')

    print("📎 날짜 NaN:", df['날짜'].isna().sum(), file=sys.stderr)
    print("📎 매출액 NaN:", df['매출액'].isna().sum(), file=sys.stderr)
    print("📎 유효한 행 수:", df.dropna(subset=['날짜', '매출액']).shape[0], file=sys.stderr)

    df = df.dropna(subset=['날짜', '매출액'])
    if df.empty or len(df) < 10:
        raise ValueError("유효한 분석 데이터가 부족합니다.")

    df_prophet = df[['날짜', '매출액']].rename(columns={'날짜': 'ds', '매출액': 'y'})
    df_prophet = df_prophet.groupby('ds', as_index=False).agg({'y': 'sum'}).sort_values('ds')
    df_prophet = df_prophet[(df_prophet['y'] >= 0) & (df_prophet['y'] <= 1e9)]

    if df_prophet.empty or len(df_prophet) < 10:
        raise ValueError("예측할 수 있는 유효한 데이터가 너무 적습니다.")

    df_prophet['y_clipped'] = np.clip(df_prophet['y'], 0, 20000000)
    df_prophet['cap'] = 20000000
    df_prophet['floor'] = 0
    df_fit = df_prophet[['ds', 'y_clipped', 'cap', 'floor']].rename(columns={'y_clipped': 'y'})

    model = Prophet(growth='logistic')
    model.fit(df_fit)
    future = model.make_future_dataframe(periods=7)
    future['cap'] = 20000000
    future['floor'] = 0

    forecast = model.predict(future)
    forecast = forecast[forecast['ds'] > df_prophet['ds'].max()]

    forecast_dict = {
        str(row['ds'].date()): max(0, int(round(row['yhat']))) if pd.notna(row['yhat']) else 0
        for _, row in forecast[['ds', 'yhat']].iterrows()
    }

    result = {
        'total_sales': int(df['매출액'].sum()),
        'avg_sales': int(df['매출액'].mean()),
        'max_sales': int(df['매출액'].max()),
        'min_sales': int(df['매출액'].min()),
        'forecast_next_7_days': forecast_dict
    }

    if project_id is not None:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            for date_str, amount in forecast_dict.items():
                if not date_str:
                    continue
                raw_data = {"name": "예측데이터", "amount": amount, "date": date_str}
                cursor.execute(
                    "INSERT INTO products (project_id, raw_data) VALUES (%s, %s)",
                    (project_id, json.dumps(raw_data, ensure_ascii=False))
                )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"DB 저장 오류: {e}", file=sys.stderr)

    return result

if __name__ == '__main__':
    args = sys.argv[1:]
    project_id = None
    if '--project' in args:
        idx = args.index('--project')
        if idx + 1 < len(args):
            project_id = int(args[idx + 1])
            del args[idx:idx + 2]

    try:
        conn = get_db_connection()
        df = pd.read_sql("SELECT raw_data FROM products WHERE project_id = %s ORDER BY uploaded_at ASC", conn, params=(project_id,))
        conn.close()

        raw_parsed = []
        for _, item in df.iterrows():
            try:
                row = json.loads(item['raw_data'])
                cleaned = {k.strip().replace('\ufeff', ''): decode_maybe(v) for k, v in row.items() if k}
                for bad_key in ['filename', 'index', '_merge']:
                    cleaned.pop(bad_key, None)
                cleaned = {k: v for k, v in cleaned.items() if not re.match(r"^_\\d+$|^Unnamed", k)}
                raw_parsed.append(cleaned)
            except Exception as e:
                print(f"JSON 파싱 실패: {e}", file=sys.stderr)

        df_parsed = pd.DataFrame(raw_parsed)
        if '날짜' in df_parsed.columns:
            df_parsed['날짜'] = df_parsed['날짜'].astype(object)
        result = analyze_combined_dataframe(df_parsed, project_id)
        print(json.dumps(result, ensure_ascii=False), file=sys.stdout)
    except Exception as e:
        print(json.dumps({'error': f'분석 실패: {str(e)}'}), file=sys.stdout)

