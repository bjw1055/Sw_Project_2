# -*- coding: utf-8 -*-
"""analyze_sales.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1Dhpx3kA46a5TXmmaREQz1vmC2248P77j
"""
# -*- coding: utf-8 -*-
import pandas as pd
from prophet import Prophet
import re
import matplotlib
matplotlib.use('Agg')  #서버 환경용 백엔드
import io
import sys
import json
import os
import mimetypes

def guess_column(candidates, options):
    for col in candidates:
        for opt in options:
            if re.search(opt, col, re.IGNORECASE):
                return col
    return None

def read_file_as_dataframe(file_stream, filename):
    ext = os.path.splitext(filename)[-1].lower()
    if not ext or len(ext) < 2:
        guessed_type, _ = mimetypes.guess_type(filename)
        if guessed_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            ext = '.xlsx'
        elif guessed_type == 'application/vnd.ms-excel':
            ext = '.xls'
        elif guessed_type == 'text/csv':
            ext = '.csv'
        elif hasattr(file_stream, 'name'):
            ext = os.path.splitext(file_stream.name)[-1].lower()

    try:
        if ext in ['.xls', '.xlsx']:
            file_stream.seek(0)
            df = pd.read_excel(file_stream)
        elif ext == '.csv':
            try:
                file_stream.seek(0)
                df = pd.read_csv(file_stream, encoding='utf-8')
            except UnicodeDecodeError:
                file_stream.seek(0)
                df = pd.read_csv(file_stream, encoding='cp949')
        else:
            return None, f"{filename}: 지원하지 않는 파일 형식입니다. (csv, xlsx만 가능)"
    except Exception as e:
        return None, f"{filename}: 파일을 읽는 중 오류 발생: {str(e)}"

    columns = df.columns.tolist()
    date_col = guess_column(columns, ['날짜', 'date', '판매일'])
    sales_col = guess_column(columns, ['매출', 'sales', 'revenue'])

    if not date_col or not sales_col:
        return None, f"{filename}: 날짜 또는 매출액 컬럼을 자동으로 찾을 수 없습니다."

    df['날짜'] = pd.to_datetime(df[date_col])
    df['매출액'] = df[sales_col]
    return df[['날짜', '매출액']], None

def analyze_combined_dataframe(df):
    df = df.sort_values('날짜')

    total_sales = df['매출액'].sum()
    avg_sales = df['매출액'].mean()
    max_sales = df['매출액'].max()
    min_sales = df['매출액'].min()

    df['z_score'] = (df['매출액'] - df['매출액'].mean()) / df['매출액'].std()
    df['is_outlier'] = df['z_score'].abs() > 2

    df_prophet = df[['날짜', '매출액']].rename(columns={'날짜': 'ds', '매출액': 'y'})
    model = Prophet()
    model.fit(df_prophet)
    future = model.make_future_dataframe(periods=7)
    forecast = model.predict(future)
    future_forecast = forecast[['ds', 'yhat']].tail(7)

    forecast_dict = {
        str(row['ds'].date()): int(round(row['yhat']))
        for _, row in future_forecast.iterrows()
    }

    return {
        'total_sales': int(total_sales),
        'avg_sales': int(avg_sales),
        'max_sales': int(max_sales),
        'min_sales': int(min_sales),
        'forecast_next_7_days': forecast_dict
    }

#CLI
if __name__ == '__main__':
    input_paths = sys.argv[1 : len(sys.argv)//2 + 1]
    input_filenames = sys.argv[len(sys.argv)//2 + 1 :]

    combined_df = []
    errors = []

    for i, path in enumerate(input_paths):
        filename = input_filenames[i] if i < len(input_filenames) else os.path.basename(path)
        with open(path, 'rb') as f:
            file_stream = io.BytesIO(f.read())
            df, err = read_file_as_dataframe(file_stream, filename)
            if df is not None:
                combined_df.append(df)
            else:
                errors.append({'filename': filename, 'error': err})

    if not combined_df:
        output = {
            'error': '업로드된 파일에서 분석 가능한 데이터가 없습니다.',
            'details': errors
        }
        print(json.dumps(output, ensure_ascii=False))
    else:
        merged_df = pd.concat(combined_df, ignore_index=True)
        result = analyze_combined_dataframe(merged_df)
        print(json.dumps(result, ensure_ascii=False))
