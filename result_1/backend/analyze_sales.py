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

#MySQL 연결
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='0000',
        database='dashboard_db'
    )

#컬럼 추정 함수
def guess_column(candidates, options):
    for col in candidates:
        for opt in options:
            if re.search(opt, col, re.IGNORECASE):
                return col
    return None

def fetch_exchange_rate(base='USD', target='KRW'):
    api_key = "63fdc587845af979431c25fe"
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/{base}"

    try:
        res = requests.get(url, timeout=5)  # ✅ timeout 추가!
        data = res.json()
        rate = data.get('conversion_rates', {}).get(target)

        if not rate:
            print(f"⚠️ 환율 정보 없음: {data}", file=sys.stderr)
            return None

        return rate
    except Exception as e:
        print(f"❌ 환율 API 오류: {e}", file=sys.stderr)
        return None
    
#인코딩 감지 함수
def detect_encoding(file_stream):
    rawdata = file_stream.read()
    result = chardet.detect(rawdata)
    encoding = result['encoding'] or 'utf-8'
    print(f"📡 감지된 인코딩: {encoding}", file=sys.stderr)
    file_stream.seek(0)
    return encoding

#파일에서 DataFrame 읽기
def read_file_as_dataframe(file_stream, filename):
    ext = os.path.splitext(filename)[-1].lower()
    try:
        if ext in ['.xls', '.xlsx']:
            file_stream.seek(0)
            df = pd.read_excel(file_stream)
        elif ext == '.csv':
            encoding = detect_encoding(file_stream)
            file_stream.seek(0)
            df = pd.read_csv(file_stream, encoding=encoding)
        else:
            return None, f"{filename}: 지원하지 않는 파일 형식입니다. (csv, xlsx만 가능)"

        df.columns = [str(col) for col in df.columns]
        df.columns = df.columns.str.strip().str.replace('\ufeff', '')

        if any("�" in col for col in df.columns):
            print(f"⚠️ 인코딩 깨짐 감지됨 → 감지된 인코딩: {encoding}", file=sys.stderr)

        columns = df.columns.tolist()

        date_col = guess_column(columns, ["날짜", "날자", "date", "Date", "판매일", "판매일자", "작성일", "일자",
                                            "작성일자", '거래일', "거래일자", '구매일', "구매일자", '등록일', "등록일자",
                                            '주문일', "주문일자", '처리일', "처리일자", '결제일', "결제일자"])
        sales_col = guess_column(columns, ['매출', '매출액', 'sales', "Sales", 'revenue', 'Revenue', "금액",
                                            '판매금액', '총액', '수익', 'amount', "Amount", '실매출', '매출합계',
                                            '총매출', '결제금액', 'order total', 'total price', '거래금액'])

        if not date_col or not sales_col:
            print(f"❌ 컬럼 추론 실패 - columns: {columns}", file=sys.stderr)
            return None, f"{filename}: 날짜 또는 매출액 컬럼을 자동으로 찾을 수 없습니다."
        
        df.rename(columns={date_col: '날짜', sales_col: '매출액'}, inplace=True)
        df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
        df['매출액'] = pd.to_numeric(df['매출액'], errors='coerce')
        df = df.dropna(subset=['날짜', '매출액'])
        if df.empty:
            return None, f"{filename}: 유효한 데이터가 충분하지 않음"
        return df[['날짜', '매출액']], None
    except Exception as e:
        return None, f"{filename}: 파일을 읽는 중 오류 발생: {str(e)}"

# 📈 예측 결과를 DB에 저장
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
            cursor.execute(
                "INSERT INTO products (project_id, raw_data) VALUES (%s, %s)",
                (project_id, json.dumps(raw_data, ensure_ascii=False))
            )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"DB 저장 오류: {e}", file=sys.stderr)

# 📊 분석 메인 함수
def analyze_combined_dataframe(df, project_id=None):
    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
    df['매출액'] = pd.to_numeric(df['매출액'], errors='coerce')
    df = df.dropna(subset=['날짜', '매출액'])

    if df.empty or len(df) < 10:
        raise ValueError("유효한 분석 데이터가 부족합니다.")
    
    total_sales = df['매출액'].sum()
    avg_sales = df['매출액'].mean()
    max_sales = df['매출액'].max()
    min_sales = df['매출액'].min()

    df['z_score'] = (df['매출액'] - avg_sales) / df['매출액'].std()
    df['is_outlier'] = df['z_score'].abs() > 2

    df_prophet = df[['날짜', '매출액']].rename(columns={'날짜': 'ds', '매출액': 'y'})
    df_prophet['ds'] = pd.to_datetime(df_prophet['ds'], errors='coerce')
    df_prophet['y'] = pd.to_numeric(df_prophet['y'], errors='coerce')
    df_prophet = df_prophet.dropna()

    # ✅ 날짜 기준 groupby
    df_prophet = df_prophet.groupby('ds', as_index=False).agg({'y': 'sum'})
    df_prophet = df_prophet.sort_values('ds')

    if df_prophet.empty or len(df_prophet) < 10:
        raise ValueError("예측할 수 있는 유효한 데이터가 존재하지 않거나 너무 적습니다.")

    try:
        model = Prophet()
        model.fit(df_prophet)
        print("📈 Prophet 학습 완료", file=sys.stderr)
    except Exception as e:
        print(f"❌ Prophet 학습 실패: {e}", file=sys.stderr)
        raise

    last_date = df_prophet['ds'].max()
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=7)
    future = pd.DataFrame({'ds': future_dates})
    forecast = model.predict(future)
    forecast_dict = {}

    for _, row in forecast[['ds', 'yhat']].iterrows():
        date = row['ds'].date()
        value = row['yhat']
        if pd.isna(value) or not pd.api.types.is_number(value) or value < 0 or value > 1e9:
            value = 0
        forecast_dict[str(date)] = int(round(value))

    insert_forecast_to_db(forecast_dict, project_id)
    print("🧾 예측 결과 forecast_dict:", file=sys.stderr) 
    print(forecast_dict, file=sys.stderr)

    return {
        'total_sales': int(total_sales),
        'avg_sales': int(avg_sales),
        'max_sales': int(max_sales),
        'min_sales': int(min_sales),
        'forecast_next_7_days': forecast_dict
    }

def is_valid_dataframe(df):
    if df is None or df.empty:
        return False
    if '날짜' not in df.columns or '매출액' not in df.columns:
        return False
    if df['날짜'].isna().all() or df['매출액'].isna().all():
        return False
    return True

# 🚀 CLI 진입점
if __name__ == '__main__':
    args = sys.argv[1:]
    project_id = None

    if '--project' in args:
        idx = args.index('--project')
        if idx + 1 < len(args):
            project_id = int(args[idx + 1])
            del args[idx:idx+2]

    if args:
        input_paths = args
        combined_df = []
        errors = []

        for path in input_paths:
            with open(path, 'rb') as f:
                file_stream = io.BytesIO(f.read())
                df, err = read_file_as_dataframe(file_stream, os.path.basename(path))
                if df is not None:
                    df['날짜'] = pd.to_datetime(df['날짜'], errors='coerce')
                    df['매출액'] = pd.to_numeric(df['매출액'], errors='coerce')
                    df = df.dropna(subset=['날짜', '매출액'])
                    combined_df.append(df)
                else:
                    errors.append({'filename': os.path.basename(path), 'error': err})

        if not combined_df:
            print(json.dumps({'error': '모든 파일이 분석 불가', 'details': errors}, ensure_ascii=False), file=sys.stdout)
        else:
            merged_df = pd.concat(combined_df, ignore_index=True)
            try:
                result = analyze_combined_dataframe(merged_df, project_id)
                print(json.dumps(result, ensure_ascii=False), file=sys.stdout)
            except Exception as e:
                print(json.dumps({'error': str(e)}, ensure_ascii=False), file=sys.stdout)

    elif project_id is not None:
        try:
            conn = get_db_connection()
            query = "SELECT raw_data FROM products WHERE project_id = %s ORDER BY uploaded_at ASC"
            df = pd.read_sql(query, conn, params=(project_id,))
            conn.close()

            raw_parsed = [json.loads(item['raw_data']) for _, item in df.iterrows()]

            df_parsed = pd.DataFrame(raw_parsed)
            df_parsed.columns = df_parsed.columns.str.strip().str.replace('\ufeff', '') 
            df_parsed = df_parsed.loc[:, ~df_parsed.columns.duplicated()]
            print(f"📋 df_parsed.columns = {df_parsed.columns.tolist()}", file=sys.stderr)
            print(f"📋 df_parsed 샘플 5행:\n{df_parsed.head()}", file=sys.stderr)

            if 'name' in df_parsed.columns:
                df_parsed = df_parsed[df_parsed['name'] != '예측데이터']
                print("✅ '예측데이터' 행 제거 완료", file=sys.stderr)

            df_parsed.columns = df_parsed.columns.str.strip().str.replace('\ufeff', '')
            if any("�" in col for col in df_parsed.columns):
                print(f"⚠️ DB 기반 분석에서 깨진 컬럼 감지됨 → {df_parsed.columns.tolist()}", file=sys.stderr)
            columns = df_parsed.columns.tolist()

            date_col = guess_column(columns, ["날짜", "날자", "date", "Date", "판매일", "판매일자", "작성일", "일자",
                                               "작성일자", '거래일', "거래일자", '구매일', "구매일자", '등록일', "등록일자",
                                               '주문일', "주문일자", '처리일', "처리일자", '결제일', "결제일자"])
            sales_col = guess_column(columns, ['매출', '매출액', 'sales', "Sales", 'revenue', 'Revenue', "금액",
                                               '판매금액', '총액', '수익', 'amount', "Amount", '실매출', '매출합계',
                                               '총매출', '결제금액', 'order total', 'total price', '거래금액'])
            currency_col = guess_column(columns, ['통화', 'currency', 'Currency', '화폐', '화폐단위'])
            
            print(f"✅ STEP 4: 추론된 date_col = {date_col}, sales_col = {sales_col}", file=sys.stderr)

            if not date_col or not sales_col:
                raise ValueError("날짜 또는 매출 컬럼을 찾을 수 없습니다.")

            rate = fetch_exchange_rate()
                
            df_parsed['날짜'] = pd.to_datetime(df_parsed[date_col], errors='coerce')
            # 날짜가 전부 1970년이거나, to_datetime 했는데 값이 이상하면 처리
            try_excel_numeric = False

            # 1. 전부 NaT거나
            if df_parsed['날짜'].isna().all():
                try_excel_numeric = True

            # 2. 날짜가 거의 다 1970년으로 나오는 경우
            elif df_parsed['날짜'].dt.year.nunique() == 1 and df_parsed['날짜'].dt.year.mode().iloc[0] == 1970:
                try_excel_numeric = True

            # 3. 타입이 전부 float/int이면 가능성 있음
            elif df_parsed[date_col].apply(lambda x: isinstance(x, (int, float))).all():
                try_excel_numeric = True

            if try_excel_numeric:
                df_parsed['날짜'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df_parsed[date_col], unit='D')
            
            df_parsed['매출액'] = pd.to_numeric(df_parsed[sales_col], errors='coerce')

            if rate:
                if currency_col and currency_col in df_parsed.columns:
                    df_parsed[currency_col] = df_parsed[currency_col].astype(str).str.upper()
                    usd_mask = df_parsed[currency_col] == 'USD'
                    df_parsed.loc[usd_mask, '매출액'] = df_parsed.loc[usd_mask, '매출액'] * rate
                    print(f"📈 환율 적용 완료: 1 USD = {rate} KRW", file=sys.stderr)
                else:
                    print("⚠️ 환율 정보를 불러오지 못해 환율 적용을 건너뜁니다.", file=sys.stderr)

            df_parsed = df_parsed.dropna(subset=['날짜', '매출액'])

            result = analyze_combined_dataframe(df_parsed, project_id)
            print(json.dumps(result, ensure_ascii=False), file=sys.stdout)
       
        except Exception as e:
            print(json.dumps({'error': f'프로젝트 기반 DB 분석 중 오류: {str(e)}'}, ensure_ascii=False), file=sys.stdout)
