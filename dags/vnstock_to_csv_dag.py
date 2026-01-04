from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock 
import os

# --- 1. Cấu hình DAG và Biến Cục bộ ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['tbuiquang103@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


TICKERS = ['FPT', 'HPG', 'VCB', 'GAS', 'VNM', 'MSN', 'MWG', 'VPB', 'TCB', 'ACB'] 

BASE_DATA_PATH = '/usr/local/airflow/dags/data_lake/vnstock_prices_csv' 

with DAG(
    'vnstock_to_csv_etl',
    default_args=default_args,
    description='ETL giá chứng khoán VN30 từ vnstock và lưu vào CSV Data Lake',
    schedule='0 17 * * 1-5',  # Chạy lúc 17:00 từ T2 đến T6
    catchup=False,
    tags=['finance', 'vnstock', 'csv'],
) as dag:
    
    def extract_close_price(**kwargs):
        """Tải giá lịch sử (O, H, L, C, V) của ngày được chỉ định."""
        
        execution_date = kwargs['ds'] 
        data = []
        
        vnstock_api = Vnstock()
        
        for ticker in TICKERS:
            try:
                stock_obj = vnstock_api.stock(symbol=ticker, source="VCI")
                

                df = stock_obj.quote.history(
                    start=execution_date, 
                    end=execution_date, 
                    interval="1D"
                )
                
                if not df.empty:
                    
                    if 'time' in df.columns:
                        df.rename(columns={'time': 'Date'}, inplace=True)
                    elif 'TradingDate' in df.columns:
                        df.rename(columns={'TradingDate': 'Date'}, inplace=True)

                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                    
                    print(f"DEBUG: Các cột DF trước khi XCom: {df.columns.tolist()}") 
                    
                    # =================================================================
                    
                    latest_row = df.iloc[-1].to_dict() 
                    
                    latest_row['Ticker'] = ticker
                    if 'Date' not in latest_row:
                        latest_row['Date'] = execution_date 
                    
                    data.append(latest_row)
                    print(f" Tải thành công {ticker} cho ngày {execution_date}")
                else:
                    print(f"⚠️ Không có dữ liệu cho {ticker} vào ngày {execution_date}")
            except Exception as e:
                print(f" Lỗi khi tải {ticker}: {e}")
                
        final_df = pd.DataFrame(data)
        print(f"Tên cột DataFrame cuối cùng: {final_df.columns.tolist()}") 
        
        return final_df 
    
    extract_task = PythonOperator(
        task_id='extract_close_price',
        python_callable=extract_close_price,
        do_xcom_push=True,
    )


    def load_to_csv_data_lake(**kwargs):
        """Lấy dữ liệu từ XCom và lưu dưới dạng CSV phân vùng."""
        
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_close_price')
        
        if df is None or df.empty:
            print(" Không có dữ liệu để lưu trữ. Bỏ qua.")
            return

        date_column = 'Date' 
        
        df[date_column] = pd.to_datetime(df[date_column]) 
        
        df['year'] = df[date_column].dt.year
        df['month'] = df[date_column].dt.month
        
        rows_saved = 0
        
        # Phân vùng và lưu trữ
        for ticker in df['Ticker'].unique():
            df_ticker = df[df['Ticker'] == ticker]
            
            # Định dạng đường dẫn theo phân vùng (ticker/year/month/date.csv)
            date_str = df_ticker[date_column].iloc[0].strftime('%Y-%m-%d')
            year = df_ticker['year'].iloc[0]
            month = df_ticker['month'].iloc[0]

            save_dir = os.path.join(
                BASE_DATA_PATH, 
                ticker, 
                str(year), 
                str(month).zfill(2) 
            )
            save_path = os.path.join(save_dir, f"{date_str}.csv")
            
            # Tạo thư mục và Lưu trữ
            os.makedirs(save_dir, exist_ok=True)
            df_ticker.to_csv(save_path, index=False)
            print(f"💾 Lưu trữ thành công {ticker} tại {save_path}")
            rows_saved += len(df_ticker)
        
        return rows_saved

    load_task = PythonOperator(
        task_id='load_to_csv_data_lake',
        python_callable=load_to_csv_data_lake,
    )
    
    def data_quality_check(**kwargs):
        """Kiểm tra: Null, Giá trị dương, và số lượng dòng."""
        
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_close_price')
        rows_loaded = ti.xcom_pull(task_ids='load_to_csv_data_lake')
        
        if df is None or df.empty:
            print(" DQ Check bị bỏ qua: Dữ liệu trống.")
            return
        
        df.columns = [col.lower() for col in df.columns] 
        
        required_columns = ['close', 'open', 'high', 'low', 'volume']
        
        if df[required_columns].isnull().any().any():
            raise ValueError("DQ Check thất bại: Có giá trị Null trong các cột giá trị.")

        if (df['close'] <= 0).any():
            raise ValueError("DQ Check thất bại: Giá đóng cửa có giá trị <= 0.")
            
        if len(df) != rows_loaded:
            raise ValueError(f"DQ Check thất bại: Số dòng tải về ({len(df)}) khác số dòng đã lưu ({rows_loaded}).")
        
        print(f" Kiểm tra Chất lượng Dữ liệu thành công cho {len(df)} dòng dữ liệu.")

    dq_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )

    extract_task >> load_task >> dq_task