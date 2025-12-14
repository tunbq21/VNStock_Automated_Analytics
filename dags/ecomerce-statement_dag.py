from vnstock import Vnstock
import pandas as pd
import os

def save_history_to_csv(symbol, start, end):
    vn = Vnstock().stock(symbol=symbol, source="VCI")
    df = vn.quote.history(start=start, end=end, interval="1D")
    
    directory = "stock_history"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    filename = f"{directory}/{symbol}_history.csv"
    df.to_csv(filename, index=False)
    print(f"Saved history for {symbol} to {filename}")

symbols = ["ACB", "VCB", "VNM"]  # danh sách bạn muốn xử lý

for sym in symbols:
    save_history_to_csv(sym, "2024-01-01", "2024-06-30")


import vnstock

stock = vnstock.Vnstock().stock("FPT", "VCI")
df = stock.quote.history(start="2024-01-01")
print(df)

stock.finance.income_statement(period="year")
print(stock.finance.balance_sheet(period="year"))
print(symbols)


# import logging
# import pandas as pd
# import os
# from vnstock import Vnstock
# # Import Finance nếu bạn muốn kết hợp lấy Báo cáo Tài chính
# # from vnstock import Finance 

# # Thiết lập Logger chuẩn (quan trọng cho Airflow để ghi Log)
# logger = logging.getLogger(__name__)

# def fetch_and_save_historical_data(symbol: str, start_date: str, end_date: str, source: str = "VCI", interval: str = "1D", base_dir: str = "stock_data"):
#     """
#     Tải dữ liệu giá lịch sử từ vnstock và lưu vào tệp CSV.

#     Tham số:
#     - symbol (str): Mã cổ phiếu (ví dụ: 'FPT').
#     - start_date (str): Ngày bắt đầu (ví dụ: '2024-01-01').
#     - end_date (str): Ngày kết thúc (ví dụ: '2024-06-30').
#     - source (str): Nguồn dữ liệu (Mặc định: 'VCI').
#     - interval (str): Khoảng thời gian dữ liệu (Mặc định: '1D' - Hàng ngày).
#     - base_dir (str): Thư mục cơ sở để lưu trữ (Mặc định: 'stock_data').
#     """
    
#     logger.info(f"▶️ Bắt đầu tải dữ liệu lịch sử cho mã: {symbol} từ {start_date} đến {end_date} (Nguồn: {source}).")
    
#     # 1. Tải Dữ liệu (Extraction)
#     try:
#         vn = Vnstock().stock(symbol=symbol, source=source)
#         df = vn.quote.history(start=start_date, end=end_date, interval=interval)
        
#         if df.empty:
#             logger.warning(f"⚠️ Dữ liệu trả về trống cho mã {symbol}. Bỏ qua bước lưu trữ.")
#             return
            
#     except Exception as e:
#         logger.error(f"❌ Lỗi khi tải dữ liệu cho mã {symbol}: {e}")
#         # Trong môi trường Airflow, việc raise Exception sẽ đánh dấu Task là Failed
#         raise Exception(f"Task Failed: Không thể tải dữ liệu cho {symbol}")

#     # 2. Xử lý & Lưu trữ (Loading)
#     try:
#         # Tạo thư mục nếu chưa tồn tại
#         full_directory = os.path.join(base_dir, "historical_prices")
#         if not os.path.exists(full_directory):
#             os.makedirs(full_directory)
#             logger.info(f"Đã tạo thư mục lưu trữ: {full_directory}")
        
#         # Tạo tên tệp tin
#         filename = f"{symbol}_{start_date}_{end_date}.csv"
#         filepath = os.path.join(full_directory, filename)
        
#         # Lưu tệp CSV
#         df.to_csv(filepath, index=False)
        
#         logger.info(f"✅ Đã lưu thành công {len(df)} dòng dữ liệu cho mã {symbol} vào tệp: {filepath}")

#     except Exception as e:
#         logger.error(f"❌ Lỗi khi lưu dữ liệu cho mã {symbol}: {e}")
#         raise Exception(f"Task Failed: Không thể lưu dữ liệu cho {symbol}")

# # --- Ví dụ về cách gọi trong Airflow PythonOperator ---
# # Giả sử đây là Task Python trong DAG của bạn
# # task_id='fetch_fpt_data',
# # python_callable=fetch_and_save_historical_data,
# # op_kwargs={
# #     'symbol': 'FPT',
# #     'start_date': '2024-01-01',
# #     'end_date': '2024-06-30',
# #     'base_dir': '/opt/airflow/dags/data_lake'  # Đường dẫn tuyệt đối trong Airflow
# # }