from vnstock import Vnstock
import pandas as pd
import os

# stock_api = Vnstock()

# df = stock_api.stock("FPT", "VCI").trading.price_board(
#     symbols_list=["FPT", "HPG", "VCB"]
# )

# print(df.columns)

curent_file = os.path.basename(__file__)
print(f"Đây là tên file: {curent_file}")


curent_file_link=os.path.abspath(__file__)
print(f"Đây là đường dẫn đầy đủ của file: {curent_file_link}")

curent_file_link =os.getcwd()
print(f"Đây là thư mục hiện tại của file: {curent_file_link}")