# from vnstock import Vnstock
# import pandas as pd
# import os

# def save_history_to_csv(symbol, start, end):
#     vn = Vnstock().stock(symbol=symbol, source="VCI")
#     df = vn.quote.history(start=start, end=end, interval="1D")
    
#     directory = "stock_history"
#     if not os.path.exists(directory):
#         os.makedirs(directory)
    
#     filename = f"{directory}/{symbol}_history.csv"
#     df.to_csv(filename, index=False)
#     print(f"Saved history for {symbol} to {filename}")

# symbols = ["ACB", "VCB", "VNM"]  # danh sách bạn muốn xử lý

# for sym in symbols:
#     save_history_to_csv(sym, "2024-01-01", "2024-06-30")


# import vnstock

# stock = vnstock.Vnstock().stock("FPT", "VCI")
# df = stock.quote.history(start="2024-01-01")
# print(df)

# stock.finance.income_statement(period="year")
# print(stock.finance.balance_sheet(period="year"))

# symbols = vnstock.Vnstock().listing.symbols()
# print(symbols)

import vnstock, pkgutil, inspect

import vnstock as pkg

for loader, module_name, is_pkg in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
    try:
        module = __import__(module_name, fromlist=["dummy"])
        funcs = [
            f for f, o in inspect.getmembers(module, inspect.isfunction)
            if not f.startswith("_")
        ]
        if funcs:
            print(f"=== {module_name} ===")
            print(funcs)
    except Exception:
        pass




