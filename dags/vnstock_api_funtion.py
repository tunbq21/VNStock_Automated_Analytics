from vnstock import Vnstock

stock_api = Vnstock()

df = stock_api.stock("FPT", "VCI").trading.price_board(
    symbols_list=["FPT", "HPG", "VCB"]
)

print(df.columns)