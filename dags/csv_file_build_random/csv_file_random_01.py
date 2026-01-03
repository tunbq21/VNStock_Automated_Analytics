import pandas as pd
import random
import numpy as np


# 1. Bảng Products (Thông tin sản phẩm)
products = pd.DataFrame({
    'product_id': ['P001', 'P002', 'P003', 'P004', 'P005'],
    'product_name': ['iPhone 15', 'MacBook M3', 'AirPods Pro', 'iPad Air', 'Apple Watch'],
    'category': ['Electronics', 'Electronics', 'Accessories', 'Electronics', 'Accessories'],
    'cost_price': [800, 1500, 150, 500, 300]
})



# 2. Bảng Sales (Giao dịch - 50,000 dòng với các lỗi thực tế)
n_sales = 50000
sales = pd.DataFrame({
    'order_id': range(1000, 1000 + n_sales),
    'product_id': np.random.choice(['P001', 'P002', 'P003', 'P004', 'P005', 'P999'], n_sales), # P999 là mã lỗi
    'customer_id': np.random.randint(5000, 6000, n_sales),
    'quantity': np.random.randint(1, 5, n_sales),
    'sale_price': np.random.uniform(200, 2000, n_sales),
    'order_date': np.random.choice(pd.date_range('2024-01-01', '2024-12-31'), n_sales),
    'store_location': np.random.choice(['Hanoi', 'HCM', 'Danang', None], n_sales) # Có giá trị NULL
})



# 3. Bảng Customers (Thông tin khách hàng)
customers = pd.DataFrame({
    'customer_id': range(5000, 6000),
    'tier': np.random.choice(['Gold', 'Silver', 'Bronze'], 1000)
})

products.to_csv("dim_products.csv", index=False)
sales.to_csv("fact_sales.csv", index=False)
customers.to_csv("dim_customers.csv", index=False)
print("Đã tạo xong bộ dữ liệu chuyên nghiệp!")