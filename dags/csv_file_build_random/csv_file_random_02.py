import pandas as pd
import numpy as np
import uuid

def generate_full_10_tables():
    # Cấu hình số dòng cho các bảng lớn
    n_sales = 10000000    # 10M dòng
    n_logs = 20000000     # 20M dòng
    n_inventory = 5000000 # 5M dòng
    n_cust = 1000000      # 1M khách
    
    print("--- Đang tạo 10 bảng dữ liệu chuyên nghiệp... ---")
    
    # 1. dim_products (10,000 dòng)
    products = pd.DataFrame({
        'product_id': [f'P{i:05d}' for i in range(1, 10001)],
        'product_name': [f'Product_{i}' for i in range(1, 10001)],
        'brand': np.random.choice(['Apple', 'Samsung', 'Sony', 'Dell', 'HP', 'LG', 'Asus'], 10000),
        'category': np.random.choice(['Electronics', 'Home', 'Office', 'Accessories'], 10000),
        'sub_category': np.random.choice(['Phone', 'Laptop', 'Screen', 'Keyboard', 'Mouse'], 10000),
        'cost_price': np.random.uniform(50, 1500, 10000).round(2),
        'supplier_id': np.random.randint(1, 201, 10000),
        'weight_kg': np.random.uniform(0.1, 10.0, 10000).round(2),
        'color': np.random.choice(['Black', 'White', 'Silver', 'Gold'], 10000),
        'warranty_months': np.random.choice([12, 24, 36], 10000),
        'is_active': np.random.choice([True, False], 10000, p=[0.95, 0.05]),
        'created_at': pd.to_datetime('2023-01-01')
    })

    # 2. dim_customers (1M dòng)
    customers = pd.DataFrame({
        'customer_id': np.arange(1, n_cust + 1),
        'full_name': [f'Customer_{i}' for i in range(1, 1001)] * 1000, # Giảm thiểu tạo string để tránh treo máy
        'gender': np.random.choice(['M', 'F', 'O'], n_cust),
        'age': np.random.randint(18, 70, n_cust),
        'city': np.random.choice(['Hanoi', 'HCM', 'Danang', 'NewYork', 'London', 'Tokyo'], n_cust),
        'country': np.random.choice(['VN', 'USA', 'UK', 'JP', 'SG'], n_cust),
        'tier': np.random.choice(['Gold', 'Silver', 'Bronze', 'Platinum'], n_cust),
        'credit_score': np.random.randint(300, 850, n_cust),
        'is_subscribed': np.random.choice([True, False], n_cust),
        'registration_date': np.random.choice(pd.date_range('2020-01-01', '2023-12-31'), n_cust),
        'income_bracket': np.random.choice(['Low', 'Medium', 'High'], n_cust),
        'device_primary': np.random.choice(['iOS', 'Android', 'Windows', 'MacOS'], n_cust)
    })

    # 3. dim_stores (500 dòng)
    stores = pd.DataFrame({
        'store_id': range(1, 501),
        'store_name': [f'Store_Branch_{i}' for i in range(1, 501)],
        'city': np.random.choice(['Hanoi', 'HCM', 'NewYork', 'London'], 500),
        'store_type': np.random.choice(['Flagship', 'Mini', 'Online', 'Outlet'], 500),
        'manager_id': np.random.randint(100, 999, 500),
        'opening_year': np.random.randint(2010, 2024, 500),
        'floor_space_m2': np.random.randint(50, 5000, 500),
        'has_parking': np.random.choice([True, False], 500),
        'latitude': np.random.uniform(-90, 90, 500),
        'longitude': np.random.uniform(-180, 180, 500)
    })

    # 4. dim_suppliers (200 dòng) - FIX LỖI TẠI ĐÂY
    n_supp = 200
    suppliers = pd.DataFrame({
        'supplier_id': range(1, n_supp + 1),
        'supplier_name': [f'Supplier_Co_{i}' for i in range(1, n_supp + 1)],
        'country': np.random.choice(['China', 'Vietnam', 'Germany', 'USA'], n_supp),
        'rating': np.random.uniform(1, 5, n_supp).round(1),
        'lead_time_days': np.random.randint(7, 45, n_supp),
        'payment_terms': np.random.choice(['Net30', 'Net60', 'COD'], n_supp),
        'is_certified': np.random.choice([True, False], n_supp)
    })

    # 5. dim_promotions (100 dòng)
    n_promo = 100
    promotions = pd.DataFrame({
        'promo_id': range(1, n_promo + 1),
        'promo_name': [f'Promo_Campaign_{i}' for i in range(1, n_promo + 1)],
        'discount_pct': np.random.choice([0.05, 0.1, 0.15, 0.2, 0.5], n_promo),
        'promo_type': np.random.choice(['Seasonal', 'FlashSale', 'MemberOnly'], n_promo),
        'min_spend': np.random.choice([0, 50, 100, 500], n_promo),
        'is_stackable': np.random.choice([True, False], n_promo)
    })

    # 6. dim_shipping_methods (5 dòng)
    shipping = pd.DataFrame({
        'ship_id': range(1, 6),
        'carrier': ['FedEx', 'DHL', 'UPS', 'VNPost', 'GrabExpress'],
        'service_level': ['Economy', 'Standard', 'Express', 'NextDay', 'SameDay'],
        'base_cost': [5, 10, 20, 30, 50]
    })

    # 7. dim_date
    dates = pd.DataFrame({'full_date': pd.date_range('2023-01-01', '2025-12-31')})
    dates['year'] = dates.full_date.dt.year
    dates['month'] = dates.full_date.dt.month
    dates['quarter'] = dates.full_date.dt.quarter
    dates['is_holiday'] = np.random.choice([True, False], len(dates), p=[0.05, 0.95])

    # 8. fact_sales (10M dòng)
    sales = pd.DataFrame({
        'order_id': np.arange(1, n_sales + 1),
        'order_time': np.random.choice(pd.date_range('2024-01-01', '2024-12-31', freq='T'), n_sales),
        'customer_id': np.random.randint(1, n_cust + 1, n_sales),
        'product_id': np.random.choice(products['product_id'], n_sales),
        'store_id': np.random.randint(1, 501, n_sales),
        'promo_id': np.random.randint(1, n_promo + 1, n_sales),
        'ship_id': np.random.randint(1, 6, n_sales),
        'quantity': np.random.randint(1, 5, n_sales),
        'sale_price': np.random.uniform(100, 2000, n_sales).round(2),
        'status': np.random.choice(['Completed', 'Cancelled', 'Returned'], n_sales, p=[0.9, 0.05, 0.05]),
        'payment_method': np.random.choice(['CreditCard', 'E-Wallet', 'Crypto'], n_sales)
    })

    # 9. fact_inventory (5M dòng)
    inventory = pd.DataFrame({
        'snapshot_date': np.random.choice(pd.date_range('2024-12-01', '2024-12-31'), n_inventory),
        'store_id': np.random.randint(1, 501, n_inventory),
        'product_id': np.random.choice(products['product_id'], n_inventory),
        'stock_on_hand': np.random.randint(0, 1000, n_inventory)
    })

    # 10. fact_web_logs (20M dòng)
    logs = pd.DataFrame({
        'log_id': np.arange(1, n_logs + 1),
        'user_id': np.random.randint(1, n_cust + 1, n_logs),
        'page_url': np.random.choice(['/home', '/product', '/cart', '/checkout'], n_logs),
        'device': np.random.choice(['Mobile', 'Desktop'], n_logs),
        'dwell_time': np.random.randint(5, 300, n_logs)
    })

    # Xuất tất cả các file
    tables = {
        "dim_products": products, "dim_customers": customers, "dim_stores": stores,
        "dim_suppliers": suppliers, "dim_promotions": promotions, "dim_shipping": shipping,
        "dim_date": dates, "fact_sales": sales, "fact_inventory": inventory, "fact_web_logs": logs
    }
    
    for name, df in tables.items():
        print(f"Đang xuất file: {name}.csv ({len(df)} dòng)...")
        df.to_csv(f"{name}.csv", index=False)
    
    
    print("--- HOÀN THÀNH TẤT CẢ 10 BẢNG! ---")

if __name__ == "__main__":
    generate_full_10_tables()