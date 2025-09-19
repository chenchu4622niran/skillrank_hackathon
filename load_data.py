import pandas as pd
import sqlite3
import os

# Step 1: Load CSV
#df = pd.read_csv('data.csv')
df = pd.read_csv('data.csv', encoding='ISO-8859-1')


# Step 2: Basic cleaning (modify column names based on actual CSV)
df.dropna(inplace=True)
df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

# Example check for available columns
print("📊 Available columns in CSV:")

print(df.columns.tolist())

# Stop here and inspect if needed
# exit()  # Uncomment this to check column names only

# Step 3: Create database folder if not exists
os.makedirs('db', exist_ok=True)
conn = sqlite3.connect('db/business.db')
cursor = conn.cursor()

# Step 4: Drop and create tables
cursor.executescript("""
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS sales;

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    city TEXT,
    signup_date DATE
);

CREATE TABLE products (
    id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    price REAL,
    stock INTEGER
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    product_id TEXT,
    quantity INTEGER,
    order_date DATE,
    total REAL
);

CREATE TABLE sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    revenue REAL,
    profit_margin REAL,
    sales_date DATE
);
""")

# Step 5: Populate the tables from your dataset
# 🔄 Adjust based on your actual CSV columns

# Sample: generate dummy customers
# ✅ Customers Table
customers = df[['customerid']].drop_duplicates()
customers['name'] = 'Customer_' + customers['customerid'].astype(str)
customers['email'] = customers['name'].str.lower() + '@example.com'
customers['city'] = df['country']
customers['signup_date'] = pd.to_datetime('2023-01-01')
customers.rename(columns={'customerid': 'id'}, inplace=True)
customers.to_sql('customers', conn, if_exists='append', index=False)

# ✅ Products Table
# ✅ Products Table
products = df[['stockcode', 'description', 'unitprice']].copy()

# Drop duplicate stockcodes
products.drop_duplicates(subset=['stockcode'], inplace=True)

# Add dummy category and stock
products['category'] = 'General'
products['stock'] = 100

# Rename columns to match DB schema
products.rename(columns={
    'stockcode': 'id',
    'description': 'name',
    'unitprice': 'price'
}, inplace=True)

products.to_sql('products', conn, if_exists='append', index=False)


# ✅ Orders Table
orders = df[['customerid', 'stockcode', 'quantity', 'invoicedate', 'unitprice']].copy()
orders['order_date'] = pd.to_datetime(orders['invoicedate'])
orders['total'] = orders['quantity'] * orders['unitprice']
orders.rename(columns={
    'customerid': 'customer_id',
    'stockcode': 'product_id'
}, inplace=True)
orders = orders[['customer_id', 'product_id', 'quantity', 'order_date', 'total']]
orders.to_sql('orders', conn, if_exists='append', index=False)

# ✅ Sales Table (Assume revenue = total, profit = 25%)
sales = orders[['order_date', 'total']].copy()
sales['revenue'] = sales['total']
sales['profit_margin'] = sales['total'] * 0.25
sales['sales_date'] = sales['order_date']
sales['order_id'] = range(1, len(sales) + 1)
sales = sales[['order_id', 'revenue', 'profit_margin', 'sales_date']]
sales.to_sql('sales', conn, if_exists='append', index=False)

conn.close()
print("✅ Database created and populated: db/business.db")

