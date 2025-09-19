from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
import pandas as pd
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

# 1. Setup FastAPI
app = FastAPI()

# 2. Input schema
class QueryRequest(BaseModel):
    question: str

# 3. Database path
DB_PATH = "db/business.db"

# 4. Load SQLCoder model once at startup
tokenizer = AutoTokenizer.from_pretrained("defog/sqlcoder-7b")
model = AutoModelForCausalLM.from_pretrained("defog/sqlcoder-7b")

# 5. SQL generation using Hugging Face model
def generate_sql_from_question(question):
    prompt = f"""
Convert this business question into a valid SQL query using the following schema:
customers(id, name, email, city, signup_date)
products(id, name, category, price, stock)
orders(id, customer_id, product_id, quantity, order_date, total)
sales(id, order_id, revenue, profit_margin, sales_date)

Question: {question}
SQL:
"""

    inputs = tokenizer(prompt, return_tensors="pt")
    with torch.no_grad():
        outputs = model.generate(**inputs, max_new_tokens=150)
    sql = tokenizer.decode(outputs[0], skip_special_tokens=True)

    # Extract only the SQL part
    return sql.split("SQL:")[-1].strip()

# 6. SQL Execution
def run_query(sql):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        return pd.DataFrame({'error': [f"SQL Execution Failed: {str(e)}"]})

# 7. Natural language query endpoint
@app.post("/query")
def query_sql(req: QueryRequest):
    sql = generate_sql_from_question(req.question)
    print(f"Generated SQL:\n{sql}")
    df = run_query(sql)
    return {
        "sql": sql,
        "result": df.to_dict(orient="records")
    }

# 8. Business KPI endpoint
@app.get("/kpi")
def get_kpi():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(revenue) FROM sales")
            total_revenue = cursor.fetchone()[0] or 0

            cursor.execute("SELECT COUNT(*) FROM orders")
            total_orders = cursor.fetchone()[0] or 0

            cursor.execute("SELECT COUNT(DISTINCT customer_id) FROM orders")
            active_customers = cursor.fetchone()[0] or 0

        return {
            "revenue": total_revenue,
            "orders": total_orders,
            "customers": active_customers
        }
    except Exception as e:
        return {"error": str(e)}
@app.get("/kpi/trends")
def get_kpi_trends():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT strftime('%Y-%m', sales_date) AS month, SUM(revenue)
                FROM sales GROUP BY month ORDER BY month DESC LIMIT 2
            """)
            rows = cursor.fetchall()
            if len(rows) < 2:
                return {"trend": "Insufficient data"}
            current, previous = rows[0][1], rows[1][1]
            change = ((current - previous) / previous) * 100 if previous else 0
            return {
                "current_month": current,
                "previous_month": previous,
                "change_percent": round(change, 2),
                "trend": "up" if change > 0 else "down"
            }
    except Exception as e:
        return {"error": str(e)}

#
from fastapi.responses import FileResponse

@app.get("/")
def dashboard():
    return FileResponse("dashboard.html")

@app.get("/chart/sales-by-month")
def sales_by_month():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT strftime('%Y-%m', sales_date) AS month, SUM(revenue)
                FROM sales GROUP BY month ORDER BY month
            """)
            rows = cursor.fetchall()
            labels = [row[0] for row in rows]
            values = [row[1] for row in rows]
        return {"labels": labels, "values": values}
    except Exception as e:
        return {"error": str(e)}

