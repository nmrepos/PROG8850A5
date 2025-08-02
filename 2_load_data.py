#!/usr/bin/env python3
"""
CSV data loader
Loads Olist e-commerce data into MySQL for performance testing
"""

import mysql.connector
import pandas as pd
import time

# Database connection
def connect_db():
    return mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='Secret5555',
        database='olist_ecommerce'
    )

def load_customers():
    """Load customers data"""
    print("Loading customers...")
    conn = connect_db()
    cursor = conn.cursor()
    
    # Read CSV and load in chunks
    df = pd.read_csv('datasets/olist_customers_dataset.csv')
    df = df.where(pd.notnull(df), None)  # Handle NaN values
    
    # Insert data
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO customers 
            (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
            VALUES (%s, %s, %s, %s, %s)
        """, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✓ Loaded {len(df)} customers")

def load_orders():
    """Load orders data"""
    print("Loading orders...")
    conn = connect_db()
    cursor = conn.cursor()
    
    df = pd.read_csv('datasets/olist_orders_dataset.csv')
    df = df.where(pd.notnull(df), None)
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO orders 
            (order_id, customer_id, order_status, order_purchase_timestamp, 
             order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✓ Loaded {len(df)} orders")

def load_payments():
    """Load payment data"""
    print("Loading payments...")
    conn = connect_db()
    cursor = conn.cursor()
    
    df = pd.read_csv('datasets/olist_order_payments_dataset.csv')
    df = df.where(pd.notnull(df), None)
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO order_payments 
            (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s)
        """, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✓ Loaded {len(df)} payments")

def load_reviews():
    """Load reviews data"""
    print("Loading reviews...")
    conn = connect_db()
    cursor = conn.cursor()
    
    df = pd.read_csv('datasets/olist_order_reviews_dataset.csv')
    df = df.where(pd.notnull(df), None)
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO order_reviews 
            (review_id, order_id, review_score, review_comment_title, 
             review_comment_message, review_creation_date, review_answer_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✓ Loaded {len(df)} reviews")

def show_data_summary():
    """Show summary of loaded data"""
    conn = connect_db()
    cursor = conn.cursor()
    
    tables = ['customers', 'orders', 'order_payments', 'order_reviews']
    print("\n" + "="*40)
    print("DATA LOADING SUMMARY")
    print("="*40)
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count:,} rows")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    start_time = time.time()
    print("Starting data loading...")
    
    # Disable foreign key checks for loading
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.close()
    conn.close()
    
    # Load data
    load_customers()
    load_orders()
    load_payments()
    load_reviews()
    
    # Re-enable foreign key checks
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    cursor.close()
    conn.close()
    
    show_data_summary()
    
    end_time = time.time()
    print(f"\nTotal loading time: {end_time - start_time:.2f} seconds")
    print("✅ Data loading complete!")
