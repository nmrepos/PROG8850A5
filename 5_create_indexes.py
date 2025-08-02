#!/usr/bin/env python3
"""
Create indexes and test performance improvement
Creates indexes for scalar queries and compares before/after performance

"""

import mysql.connector
import time

def connect_db():
    """Connect to database"""
    return mysql.connector.connect(
        host='127.0.0.1',
        user='root',
        password='Secret5555',
        database='olist_ecommerce'
    )

def time_query(query, description):
    """Execute a query and measure execution time
    
    This function runs each query 3 times and averages the results to get
    accurate timing measurements. This eliminates random variations and
    gives us reliable performance data for comparison.
    """
    conn = connect_db()
    cursor = conn.cursor()
    
    print(f"\n{description}")
    print("-" * 50)
    print(f"Query: {query.strip()}")
    
    # Run query 3 times and average the results - this gives more reliable timing
    times = []
    for i in range(3):
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()  # Fetch all results to measure complete execution
        end_time = time.time()
        times.append(end_time - start_time)
    
    avg_time = sum(times) / len(times)
    print(f"Average execution time: {avg_time:.4f} seconds")
    print(f"Results returned: {len(results)} rows")
    
    cursor.close()
    conn.close()
    return avg_time, len(results)

def explain_query(query, description):
    """Show EXPLAIN output for a query
    
    EXPLAIN shows us HOW MySQL executes a query - what indexes it uses,
    how many rows it examines, and what operations it performs.
    This is crucial for understanding why indexes improve performance.
    """
    conn = connect_db()
    cursor = conn.cursor()
    
    print(f"\nEXPLAIN for: {description}")
    print("-" * 30)
    
    explain_query = f"EXPLAIN {query}"
    cursor.execute(explain_query)
    explain_results = cursor.fetchall()
    
    if explain_results:
        # Print header - these columns show us the query execution plan
        print("id | select_type | table | type | possible_keys | key | key_len | ref | rows | Extra")
        print("-" * 80)
        for row in explain_results:
            print(" | ".join(str(x) if x is not None else 'NULL' for x in row))
    
    cursor.close()
    conn.close()

def create_indexes():
    """Create indexes for better performance"""
    conn = connect_db()
    cursor = conn.cursor()
    
    print("="*60)
    print("CREATING INDEXES FOR PERFORMANCE OPTIMIZATION")
    print("="*60)
    
    indexes = [
        ("idx_payment_value", "CREATE INDEX idx_payment_value ON order_payments(payment_value)"),
        ("idx_payment_type", "CREATE INDEX idx_payment_type ON order_payments(payment_type)"),
        ("idx_order_purchase_date", "CREATE INDEX idx_order_purchase_date ON orders(order_purchase_timestamp)"),
        ("idx_customer_state", "CREATE INDEX idx_customer_state ON customers(customer_state)"),
        ("idx_customer_id_orders", "CREATE INDEX idx_customer_id_orders ON orders(customer_id)")
    ]
    
    for index_name, create_sql in indexes:
        try:
            print(f"\nCreating index: {index_name}")
            cursor.execute(create_sql)
            print(f"Index {index_name} created successfully")
        except mysql.connector.Error as e:
            if "Duplicate key name" in str(e):
                print(f"Index {index_name} already exists")
            else:
                print(f"Error creating {index_name}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()

def test_queries_with_indexes():
    """Test the same queries from Step 3 with indexes"""
    print("\n\n" + "="*60)
    print("TESTING QUERIES WITH INDEXES")
    print("="*60)
    
    results = []
    
    # Test 1: High-value payments
    print("\n1. HIGH-VALUE PAYMENTS")
    print("="*30)
    
    query1 = "SELECT payment_type, payment_value FROM order_payments WHERE payment_value > 1000 ORDER BY payment_value DESC LIMIT 10"
    time1, rows1 = time_query(query1, "High-value payments (>$1000) - WITH INDEX")
    explain_query(query1, "High-value payments WITH INDEX")
    results.append(("High-value payments", time1, rows1))
    
    # Test 2: Average payment by type
    print("\n\n2. PAYMENT ANALYSIS")
    print("="*25)
    
    query2 = "SELECT payment_type, AVG(payment_value) as avg_payment, COUNT(*) as count FROM order_payments GROUP BY payment_type ORDER BY avg_payment DESC"
    time2, rows2 = time_query(query2, "Average payment by type - WITH INDEX")
    explain_query(query2, "Payment analysis WITH INDEX")
    results.append(("Payment analysis", time2, rows2))
    
    # Test 3: Orders by year
    print("\n\n3. DATE ANALYSIS")
    print("="*20)
    
    query3 = "SELECT YEAR(order_purchase_timestamp) as year, COUNT(*) as order_count FROM orders GROUP BY YEAR(order_purchase_timestamp) ORDER BY year"
    time3, rows3 = time_query(query3, "Orders by year - WITH INDEX")
    explain_query(query3, "Date analysis WITH INDEX")
    results.append(("Date analysis", time3, rows3))
    
    # Test 4: Geographic analysis
    print("\n\n4. GEOGRAPHIC ANALYSIS")
    print("="*30)
    
    query4 = "SELECT customer_state, COUNT(*) as customer_count FROM customers GROUP BY customer_state ORDER BY customer_count DESC LIMIT 10"
    time4, rows4 = time_query(query4, "Top states by customer count - WITH INDEX")
    explain_query(query4, "Geographic analysis WITH INDEX")
    results.append(("Geographic analysis", time4, rows4))
    
    # Test 5: Complex JOIN
    print("\n\n5. JOIN PERFORMANCE")
    print("="*25)
    
    query5 = """
    SELECT c.customer_state, AVG(p.payment_value) as avg_payment 
    FROM customers c 
    JOIN orders o ON c.customer_id = o.customer_id 
    JOIN order_payments p ON o.order_id = p.order_id 
    GROUP BY c.customer_state 
    ORDER BY avg_payment DESC 
    LIMIT 5
    """
    time5, rows5 = time_query(query5, "Average payment by state (JOIN) - WITH INDEX")
    explain_query(query5, "JOIN performance WITH INDEX")
    results.append(("JOIN performance", time5, rows5))
    
    return results

def compare_performance():
    """Compare performance before and after indexes"""
    print("\n\n" + "="*60)
    print("PERFORMANCE COMPARISON: BEFORE vs AFTER INDEXES")
    print("="*60)
    
    # These are the results from Step 3 (before indexes)
    before_results = [
        ("High-value payments", 0.0623, 10),
        ("Payment analysis", 0.1012, 4), 
        ("Date analysis", 0.4509, 3),
        ("Geographic analysis", 0.3706, 10),
        ("JOIN performance", 0.4509, 5)
    ]
    
    # Get current results (after indexes)
    after_results = test_queries_with_indexes()
    
    print(f"\n{'Query':<25} {'Before (s)':<12} {'After (s)':<12} {'Improvement':<15}")
    print("-" * 70)
    
    for i, (query_name, before_time, before_rows) in enumerate(before_results):
        if i < len(after_results):
            after_name, after_time, after_rows = after_results[i]
            improvement = ((before_time - after_time) / before_time) * 100
            print(f"{query_name:<25} {before_time:<12.4f} {after_time:<12.4f} {improvement:>+14.1f}%")
    
    print(f"\nOverall Performance Summary:")
    before_avg = sum(r[1] for r in before_results) / len(before_results)
    after_avg = sum(r[1] for r in after_results) / len(after_results)
    total_improvement = ((before_avg - after_avg) / before_avg) * 100
    print(f"Average time before indexes: {before_avg:.4f} seconds")
    print(f"Average time after indexes:  {after_avg:.4f} seconds")
    print(f"Overall improvement: {total_improvement:+.1f}%")

if __name__ == "__main__":
    print("="*60)
    print("STEP 5: INDEX CREATION AND PERFORMANCE TESTING")
    print("="*60)
    
    # Create indexes
    create_indexes()
    
    # Compare performance
    compare_performance()
    
    print("\nIndexes created and performance improved")
