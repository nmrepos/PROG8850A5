#!/usr/bin/env python3
"""
Test scalar field queries
Tests queries on amounts, dates, and other scalar fields with timing
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
    """Execute a query and measure execution time"""
    conn = connect_db()
    cursor = conn.cursor()
    
    print(f"\n{description}")
    print("-" * 50)
    print(f"Query: {query}")
    
    # Run query 3 times and average the results
    times = []
    for i in range(3):
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        end_time = time.time()
        times.append(end_time - start_time)
    
    avg_time = sum(times) / len(times)
    print(f"Average execution time: {avg_time:.4f} seconds")
    print(f"Results returned: {len(results)} rows")
    
    # Show first few results
    if results:
        print("Sample results:")
        for i, result in enumerate(results[:5]):
            print(f"  {result}")
        if len(results) > 5:
            print(f"  ... and {len(results) - 5} more rows")
    
    cursor.close()
    conn.close()
    return avg_time, len(results)

def explain_query(query, description):
    """Show EXPLAIN output for a query"""
    conn = connect_db()
    cursor = conn.cursor()
    
    print(f"\nEXPLAIN for: {description}")
    print("-" * 30)
    
    explain_query = f"EXPLAIN {query}"
    cursor.execute(explain_query)
    explain_results = cursor.fetchall()
    
    if explain_results:
        # Print header
        print("id | select_type | table | type | possible_keys | key | key_len | ref | rows | Extra")
        print("-" * 80)
        for row in explain_results:
            print(" | ".join(str(x) if x is not None else 'NULL' for x in row))
    
    cursor.close()
    conn.close()

def test_scalar_queries():
    """Test various scalar field queries"""
    print("="*60)
    print("STEP 3: TESTING SCALAR FIELD QUERIES (WITHOUT INDEXES)")
    print("="*60)
    
    results = []
    
    # 1. Payment value queries (amounts)
    print("\n1. PAYMENT VALUE QUERIES")
    print("="*30)
    
    # High-value payments
    query1 = "SELECT payment_type, payment_value FROM order_payments WHERE payment_value > 1000 ORDER BY payment_value DESC LIMIT 10"
    time1, rows1 = time_query(query1, "High-value payments (>$1000)")
    explain_query(query1, "High-value payments")
    results.append(("High-value payments", time1, rows1))
    
    # Average payment by type
    query2 = "SELECT payment_type, AVG(payment_value) as avg_payment, COUNT(*) as count FROM order_payments GROUP BY payment_type ORDER BY avg_payment DESC"
    time2, rows2 = time_query(query2, "Average payment by type")
    explain_query(query2, "Average payment by type")
    results.append(("Average payment by type", time2, rows2))
    
    # 2. Date-based queries
    print("\n\n2. DATE-BASED QUERIES")
    print("="*30)
    
    # Orders by year
    query3 = "SELECT YEAR(order_purchase_timestamp) as year, COUNT(*) as order_count FROM orders GROUP BY YEAR(order_purchase_timestamp) ORDER BY year"
    time3, rows3 = time_query(query3, "Orders by year")
    explain_query(query3, "Orders by year")
    results.append(("Orders by year", time3, rows3))
    
    # Orders in 2018
    query4 = "SELECT COUNT(*) as order_count FROM orders WHERE YEAR(order_purchase_timestamp) = 2018"
    time4, rows4 = time_query(query4, "Orders in 2018")
    explain_query(query4, "Orders in 2018")
    results.append(("Orders in 2018", time4, rows4))
    
    # 3. Geographic queries
    print("\n\n3. GEOGRAPHIC QUERIES")
    print("="*30)
    
    # Customers by state
    query5 = "SELECT customer_state, COUNT(*) as customer_count FROM customers GROUP BY customer_state ORDER BY customer_count DESC LIMIT 10"
    time5, rows5 = time_query(query5, "Top 10 states by customer count")
    explain_query(query5, "Top states")
    results.append(("Top states", time5, rows5))
    
    # Complex JOIN query
    query6 = """
    SELECT c.customer_state, AVG(p.payment_value) as avg_payment 
    FROM customers c 
    JOIN orders o ON c.customer_id = o.customer_id 
    JOIN order_payments p ON o.order_id = p.order_id 
    GROUP BY c.customer_state 
    ORDER BY avg_payment DESC 
    LIMIT 5
    """
    time6, rows6 = time_query(query6, "Average payment by state (JOIN query)")
    explain_query(query6, "Average payment by state")
    results.append(("Average payment by state", time6, rows6))
    
    # Summary
    print("\n\n" + "="*60)
    print("SCALAR QUERY PERFORMANCE SUMMARY (WITHOUT INDEXES)")
    print("="*60)
    print(f"{'Query':<30} {'Time (s)':<12} {'Rows':<8}")
    print("-" * 50)
    for query_name, exec_time, row_count in results:
        print(f"{query_name:<30} {exec_time:<12.4f} {row_count:<8}")
    
    print(f"\nTotal queries tested: {len(results)}")
    print(f"Average query time: {sum(r[1] for r in results) / len(results):.4f} seconds")
    
    return results

if __name__ == "__main__":
    test_scalar_queries()
    print("\n✅ Step 3 completed: Scalar field queries tested and timed")
