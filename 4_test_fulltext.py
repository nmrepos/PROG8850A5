#!/usr/bin/env python3
"""
Test full-text search queries
Tests MATCH() ... AGAINST() queries with timing
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
    print(f"Query: {query.strip()}")
    
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
        for i, result in enumerate(results[:3]):
            print(f"  {result}")
        if len(results) > 3:
            print(f"  ... and {len(results) - 3} more rows")
    
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

def test_fulltext_searches():
    """Test full-text search queries"""
    print("="*60)
    print("STEP 4: TESTING FULL-TEXT SEARCHES")
    print("="*60)
    
    results = []
    
    # Test 1: Search for product reviews
    print("\n1. PRODUCT QUALITY SEARCH")
    print("="*30)
    
    query1 = """
    SELECT review_id, review_score, review_comment_message
    FROM order_reviews
    WHERE MATCH(review_comment_title, review_comment_message) 
    AGAINST ('produto qualidade' IN NATURAL LANGUAGE MODE)
    LIMIT 5
    """
    time1, rows1 = time_query(query1, "Search for 'produto qualidade' (product quality)")
    explain_query(query1, "Product quality search")
    results.append(("Product quality search", time1, rows1))
    
    # Test 2: Boolean search
    print("\n\n2. BOOLEAN SEARCH")
    print("="*25)
    
    query2 = """
    SELECT review_id, review_score, review_comment_message
    FROM order_reviews
    WHERE MATCH(review_comment_title, review_comment_message) 
    AGAINST ('+bom +recomendo' IN BOOLEAN MODE)
    LIMIT 5
    """
    time2, rows2 = time_query(query2, "Boolean search: must contain 'bom' AND 'recomendo'")
    explain_query(query2, "Boolean search")
    results.append(("Boolean search", time2, rows2))
    
    # Test 3: Delivery reviews
    print("\n\n3. DELIVERY SEARCH")
    print("="*25)
    
    query3 = """
    SELECT review_id, review_score, review_comment_message
    FROM order_reviews
    WHERE MATCH(review_comment_title, review_comment_message) 
    AGAINST ('entrega rapido' IN NATURAL LANGUAGE MODE)
    LIMIT 5
    """
    time3, rows3 = time_query(query3, "Search for 'entrega rapido' (fast delivery)")
    explain_query(query3, "Delivery search")
    results.append(("Delivery search", time3, rows3))
    
    # Summary
    print("\n\n" + "="*60)
    print("FULL-TEXT SEARCH PERFORMANCE SUMMARY")
    print("="*60)
    print(f"{'Search Type':<25} {'Time (s)':<12} {'Rows':<8}")
    print("-" * 45)
    for query_name, exec_time, row_count in results:
        print(f"{query_name:<25} {exec_time:<12.4f} {row_count:<8}")
    
    print(f"\nTotal searches tested: {len(results)}")
    print(f"Average search time: {sum(r[1] for r in results) / len(results):.4f} seconds")
    
    return results

if __name__ == "__main__":
    test_fulltext_searches()
    print("\n✅ Step 4 completed: Full-text searches tested")