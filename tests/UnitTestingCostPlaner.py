"""
Cara penggunaan cost planner
run command ini -> python tests/UnitTestingCostPlaner.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from model.query_tree import QueryTree
from model.parsed_query import ParsedQuery
from helper.cost import CostPlanner

def test_simple_select():
    """Test 1: Simple SELECT dengan WHERE clause"""
    print("\n" + "="*70)
    print("TEST 1: Simple SELECT dengan WHERE")
    print("="*70)
    
    # Query: SELECT name FROM employees WHERE salary > 50000
    table_node = QueryTree("TABLE", "employees")
    select_node = QueryTree("SIGMA", "salary > 50000")
    select_node.add_child(table_node)
    project_node = QueryTree("PROJECT", "name")
    project_node.add_child(select_node)
    
    # parsed query
    query_text = "SELECT name FROM employees WHERE salary > 50000"
    parsed_query = ParsedQuery(query=query_text)
    parsed_query.query_tree = project_node
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_join_query():
    """Test 2: JOIN query"""
    print("\n" + "="*70)
    print("TEST 2: JOIN Query")
    print("="*70)
    
    # Query: SELECT * FROM employees JOIN departments ON emp.dept_id = dept.id
    table_emp = QueryTree("TABLE", "employees")
    table_dept = QueryTree("TABLE", "departments")
    
    join_node = QueryTree("JOIN", "INNER")
    join_node.add_child(table_emp)
    join_node.add_child(table_dept)
    
    project_node = QueryTree("PROJECT", "*")
    project_node.add_child(join_node)
    
    # Create parsed query
    query_text = "SELECT * FROM employees JOIN departments ON emp.dept_id = dept.id"
    parsed_query = ParsedQuery(query=query_text)
    parsed_query.query_tree = project_node
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_complex_query():
    """Test 3: Complex query dengan JOIN, WHERE, ORDER BY"""
    print("\n" + "="*70)
    print("TEST 3: Complex Query (JOIN + WHERE + ORDER BY)")
    print("="*70)
    
    # Query: SELECT e.name, d.name 
    #        FROM employees e JOIN departments d ON e.dept_id = d.id
    #        WHERE e.salary > 50000
    #        ORDER BY e.name
    
    table_emp = QueryTree("TABLE", "employees")
    table_dept = QueryTree("TABLE", "departments")
    
    # Join
    join_node = QueryTree("JOIN", "INNER")
    join_node.add_child(table_emp)
    join_node.add_child(table_dept)
    
    # Selection (WHERE)
    select_node = QueryTree("SIGMA", "salary > 50000")
    select_node.add_child(join_node)
    
    # Sort (ORDER BY)
    sort_node = QueryTree("SORT", "e.name")
    sort_node.add_child(select_node)
    
    # Projection (SELECT columns)
    project_node = QueryTree("PROJECT", "e.name, d.name")
    project_node.add_child(sort_node)
    
    # Create parsed query
    query_text = """
    SELECT e.name, d.name 
    FROM employees e JOIN departments d ON e.dept_id = d.id
    WHERE e.salary > 50000
    ORDER BY e.name
    """
    parsed_query = ParsedQuery(query=query_text)
    parsed_query.query_tree = project_node
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_aggregation_query():
    """Test 4: Aggregation query dengan GROUP BY"""
    print("\n" + "="*70)
    print("TEST 4: Aggregation Query (GROUP BY)")
    print("="*70)
    
    # Query: SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id
    
    # Build query tree
    table_node = QueryTree("TABLE", "employees")
    
    # Group by
    group_node = QueryTree("GROUP", "dept_id")
    group_node.add_child(table_node)
    
    # Aggregation
    agg_node = QueryTree("AGGREGATE", "COUNT(*)")
    agg_node.add_child(group_node)
    
    # Projection
    project_node = QueryTree("PROJECT", "dept_id, COUNT(*)")
    project_node.add_child(agg_node)
    
    # Create parsed query
    query_text = "SELECT dept_id, COUNT(*) FROM employees GROUP BY dept_id"
    parsed_query = ParsedQuery(query=query_text)
    parsed_query.query_tree = project_node
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def test_with_limit():
    """Test 5: Query dengan LIMIT"""
    print("\n" + "="*70)
    print("TEST 5: Query dengan LIMIT")
    print("="*70)
    
    # Query: SELECT * FROM orders WHERE status = 'completed' LIMIT 100
    
    # Build query tree
    table_node = QueryTree("TABLE", "orders")
    
    select_node = QueryTree("SIGMA", "status = 'completed'")
    select_node.add_child(table_node)
    
    limit_node = QueryTree("LIMIT", "100")
    limit_node.add_child(select_node)
    
    project_node = QueryTree("PROJECT", "*")
    project_node.add_child(limit_node)
    
    # Create parsed query
    query_text = "SELECT * FROM orders WHERE status = 'completed' LIMIT 100"
    parsed_query = ParsedQuery(query=query_text)
    parsed_query.query_tree = project_node
    
    # Plan dan print
    planner = CostPlanner()
    cost_plan = planner.plan_query(parsed_query)
    planner.print_cost_breakdown(cost_plan)


def print_table_statistics():
    """Print statistik tabel yang digunakan"""
    print("\n" + "="*70)
    print("STATISTIK TABEL (Dummy Data)")
    print("="*70)
    
    planner = CostPlanner()
    
    tables = ["employees", "departments", "orders", "customers", "products"]
    
    print(f"{'Table':<15} {'Blocking Factor':<18} {'Total Blocks':<15} {'Total Records':<15}")
    print("-" * 70)
    
    for table in tables:
        bf = planner.get_blocking_factor(table)
        blocks = planner.get_total_blocks(table)
        records = planner.get_total_records(table)
        print(f"{table:<15} {bf:<18} {blocks:<15} {records:<15}")
    
    print("="*70)


def test_get_cost_function():
    print("\n" + "="*70)
    print("TEST 6: Fungsi get_cost() - Simple Cost Retrieval")
    print("="*70)
    
    # Query: SELECT * FROM products
    table_node = QueryTree("TABLE", "products")
    project_node = QueryTree("PROJECT", "*")
    project_node.add_child(table_node)
    
    query_text = "SELECT * FROM products"
    parsed_query = ParsedQuery(query=query_text)
    parsed_query.query_tree = project_node
    
    # Test get_cost() function
    planner = CostPlanner()
    cost = planner.get_cost(parsed_query)
    
    print(f"Query: {query_text}")
    print(f"Cost (using get_cost()): {cost}")
    print(f"Type: {type(cost)}")
    print("\nFungsi get_cost() mengembalikan integer cost langsung!")
    print("="*70)


if __name__ == "__main__":
    print("\n" + "#"*70)
    print("# COST PLANNER - TEST SUITE")
    print("#"*70)
    
    # Print statistik dulu
    print_table_statistics()
    
    # Run all tests
    test_simple_select()
    test_join_query()
    test_complex_query()
    test_aggregation_query()
    test_with_limit()
    test_get_cost_function()
    
    print("\n" + "#"*70)
    print("# SEMUA TEST SELESAI")
    print("#"*70)
