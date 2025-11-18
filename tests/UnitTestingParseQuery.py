import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimizer import OptimizationEngine

def print_tree(node, prefix="", is_last=True):
    if node is None:
        return
    
    connector = "└── " if is_last else "├── "
    
    node_info = f"{node.type}"
    if node.val:
        val_display = node.val if len(node.val) <= 40 else node.val[:37] + "..."
        node_info += f" ({val_display})"
    
    print(prefix + connector + node_info)
    
    extension = "    " if is_last else "│   "
    
    if hasattr(node, 'columns') and node.columns and node.columns != "*":
        if isinstance(node.columns, list):
            cols_str = ", ".join([c['column'] for c in node.columns[:3]])
            if len(node.columns) > 3:
                cols_str += ", ..."
            print(prefix + extension + "  ⮡ columns=[" + cols_str + "]")
    
    if hasattr(node, 'condition') and node.condition:
        cond_type = node.condition.get('type', '')
        print(prefix + extension + "  ⮡ condition_type=" + cond_type)
    
    if hasattr(node, 'table_name') and node.table_name:
        table_str = node.table_name
        if hasattr(node, 'table_alias') and node.table_alias:
            table_str += f" AS {node.table_alias}"
        print(prefix + extension + "  ⮡ table=" + table_str)
    
    if hasattr(node, 'limit_value') and node.limit_value is not None:
        print(prefix + extension + f"  ⮡ limit={node.limit_value}")
    
    if hasattr(node, 'set_clauses') and node.set_clauses:
        print(prefix + extension + f"  ⮡ set={node.set_clauses}")
    
    if node.childs:
        for i, child in enumerate(node.childs):
            is_last_child = (i == len(node.childs) - 1)
            print_tree(child, prefix + extension, is_last_child)


def test_query(query_str, description=""):
    """Test single query"""
    print("\n" + "="*70)
    print(f"TEST: {description}")
    print("="*70)
    print(f"Query: {query_str}")
    print("-"*70)
    
    try:
        optimizer = OptimizationEngine()
        parsed = optimizer.parse_query(query_str)
        
        if parsed.query_tree:
            print("Tree Structure:")
            print_tree(parsed.query_tree)
        else:
            print("No tree generated")
            
    except Exception as e:
        print(f"ERROR: {e}")
    
    print("="*70)


if __name__ == "__main__":
    # SELECT Tests
    test_query("SELECT * FROM students;", "1. Simple SELECT *")
    
    test_query("SELECT name, age FROM students;", "2. SELECT specific columns")
    
    test_query("SELECT name FROM students WHERE age > 18;", "3. SELECT with WHERE")
    
    test_query("SELECT name FROM students WHERE age > 18 AND gpa > 3.0;", 
               "4. SELECT with WHERE AND (Single SIGMA)")
    
    test_query("SELECT * FROM students WHERE age < 18 OR age > 65;", 
               "5. SELECT with WHERE OR")
    
    test_query("SELECT * FROM students WHERE age > 18 AND gpa > 3.0 OR department = 'CS';",
               "6. SELECT MIXED AND/OR (SHOULD WORK!)")
    
    test_query("SELECT name FROM students ORDER BY age DESC LIMIT 10;",
               "7. SELECT with ORDER BY and LIMIT")
    
    test_query("SELECT department FROM students GROUP BY department;",
               "8. SELECT with GROUP BY")
    
    test_query("SELECT s.name FROM student AS s WHERE s.age > 18;",
               "9. SELECT with table alias")
    
    test_query("SELECT * FROM students, courses;",
               "10. SELECT Cartesian Product")
    
    test_query("SELECT * FROM students JOIN courses ON students.id = courses.student_id;",
               "11. SELECT with JOIN ON")
    
    test_query("SELECT * FROM students NATURAL JOIN enrollments;",
               "12. SELECT with NATURAL JOIN")
    
    # UPDATE Tests
    test_query("UPDATE students SET gpa = 4.0;", "13. Simple UPDATE")
    
    test_query("UPDATE students SET gpa = 4.0, age = 21;", 
               "14. UPDATE multiple SET")
    
    test_query("UPDATE students SET gpa = 4.0 WHERE id = 123;", 
               "15. UPDATE with WHERE")
    
    test_query("UPDATE employee SET salary = 1.05 * salary WHERE department = 'IT' AND years > 5;",
               "16. UPDATE with expression and multiple WHERE")
    
    # DELETE Tests
    test_query("DELETE FROM students;", "17. Simple DELETE")
    
    test_query("DELETE FROM students WHERE age < 18;", "18. DELETE with WHERE")
    
    test_query("DELETE FROM employee WHERE department = 'RnD' AND salary < 5000;",
               "19. DELETE with multiple WHERE AND")
    
    # INSERT Tests
    test_query("INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20);",
               "20. Simple INSERT")
    
    # Transaction Tests
    test_query("BEGIN TRANSACTION;", "21. BEGIN TRANSACTION")
    test_query("COMMIT;", "22. COMMIT")
    test_query("ROLLBACK;", "23. ROLLBACK")
    
    print("\n" + "╔" + "="*68 + "╗")
    print("║" + " "*25 + "TESTS COMPLETED" + " "*28 + "║")
    print("╚" + "="*68 + "╝")
    print("\nCatatan:")
    print("- Single SIGMA untuk WHERE (dengan structured condition)")
    print("- Check apakah UPDATE multiple SET jadi single atau chained")
    print("- Mixed AND/OR sekarang SUPPORTED!")
    print("- Symbol ⮡ menunjukkan structured attributes\n")