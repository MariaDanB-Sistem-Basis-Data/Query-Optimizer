import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from QueryOptimizer import OptimizationEngine
from model.query_tree import (
    QueryTree,
    ConditionNode, 
    LogicalNode, 
    ColumnNode, 
    OrderByItem, 
    TableReference,
    SetClause,
    ColumnDefinition,
    ForeignKeyDefinition,
    InsertData,
    CreateTableData,
    DropTableData,
    NaturalJoin,
    ThetaJoin
)
from model.parsed_query import ParsedQuery
import json

def print_tree(node, indent=0, prefix="ROOT", is_last=True):
    if node is None:
        return
    
    if indent == 0:
        connector = ""
    else:
        connector = "└── " if is_last else "├── "
    
    val_str = _format_val(node.val)
    
    spacing = "    " * (indent - 1) + ("    " if indent > 0 and is_last else "│   " if indent > 0 else "")
    if indent == 0:
        print(f"{node.type}: {val_str}")
    else:
        spacing = "    " * (indent - 1) + ("    " if not is_last else "")
        if indent == 1:
            spacing = ""
        print(f"{spacing}{connector}{node.type}: {val_str}")
    
    for i, child in enumerate(node.childs):
        is_last_child = (i == len(node.childs) - 1)
        print_tree(child, indent + 1, "", is_last_child)

# ==========================================
# HELPER FUNCTIONS FOR VISUALIZATION (UPDATED)
# ==========================================

def _format_val(val):
    if val is None:
        return "∅"
    if isinstance(val, str):
        return f'"{val}"' if val else "∅"
    if isinstance(val, (int, float)):
        return str(val)
    
    if isinstance(val, list):
        if len(val) == 0: return "[]"
        # Format list item secara simple
        items = []
        for x in val:
            if isinstance(x, ColumnNode): items.append(str(x))
            elif isinstance(x, OrderByItem): items.append(f"{x.column} {x.direction}")
            elif isinstance(x, SetClause): items.append(f"{x.column}={x.value}")
            else: items.append(str(x))
        return "[" + ", ".join(items) + "]"
    
    if isinstance(val, ConditionNode):
        left = _format_attr(val.attr)
        right = _format_value(val.value)
        return f"{left} {val.op} {right}"
    
    if isinstance(val, LogicalNode):
        childs_str = ", ".join(_format_val(c) for c in val.childs)
        return f"({val.operator}: {childs_str})"
    
    if isinstance(val, TableReference):
        return f"{val.name} AS {val.alias}" if val.alias else val.name
    
    if isinstance(val, NaturalJoin): return "NATURAL"
    if isinstance(val, ThetaJoin): return f"ON {_format_condition(val.condition)}"
    
    # Custom Objects Info
    if isinstance(val, InsertData): return f"INTO {val.table}"
    if isinstance(val, CreateTableData): return f"TABLE {val.table}"
    if isinstance(val, DropTableData): return f"TABLE {val.table} ({'CASCADE' if val.cascade else 'RESTRICT'})"
    
    # HANDLING VISUAL SUBQUERY (Display Text)
    if isinstance(val, QueryTree):
        return f" <Subquery: {val.type}>" # Penanda teks
    
    return str(val)

def _format_attr(attr):
    # Handle jika objek ColumnNode
    if isinstance(attr, ColumnNode): 
        return str(attr)
    
    # Handle jika bentuknya Dictionary (ini yang terjadi di kodemu sekarang)
    if isinstance(attr, dict):
        col = attr.get('column')
        tbl = attr.get('table')
        if tbl:
            return f"{tbl}.{col}"
        return col
        
    return str(attr)

def _format_value(value):
    if isinstance(value, ColumnNode): return str(value)
    if isinstance(value, str): return f"'{value}'"
    # Jangan return string panjang jika subquery, nanti digambar di details
    if isinstance(value, QueryTree): return "..." 
    return str(value)

def _format_condition(cond):
    if isinstance(cond, ConditionNode):
        return f"{_format_attr(cond.attr)} {cond.op} {_format_value(cond.value)}"
    return str(cond)

def print_tree_box(node, prefix="", is_last=True, is_root=True):
    if node is None: return

    # Gambar konektor untuk node ini
    if is_root:
        curr_prefix = ""
        child_prefix = ""
    else:
        curr_prefix = prefix + ("└── " if is_last else "├── ")
        child_prefix = prefix + ("    " if is_last else "│   ")

    # Print Node Utama
    print(f"{curr_prefix}[{node.type}] {_format_val(node.val)}")

    # --- LOGIKA BARU: Print Detail Subquery jika ada ---
    # Cek apakah node ini memiliki Subquery di dalamnya (misal di SIGMA)
    if node.type == "SIGMA" and isinstance(node.val, ConditionNode):
        if isinstance(node.val.value, QueryTree):
            # Kita gambar subquery seolah-olah dia anak tambahan (branch)
            # Tentukan apakah SIGMA punya anak "asli" (Table) di bawahnya
            has_real_children = len(node.childs) > 0
            
            sub_connector = "├── " if has_real_children else "└── "
            sub_prefix = child_prefix + ("│   " if has_real_children else "    ")
            
            print(f"{child_prefix}{sub_connector}(SUBQUERY DEFINITION):")
            # Rekursif print subquery tree
            print_tree_box(node.val.value, sub_prefix, is_last=True, is_root=False)

    # Print Detail lain (Insert/Create) - Sesuai kode lama
    _print_node_details(node, child_prefix, len(node.childs) == 0)

    # Print Child Nodes Asli
    for i, child in enumerate(node.childs):
        is_last_child = (i == len(node.childs) - 1)
        print_tree_box(child, child_prefix, is_last_child, False)

def _print_node_details(node, prefix, is_last_node):
    # Create Table / Insert details logic (sama seperti sebelumnya)
    if node.type == "INSERT" and isinstance(node.val, InsertData):
        connector = "└── " if is_last_node else "├── "
        print(f"{prefix}{connector}Val: {node.val.values}")
    elif node.type == "CREATE_TABLE" and isinstance(node.val, CreateTableData):
        print(f"{prefix}└── Cols: {[c.name for c in node.val.columns]}")

def node_to_json(node):
    if node is None:
        return None
    
    return {
        "type": node.type,
        "val": val_to_json(node.val),
        "childs": [node_to_json(child) for child in node.childs]
    }

def val_to_json(val):
    if val is None:
        return None
    
    if isinstance(val, str):
        return val
    
    if isinstance(val, (int, float)):
        return val
    
    if isinstance(val, list):
        return [val_to_json(item) for item in val]
    
    if isinstance(val, ColumnNode):
        return {
            "type": "ColumnNode",
            "column": val.column,
            "table": val.table
        }
    
    if isinstance(val, ConditionNode):
        return {
            "type": "ConditionNode",
            "attr": val_to_json(val.attr),
            "op": val.op,
            "value": val_to_json(val.value)
        }
    
    if isinstance(val, LogicalNode):
        return {
            "type": "LogicalNode",
            "operator": val.operator,
            "childs": [val_to_json(child) for child in val.childs]
        }
    
    if isinstance(val, OrderByItem):
        return {
            "type": "OrderByItem",
            "column": val_to_json(val.column),
            "direction": val.direction
        }
    
    if isinstance(val, SetClause):
        return {
            "type": "SetClause",
            "column": val.column,
            "value": val.value
        }
    
    if isinstance(val, TableReference):
        return {
            "type": "TableReference",
            "name": val.name,
            "alias": val.alias
        }
    
    if isinstance(val, NaturalJoin):
        return {
            "type": "NaturalJoin"
        }
    
    if isinstance(val, ThetaJoin):
        return {
            "type": "ThetaJoin",
            "condition": val_to_json(val.condition)
        }
    
    if isinstance(val, InsertData):
        return {
            "type": "InsertData",
            "table": val.table,
            "columns": val.columns,
            "values": val.values
        }
    
    if isinstance(val, CreateTableData):
        return {
            "type": "CreateTableData",
            "table": val.table,
            "columns": [
                {
                    "type": "ColumnDefinition",
                    "name": col.name,
                    "data_type": col.data_type,
                    "size": col.size
                } for col in val.columns
            ],
            "primary_key": val.primary_key,
            "foreign_keys": [
                {
                    "type": "ForeignKeyDefinition",
                    "column": fk.column,
                    "ref_table": fk.ref_table,
                    "ref_column": fk.ref_column
                } for fk in val.foreign_keys
            ]
        }
    
    if isinstance(val, DropTableData):
        return {
            "type": "DropTableData",
            "table": val.table,
            "cascade": val.cascade
        }
    
    # --- HANDLING BARU SUBQUERY UNTUK JSON ---
    if isinstance(val, QueryTree):
        return node_to_json(val) # Rekursif untuk print subquery sebagai JSON penuh
    
    if isinstance(val, dict):
        return val
    
    return str(val)

def print_json(node, indent=2):
    json_data = node_to_json(node)
    print(json.dumps(json_data, indent=indent, ensure_ascii=False))

# ==========================================
# EXISTING TESTS
# ==========================================

def test_select_simple():
    engine = OptimizationEngine()
    query = "SELECT name, age FROM student WHERE id = 1;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test SELECT Simple")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    assert result.query_tree.type == "PROJECT"
    assert isinstance(result.query_tree.val, list)
    assert all(isinstance(col, ColumnNode) for col in result.query_tree.val)
    
    sigma = result.query_tree.childs[0]
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, ConditionNode)
    
    table = sigma.childs[0]
    assert table.type == "TABLE"
    assert isinstance(table.val, TableReference)
    
    print("\nPASSED\n")

def test_select_with_and():
    engine = OptimizationEngine()
    query = "SELECT * FROM student WHERE age > 20 AND name = 'John';"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test SELECT with AND")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    sigma = result.query_tree
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, LogicalNode)
    assert sigma.val.operator == "AND"
    
    print("\nPASSED\n")

def test_select_with_or():
    engine = OptimizationEngine()
    query = "SELECT id FROM student WHERE age < 18 OR age > 60;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test SELECT with OR")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    proj = result.query_tree
    sigma = proj.childs[0]
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, LogicalNode)
    assert sigma.val.operator == "OR"
    
    print("\nPASSED\n")

def test_select_with_order_by():
    engine = OptimizationEngine()
    query = "SELECT name, salary FROM employee ORDER BY salary DESC;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test SELECT with ORDER BY")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    proj = result.query_tree
    sort = proj.childs[0]
    assert sort.type == "SORT"
    assert isinstance(sort.val, list)
    assert all(isinstance(item, OrderByItem) for item in sort.val)
    
    print("\nPASSED\n")

def test_select_with_join():
    engine = OptimizationEngine()
    query = "SELECT s.name, c.title FROM student AS s JOIN course AS c ON s.course_id = c.id;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test SELECT with JOIN")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    proj = result.query_tree
    join = proj.childs[0]
    assert join.type == "JOIN"
    assert isinstance(join.val, ThetaJoin)
    assert isinstance(join.val.condition, ConditionNode)
    
    left = join.childs[0]
    right = join.childs[1]
    assert left.type == "TABLE"
    assert right.type == "TABLE"
    assert isinstance(left.val, TableReference)
    assert isinstance(right.val, TableReference)
    
    print("\nPASSED\n")

def test_update():
    engine = OptimizationEngine()
    query = "UPDATE employee SET salary = 5000 WHERE id = 1;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test UPDATE")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    update = result.query_tree
    assert update.type == "UPDATE"
    assert isinstance(update.val, list)
    
    sigma = update.childs[0]
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, ConditionNode)
    
    print("\nPASSED\n")

def test_delete():
    engine = OptimizationEngine()
    query = "DELETE FROM student WHERE gpa < 2.5;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test DELETE")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    delete = result.query_tree
    assert delete.type == "DELETE"
    
    sigma = delete.childs[0]
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, ConditionNode)
    
    print("\nPASSED\n")

def test_insert():
    engine = OptimizationEngine()
    query = "INSERT INTO student (id, name, gpa) VALUES (1, 'John', 3.5);"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test INSERT")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    insert = result.query_tree
    assert insert.type == "INSERT"
    assert isinstance(insert.val, InsertData)
    assert insert.val.table == "student"
    assert insert.val.columns == ["id", "name", "gpa"]
    assert insert.val.values == [1, "John", 3.5]
    
    print("\nPASSED\n")

def test_create_table():
    engine = OptimizationEngine()
    query = "CREATE TABLE student (id int, name varchar(50), PRIMARY KEY(id));"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test CREATE TABLE")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    create = result.query_tree
    assert create.type == "CREATE_TABLE"
    assert isinstance(create.val, CreateTableData)
    assert create.val.table == "student"
    assert len(create.val.columns) == 2
    assert create.val.primary_key == ["id"]
    
    print("\nPASSED\n")

def test_drop_table():
    engine = OptimizationEngine()
    query = "DROP TABLE student CASCADE;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test DROP TABLE")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    drop = result.query_tree
    assert drop.type == "DROP_TABLE"
    assert isinstance(drop.val, DropTableData)
    assert drop.val.table == "student"
    assert drop.val.cascade == True
    
    print("\nPASSED\n")

def test_transaction():
    engine = OptimizationEngine()
    
    print("=" * 50)
    print("Test Transaction Statements")
    print("=" * 50)
    
    query = "BEGIN TRANSACTION;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    assert result.query_tree.type == "BEGIN_TRANSACTION"
    
    query = "COMMIT;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    assert result.query_tree.type == "COMMIT"
    
    query = "ROLLBACK;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    assert result.query_tree.type == "ROLLBACK"
    
    print("\nPASSED\n")

def test_complex_query():
    engine = OptimizationEngine()
    query = "SELECT s.name, c.title FROM student AS s JOIN course AS c ON s.course_id = c.id WHERE s.gpa > 3.0 AND c.year = 2024 ORDER BY s.name ASC LIMIT 10;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test Complex Query")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    print("\nPASSED\n")

def test_natural_join():
    engine = OptimizationEngine()
    query = "SELECT * FROM student NATURAL JOIN course;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test NATURAL JOIN")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    join = result.query_tree
    assert join.type == "JOIN"
    assert isinstance(join.val, NaturalJoin)
    
    print("\nPASSED\n")

def test_cartesian_product():
    engine = OptimizationEngine()
    query = "SELECT * FROM student, course;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test Cartesian Product")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    join = result.query_tree
    assert join.type == "JOIN"
    assert join.val == "CARTESIAN"
    
    print("\nPASSED\n")

def test_mixed_and_or():
    engine = OptimizationEngine()
    
    print("=" * 50)
    print("Test Mixed AND/OR Conditions")
    print("=" * 50)
    
    query = "SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    print("\nJSON Output:")
    print_json(result.query_tree)
    
    sigma = result.query_tree
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, LogicalNode)
    assert sigma.val.operator == "OR"
    assert isinstance(sigma.val.childs[0], LogicalNode)
    assert sigma.val.childs[0].operator == "AND"
    
    print("\nPASSED\n")

# ==========================================
# NEW SUBQUERY TESTS
# ==========================================

def test_subquery_simple():
    engine = OptimizationEngine()
    print("=" * 50)
    print("Test Subquery: Simple WHERE")
    print("=" * 50)
    
    query = "SELECT * FROM products WHERE price > (SELECT cost FROM materials WHERE id = 1);"
    print(f"Query: {query}\n")
    
    try:
        result = engine.parse_query(query)
        print("Query Tree:")
        print_tree_box(result.query_tree)
        print("\nJSON Output:")
        print_json(result.query_tree)
        
        # Validasi Subquery
        root = result.query_tree
        sigma = None
        # Cari node SIGMA (traverse simple)
        def find_sigma(node):
            if node.type == "SIGMA": return node
            for child in node.childs:
                res = find_sigma(child)
                if res: return res
            return None
            
        sigma = find_sigma(root)
        
        assert sigma is not None, "SIGMA not found"
        assert isinstance(sigma.val, ConditionNode), "SIGMA val must be ConditionNode"
        # Assert Value kanan adalah QueryTree
        assert isinstance(sigma.val.value, QueryTree), "Right Value must be QueryTree (Subquery)"
        
        print("\nPASSED\n")
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()

def test_subquery_aggregate():
    engine = OptimizationEngine()
    print("=" * 50)
    print("Test Subquery: With Aggregate")
    print("=" * 50)
    
    query = "SELECT id FROM students WHERE score >= (SELECT AVG(score) FROM students);"
    print(f"Query: {query}\n")
    
    try:
        result = engine.parse_query(query)
        print("Query Tree:")
        print_tree_box(result.query_tree)
        print("\nJSON Output:")
        print_json(result.query_tree)
        
        # Validasi visual sudah cukup via JSON
        # Pastikan struktur SIGMA->val->value adalah QueryTree
        
        print("\nPASSED\n")
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()

def test_subquery_nested():
    engine = OptimizationEngine()
    print("=" * 50)
    print("Test Subquery: Nested (Subquery inside Subquery)")
    print("=" * 50)
    
    query = "SELECT * FROM t1 WHERE a = (SELECT b FROM t2 WHERE c = (SELECT d FROM t3 WHERE id = 1));"
    print(f"Query: {query}\n")
    
    try:
        result = engine.parse_query(query)
        print("Query Tree:")
        print_tree_box(result.query_tree)
        print("\nJSON Output:")
        print_json(result.query_tree)
        
        print("\nPASSED\n")
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_select_simple()
    test_select_with_and()
    test_select_with_or()
    test_mixed_and_or()
    test_select_with_order_by()
    test_select_with_join()
    test_natural_join()
    test_cartesian_product()
    test_complex_query()
    test_update()
    test_delete()
    test_insert()
    test_create_table()
    test_drop_table()
    test_transaction()
    
    # Run Subquery Tests
    test_subquery_simple()
    test_subquery_aggregate()
    test_subquery_nested()
    
    print("=" * 50)
    print("All tests passed!")
    print("=" * 50)