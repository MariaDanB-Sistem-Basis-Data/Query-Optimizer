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
    DropTableData
)

def print_tree(node, indent=0, prefix="ROOT", is_last=True):
    if node is None:
        return
    
    # determine connector
    if indent == 0:
        connector = ""
    else:
        connector = "└── " if is_last else "├── "
    
    # format value untuk display
    val_str = _format_val(node.val)
    
    # print current node
    spacing = "    " * (indent - 1) + ("    " if indent > 0 and is_last else "│   " if indent > 0 else "")
    if indent == 0:
        print(f"{node.type}: {val_str}")
    else:
        spacing = "    " * (indent - 1) + ("    " if not is_last else "")
        if indent == 1:
            spacing = ""
        print(f"{spacing}{connector}{node.type}: {val_str}")
    
    # print children
    for i, child in enumerate(node.childs):
        is_last_child = (i == len(node.childs) - 1)
        print_tree(child, indent + 1, "", is_last_child)

# helper untuk format val menjadi string yang readable
def _format_val(val):
    if val is None:
        return "∅"
    
    if isinstance(val, str):
        return f'"{val}"' if val else "∅"
    
    if isinstance(val, int):
        return str(val)
    
    if isinstance(val, list):
        if len(val) == 0:
            return "[]"
        if isinstance(val[0], ColumnNode):
            return "[" + ", ".join(str(c) for c in val) + "]"
        if isinstance(val[0], OrderByItem):
            return "[" + ", ".join(str(o) for o in val) + "]"
        if isinstance(val[0], SetClause):
            return "[" + ", ".join(str(s) for s in val) + "]"
        if isinstance(val[0], ColumnDefinition):
            return "[" + ", ".join(str(c) for c in val) + "]"
        return str(val)
    
    if isinstance(val, ConditionNode):
        left = _format_attr(val.attr)
        right = _format_value(val.value)
        return f"{left} {val.op} {right}"
    
    if isinstance(val, LogicalNode):
        childs_str = ", ".join(_format_val(c) for c in val.childs)
        return f"({val.operator}: {childs_str})"
    
    if isinstance(val, TableReference):
        if val.alias:
            return f"{val.name} AS {val.alias}"
        return val.name
    
    if isinstance(val, TableReference):
        if val.alias:
            return f"{val.name} AS {val.alias}"
        return val.name
    
    if isinstance(val, InsertData):
        return f"table={val.table}"
    
    if isinstance(val, CreateTableData):
        return f"table={val.table}"
    
    if isinstance(val, DropTableData):
        mode = "CASCADE" if val.cascade else "RESTRICT"
        return f"table={val.table}, mode={mode}"
    
    if isinstance(val, dict):
        return str(val)
    
    return str(val)

def _format_attr(attr):
    if isinstance(attr, ColumnNode):
        if attr.table:
            return f"{attr.table}.{attr.column}"
        return attr.column
    if isinstance(attr, dict):
        if attr.get('table'):
            return f"{attr['table']}.{attr['column']}"
        return attr.get('column', str(attr))
    return str(attr)

def _format_value(value):
    if isinstance(value, ColumnNode):
        if value.table:
            return f"{value.table}.{value.column}"
        return value.column
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, dict):
        if value.get('table'):
            return f"{value['table']}.{value['column']}"
        return value.get('column', str(value))
    return str(value)

def print_tree_box(node, prefix="", is_last=True, is_root=True):
    if node is None:
        return
    
    # connector characters
    if is_root:
        current_prefix = ""
        child_prefix = ""
    else:
        current_prefix = prefix + ("└── " if is_last else "├── ")
        child_prefix = prefix + ("    " if is_last else "│   ")
    
    # format value
    val_str = _format_val(node.val)
    
    # print node
    print(f"{current_prefix}[{node.type}] {val_str}")
    
    # print detail untuk InsertData, CreateTableData, DropTableData
    if is_root or True:  # selalu print detail
        detail_prefix = child_prefix if not is_root else ""
        _print_node_details(node, detail_prefix, len(node.childs) == 0)
    
    # print children
    for i, child in enumerate(node.childs):
        is_last_child = (i == len(node.childs) - 1)
        print_tree_box(child, child_prefix, is_last_child, False)


def _print_node_details(node, prefix, is_last_node):
    """print detail tambahan untuk node tertentu"""
    
    if node.type == "INSERT" and isinstance(node.val, InsertData):
        connector = "└── " if is_last_node else "├── "
        print(f"{prefix}{connector}columns: {node.val.columns}")
        print(f"{prefix}    └── values: {node.val.values}")
    
    elif node.type == "CREATE_TABLE" and isinstance(node.val, CreateTableData):
        has_pk = len(node.val.primary_key) > 0
        has_fk = len(node.val.foreign_keys) > 0
        
        # columns
        print(f"{prefix}├── columns:")
        for i, col in enumerate(node.val.columns):
            is_last_col = (i == len(node.val.columns) - 1) and not has_pk and not has_fk
            col_connector = "└── " if is_last_col else "├── "
            size_str = f"({col.size})" if col.size else ""
            print(f"{prefix}│   {col_connector}{col.name}: {col.data_type}{size_str}")
        
        # primary key
        if has_pk:
            pk_connector = "└── " if not has_fk else "├── "
            print(f"{prefix}{pk_connector}primary_key: {node.val.primary_key}")
        
        # foreign keys
        if has_fk:
            print(f"{prefix}└── foreign_keys:")
            for i, fk in enumerate(node.val.foreign_keys):
                fk_connector = "└── " if i == len(node.val.foreign_keys) - 1 else "├── "
                print(f"{prefix}    {fk_connector}{fk.column} -> {fk.ref_table}.{fk.ref_column}")

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
    
    # check PROJECT
    assert result.query_tree.type == "PROJECT"
    assert isinstance(result.query_tree.val, list)
    assert all(isinstance(col, ColumnNode) for col in result.query_tree.val)
    
    # check SIGMA
    sigma = result.query_tree.childs[0]
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, ConditionNode)
    
    # check TABLE
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
    
    # check SIGMA with LogicalNode
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
    
    proj = result.query_tree
    join = proj.childs[0]
    assert join.type == "JOIN"
    
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
    
    update = result.query_tree
    assert update.type == "UPDATE"
    assert isinstance(update.val, list)
    
    sigma = update.childs[0]
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, ConditionNode)
    
    print("\nPassed\n")

def test_delete():
    engine = OptimizationEngine()
    query = "DELETE FROM student WHERE gpa < 2.0;"
    result = engine.parse_query(query)
    
    print("=" * 50)
    print("Test DELETE")
    print("=" * 50)
    print(f"Query: {query}\n")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    
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
    assert result.query_tree.type == "BEGIN_TRANSACTION"
    
    query = "COMMIT;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    assert result.query_tree.type == "COMMIT"
    
    query = "ROLLBACK;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
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
    
    print("\nPassed\n")

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
    
    join = result.query_tree
    assert join.type == "JOIN"
    assert join.val == "NATURAL"
    
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
    
    join = result.query_tree
    assert join.type == "JOIN"
    assert join.val == "CARTESIAN"
    
    print("\nPASSED\n")

def test_mixed_and_or():
    engine = OptimizationEngine()
    
    print("=" * 50)
    print("Test Mixed AND/OR Conditions")
    print("=" * 50)
    
    # test 1: a AND b OR c
    query = "SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    
    sigma = result.query_tree
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, LogicalNode)
    assert sigma.val.operator == "OR"
    # first child should be AND node
    assert isinstance(sigma.val.childs[0], LogicalNode)
    assert sigma.val.childs[0].operator == "AND"
    
    # test 2: a OR b AND c
    query = "SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    
    sigma = result.query_tree
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, LogicalNode)
    assert sigma.val.operator == "OR"
    # second child should be AND node
    assert isinstance(sigma.val.childs[1], LogicalNode)
    assert sigma.val.childs[1].operator == "AND"
    
    # test 3: a AND b AND c OR d
    query = "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 OR d = 4;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    
    sigma = result.query_tree
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, LogicalNode)
    assert sigma.val.operator == "OR"
    # first child should be AND with 3 conditions
    assert isinstance(sigma.val.childs[0], LogicalNode)
    assert sigma.val.childs[0].operator == "AND"
    assert len(sigma.val.childs[0].childs) == 3
    
    # test 4: a OR b OR c AND d
    query = "SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3 AND d = 4;"
    result = engine.parse_query(query)
    print(f"\nQuery: {query}")
    print("Query Tree:")
    print_tree_box(result.query_tree)
    
    sigma = result.query_tree
    assert sigma.type == "SIGMA"
    assert isinstance(sigma.val, LogicalNode)
    assert sigma.val.operator == "OR"
    # last child should be AND node
    assert isinstance(sigma.val.childs[2], LogicalNode)
    assert sigma.val.childs[2].operator == "AND"
    
    print("\nPASSED\n")

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
    
    print("=" * 50)
    print("All tests passed!")
    print("=" * 50)