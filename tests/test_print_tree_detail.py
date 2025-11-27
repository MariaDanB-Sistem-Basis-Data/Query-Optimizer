"""
Run test tree -> python -m tests.test_print_tree_detail
"""

from QueryOptimizer import OptimizationEngine
from model.query_tree import (
    QueryTree, LogicalNode, ConditionNode, ColumnNode, 
    ThetaJoin, TableReference
)


def print_tree(node, indent=0, label=""):
    if node is None:
        return
    
    prefix = "  " * indent
    
    if label:
        print(f"{prefix}[{label}]")
    
    # print berdasarkan tipe node
    if isinstance(node, QueryTree):
        print(f"{prefix}QueryTree:")
        print(f"{prefix}  type: {node.type}")
        print(f"{prefix}  val: ", end="")
        
        # handle different val types
        if isinstance(node.val, (LogicalNode, ConditionNode, ColumnNode, ThetaJoin, TableReference)):
            print()
            print_tree(node.val, indent + 2, "")
        elif isinstance(node.val, list):
            if node.val and isinstance(node.val[0], (ColumnNode, LogicalNode, ConditionNode)):
                print(f"[{len(node.val)} items]")
                for i, item in enumerate(node.val):
                    print(f"{prefix}    [{i}]:")
                    print_tree(item, indent + 3, "")
            else:
                print(node.val)
        else:
            print(node.val)
        
        # print children
        if node.childs:
            print(f"{prefix}  childs: ({len(node.childs)} children)")
            for i, child in enumerate(node.childs):
                print_tree(child, indent + 2, f"CHILD[{i}]")
    
    elif isinstance(node, LogicalNode):
        print(f"{prefix}LogicalNode:")
        print(f"{prefix}  operator: {node.operator}")
        print(f"{prefix}  childs: ({len(node.childs)} conditions)")
        for i, child in enumerate(node.childs):
            print_tree(child, indent + 2, f"COND[{i}]")
    
    elif isinstance(node, ConditionNode):
        print(f"{prefix}ConditionNode:")
        print(f"{prefix}  attr: ", end="")
        if isinstance(node.attr, ColumnNode):
            print(f"{node.attr.table}.{node.attr.column}" if node.attr.table else node.attr.column)
        elif isinstance(node.attr, dict):
            table = node.attr.get('table', '')
            col = node.attr.get('column', '')
            print(f"{table}.{col}" if table else col)
        else:
            print(node.attr)
        
        print(f"{prefix}  op: {node.op}")
        
        print(f"{prefix}  value: ", end="")
        if isinstance(node.value, ColumnNode):
            print(f"{node.value.table}.{node.value.column}" if node.value.table else node.value.column)
        elif isinstance(node.value, dict):
            table = node.value.get('table', '')
            col = node.value.get('column', '')
            print(f"{table}.{col}" if table else col)
        else:
            print(node.value)
    
    elif isinstance(node, ColumnNode):
        print(f"{prefix}ColumnNode:")
        print(f"{prefix}  table: {node.table}")
        print(f"{prefix}  column: {node.column}")
    
    elif isinstance(node, ThetaJoin):
        print(f"{prefix}ThetaJoin:")
        print(f"{prefix}  condition:")
        print_tree(node.condition, indent + 2, "")
    
    elif isinstance(node, TableReference):
        print(f"{prefix}TableReference:")
        print(f"{prefix}  name: {node.name}")
        print(f"{prefix}  alias: {node.alias}")


def main():
    query = """
    SELECT * 
    FROM students s
    JOIN enrollments e ON s.student_id = e.student_id 
    JOIN courses c ON c.course_id = e.course_id 
    WHERE s.student_id = 1 AND s.major = 'CS';
    """
    
    print("=" * 80)
    print("QUERY:")
    print("=" * 80)
    print(query.strip())
    print()
    
    print("=" * 80)
    print("TREE STRUCTURE WITH DETAILS:")
    print("=" * 80)
    print()
    
    # parse query
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    # print tree
    print_tree(parsed.query_tree)
    
    print()
    print("=" * 80)
    print("TREE EXPLANATION:")
    print("=" * 80)

if __name__ == "__main__":
    main()
