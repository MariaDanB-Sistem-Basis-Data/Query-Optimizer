"""
Jalanin ini rek -> python -m tests.test_cost_breakdown_multi_join
"""

from QueryOptimizer import OptimizationEngine
from helper.cost import CostPlanner


def print_cost_tree(cost_info, indent=0):
    prefix = "  " * indent
    
    operation = cost_info.get("operation", "UNKNOWN")
    cost = cost_info.get("cost", 0)
    description = cost_info.get("description", "")
    
    print(f"{prefix}┌─ {operation}")
    print(f"{prefix}│  Cost: {cost:.2f}")
    print(f"{prefix}│  Records (n_r): {cost_info.get('n_r', 0):,}")
    print(f"{prefix}│  Blocks (b_r): {cost_info.get('b_r', 0):,}")
    print(f"{prefix}│  Blocking Factor (f_r): {cost_info.get('f_r', 0)}")
    
    if operation == "SELECTION":
        selectivity = cost_info.get("selectivity", 0)
        condition = cost_info.get("condition", "")
        print(f"{prefix}│  Selectivity: {selectivity:.4f}")
        print(f"{prefix}│  Condition: {condition}")
    
    elif operation == "JOIN":
        join_method = cost_info.get("join_method", "")
        join_cost = cost_info.get("join_cost", 0)
        print(f"{prefix}│  Join Method: {join_method}")
        print(f"{prefix}│  Join Cost: {join_cost:.2f}")
        
        # print V(A,r) untuk join attributes
        v_a_r = cost_info.get("v_a_r", {})
        if v_a_r:
            print(f"{prefix}│  Distinct Values (V(A,r)):")
            for attr, val in list(v_a_r.items())[:3]:  # print first 3
                print(f"{prefix}│    - {attr}: {val:,}")
    
    elif operation == "TABLE_SCAN":
        table = cost_info.get("table", "")
        print(f"{prefix}│  Table: {table}")
        
        # print index info
        indexes = cost_info.get("indexes", {})
        if indexes:
            print(f"{prefix}│  Indexes:")
            for col, idx_info in list(indexes.items())[:3]:  # print first 3
                idx_type = idx_info.get('type', 'none')
                idx_val = idx_info.get('value', None)
                if idx_type != 'none':
                    print(f"{prefix}│    - {col}: {idx_type} (value={idx_val})")
    
    print(f"{prefix}│  {description}")
    print(f"{prefix}└─")


def main():
    query = """
    SELECT * 
    FROM students 
    JOIN enrollments ON students.student_id = enrollments.student_id 
    JOIN courses ON courses.course_id = enrollments.course_id 
    WHERE students.student_id = 1 AND students.major = 'CS';
    """
    
    print("=" * 80)
    print("QUERY:")
    print("=" * 80)
    print(query.strip())
    print()
    
    # parse query
    optimizer = OptimizationEngine()
    parsed = optimizer.parse_query(query)
    
    planner = CostPlanner()

    cost_info = planner.calculate_cost(parsed.query_tree)
    print(f"   Total Cost: {cost_info['cost']:.2f} block I/O")
    print(f"   Final Records: {cost_info['n_r']:,}")
    print(f"   Final Blocks: {cost_info['b_r']:,}")
    print("\n=" * 80)
    print("DETAILED BREAKDOWN:")
    print("=" * 80)
    print()
    
    # print tree breakdown secara rekursif
    print_cost_breakdown_recursive(parsed.query_tree, planner, level=0)
    
    print()
    print("=" * 80)
    print("SUMMARY:")
    print("=" * 80)

    print(cost_info.get('cost'))


def print_cost_breakdown_recursive(node, planner, level=0):
    prefix = "  " * level
    
    if node.type == "TABLE":
        cost = planner.cost_table_scan(node)
        print(f"{prefix}{node.type}: {node.val.name if hasattr(node.val, 'name') else node.val}")
        print_cost_tree(cost, level + 1)
    
    elif node.type == "SIGMA":
        if node.childs:
            print_cost_breakdown_recursive(node.childs[0], planner, level)
        
        child_cost = planner.calculate_cost(node.childs[0]) if node.childs else {}
        cost = planner.cost_selection(node, child_cost)
        
        print(f"\n{prefix}{node.type} (SELECTION)")
        print_cost_tree(cost, level + 1)
    
    elif node.type == "JOIN":
        print(f"{prefix}{node.type}")
        print(f"{prefix}  LEFT:")
        if len(node.childs) > 0:
            print_cost_breakdown_recursive(node.childs[0], planner, level + 2)
        
        print(f"{prefix}  RIGHT:")
        if len(node.childs) > 1:
            print_cost_breakdown_recursive(node.childs[1], planner, level + 2)
        
        if len(node.childs) >= 2:
            left_cost = planner.calculate_cost(node.childs[0])
            right_cost = planner.calculate_cost(node.childs[1])
            cost = planner.cost_join(node, left_cost, right_cost)
            
            print(f"\n{prefix}  JOIN RESULT:")
            print_cost_tree(cost, level + 2)
    
    elif node.type == "PROJECT":
        # print child first
        if node.childs:
            print_cost_breakdown_recursive(node.childs[0], planner, level)
        
        child_cost = planner.calculate_cost(node.childs[0]) if node.childs else {}
        cost = planner.cost_projection(node, child_cost)
        
        print(f"\n{prefix}📝 {node.type} (PROJECTION)")
        print_cost_tree(cost, level + 1)


if __name__ == "__main__":
    main()
