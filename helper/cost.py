from model.query_tree import QueryTree
from model.parsed_query import ParsedQuery

class CostPlanner:
    def __init__(self):
        # Dummy constants untuk blocking factor
        self.BLOCK_SIZE = 4096
        self.PAGE_SIZE = 4096
        
    def get_blocking_factor(self, table_name: str) -> int:
        """
        Dummy function untuk mendapatkan blocking factor dari sebuah table.
        Blocking factor = jumlah record per block
        """
        # Hardcoded dummy
        dummy_bf = {
            "employees": 10,
            "departments": 20,
            "orders": 15,
            "customers": 12,
            "products": 25
        }
        return dummy_bf.get(table_name.lower(), 10)  # default 10 (blocking factor)
    
    def get_total_blocks(self, table_name: str) -> int:
        """
        Dummy function return total blocks dari sebuah table.
        """
        # Hardcoded dummy values
        dummy_blocks = {
            "employees": 1000,
            "departments": 50,
            "orders": 5000,
            "customers": 2000,
            "products": 800
        }
        return dummy_blocks.get(table_name.lower(), 500)  # default 500
    
    def get_total_records(self, table_name: str) -> int:
        """
        Dummy function return total records dari sebuah table.
        """
        blocks = self.get_total_blocks(table_name)
        bf = self.get_blocking_factor(table_name)
        return blocks * bf
    
    def estimate_selectivity(self, condition: str) -> float:
        """
        Estimasi selectivity dari kondisi WHERE/HAVING.
        Returnnya di range 0.0 - 1.0
        """
        # Dummy estimation based on operator
        if "=" in condition:
            return 0.1
        elif ">" in condition or "<" in condition:
            return 0.3
        elif "LIKE" in condition.upper():
            return 0.2
        elif "IN" in condition.upper():
            return 0.15
        else:
            return 0.5
    
    # ================================================ COST FUNCTIONS ================================================
    
    def cost_table_scan(self, node: QueryTree) -> dict:
        """
        Cost = jumlah blocks yang harus dibaca
        """
        table_name = node.val
        total_blocks = self.get_total_blocks(table_name)
        
        return {
            "operation": "TABLE_SCAN",
            "table": table_name,
            "cost": total_blocks,
            "blocks_read": total_blocks,
            "estimated_records": self.get_total_records(table_name),
            "description": f"Full scan of table {table_name}"
        }
    
    def cost_selection(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi SELECTION (WHERE clause).
        Cost = input cost + cost processing
        
        # TODO: Implementasi index usage (semisal bonus dari SM dikerjakan)
        # Ketika menggunakan index, cost bisa jauh lebih rendah
        # Namun perlu check apakah index masih valid:
        # - Jika table sudah difilter sebelumnya, struktur berubah -> index tidak bisa digunakan
        # - Jika ada aggregation sebelumnya -> index tidak bisa digunakan
        # - Index hanya berguna di level base table
        """
        condition = node.val
        selectivity = self.estimate_selectivity(condition)
        
        input_records = input_cost.get("estimated_records", 1000)
        output_records = int(input_records * selectivity)
        
        # Cost
        processing_cost = input_cost.get("cost", 0) * 0.1
        total_cost = input_cost.get("cost", 0) + processing_cost
        
        return {
            "operation": "SELECTION",
            "condition": condition,
            "cost": total_cost,
            "blocks_read": input_cost.get("blocks_read", 0),
            "estimated_records": output_records,
            "selectivity": selectivity,
            "description": f"Filter with condition: {condition}"
        }
    
    def cost_projection(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi PROJECTION (SELECT columns).
        Cost biasanya rendah, hanya processing overhead
        """
        columns = node.val
        input_records = input_cost.get("estimated_records", 1000)
        
        # Projection cost biasanya minimal
        processing_cost = input_cost.get("cost", 0) * 0.05
        total_cost = input_cost.get("cost", 0) + processing_cost
        
        return {
            "operation": "PROJECTION",
            "columns": columns,
            "cost": total_cost,
            "blocks_read": input_cost.get("blocks_read", 0),
            "estimated_records": input_records,
            "description": f"Project columns: {columns}"
        }
    
    def cost_join(self, node: QueryTree, left_cost: dict, right_cost: dict) -> dict:
        """
        Cost untuk operasi JOIN.
        Menggunakan nested loop join sebagai baseline.
        
        Cost = cost(left) + cost(right) + (records_left * blocks_right)
        
        # TODO: Implementasi join algorithms lain:
        # - Hash Join: better untuk large tables
        # - Merge Join: better jika data sudah sorted
        # - Index Nested Loop: jika ada index di right table
        """
        left_records = left_cost.get("estimated_records", 1000)
        right_blocks = right_cost.get("blocks_read", 100)
        
        # Nested loop join cost
        join_cost = left_records * right_blocks
        total_cost = left_cost.get("cost", 0) + right_cost.get("cost", 0) + join_cost
        
        # Estimate output records (assume 10% join selectivity)
        output_records = int(left_records * right_cost.get("estimated_records", 1000) * 0.1)
        
        return {
            "operation": "JOIN",
            "join_type": node.val if node.val else "INNER",
            "cost": total_cost,
            "blocks_read": left_cost.get("blocks_read", 0) + right_cost.get("blocks_read", 0),
            "estimated_records": output_records,
            "left_records": left_records,
            "right_blocks": right_blocks,
            "description": "Nested loop join"
        }
    
    def cost_sort(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi SORT (ORDER BY).
        Menggunakan external merge sort.
        
        Cost ≈ 2 * blocks * (1 + log_M(blocks))
        dimana M = jumlah blocks yang fit di memory
        """
        input_blocks = input_cost.get("blocks_read", 100)
        input_records = input_cost.get("estimated_records", 1000)
        
        # Misal memori hold 100 blocks
        M = 100
        if input_blocks <= M:
            # Kalau fits di memori, sekali pass aja
            sort_cost = input_blocks
        else:
            # perlu external sort
            import math
            passes = math.ceil(math.log(input_blocks / M, M))
            sort_cost = 2 * input_blocks * (1 + passes)
        
        total_cost = input_cost.get("cost", 0) + sort_cost
        
        return {
            "operation": "SORT",
            "sort_key": node.val,
            "cost": total_cost,
            "blocks_read": input_blocks,
            "estimated_records": input_records,
            "sort_cost": sort_cost,
            "description": f"Sort by {node.val}"
        }
    
    def cost_limit(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi LIMIT.
        Jika LIMIT kecil, bisa early termination.
        """
        limit_val = int(node.val) if node.val.isdigit() else 100
        input_records = input_cost.get("estimated_records", 1000)
        
        output_records = min(limit_val, input_records)
        
        # Cost reduction jika limit sangat kecil
        reduction_factor = min(1.0, output_records / input_records)
        total_cost = input_cost.get("cost", 0) * reduction_factor
        
        return {
            "operation": "LIMIT",
            "limit": limit_val,
            "cost": total_cost,
            "blocks_read": input_cost.get("blocks_read", 0),
            "estimated_records": output_records,
            "description": f"Limit to {limit_val} records"
        }
    
    def cost_aggregation(self, node: QueryTree, input_cost: dict) -> dict:
        """
        Cost untuk operasi AGGREGATION (GROUP BY, COUNT, SUM, etc).
        """
        input_records = input_cost.get("estimated_records", 1000)
        
        # Assume aggregation creates hash table
        # Cost = read all + build hash table
        agg_cost = input_cost.get("cost", 0) * 0.2
        total_cost = input_cost.get("cost", 0) + agg_cost
        
        # Estimate output groups (assume 10% of input records)
        output_records = int(input_records * 0.1)
        
        return {
            "operation": "AGGREGATION",
            "aggregate": node.val,
            "cost": total_cost,
            "blocks_read": input_cost.get("blocks_read", 0),
            "estimated_records": output_records,
            "description": f"Aggregation: {node.val}"
        }
    
    # =================================================================== MAIN COST PLANNING ======================================================================
    
    def calculate_cost(self, node: QueryTree) -> dict:
        """
        Recursively calculate cost untuk query tree.
        Bottom-up approach: calculate children first, then parent.
        """
        if node.type == "TABLE":
            return self.cost_table_scan(node)
        
        elif node.type == "SIGMA" or node.type == "SELECT":
            # Selection
            if not node.childs:
                return {"cost": 0, "estimated_records": 0}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_selection(node, child_cost)
        
        elif node.type == "PROJECT":
            # Projection
            if not node.childs:
                return {"cost": 0, "estimated_records": 0}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_projection(node, child_cost)
        
        elif node.type == "JOIN":
            # Join
            if len(node.childs) < 2:
                return {"cost": 0, "estimated_records": 0}
            left_cost = self.calculate_cost(node.childs[0])
            right_cost = self.calculate_cost(node.childs[1])
            return self.cost_join(node, left_cost, right_cost)
        
        elif node.type == "SORT" or node.type == "ORDER":
            # Sort
            if not node.childs:
                return {"cost": 0, "estimated_records": 0}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_sort(node, child_cost)
        
        elif node.type == "LIMIT":
            # Limit
            if not node.childs:
                return {"cost": 0, "estimated_records": 0}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_limit(node, child_cost)
        
        elif node.type in ["GROUP", "AGGREGATE", "COUNT", "SUM", "AVG"]:
            # Aggregation
            if not node.childs:
                return {"cost": 0, "estimated_records": 0}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_aggregation(node, child_cost)
        
        else:
            # Operasi tidak dikenali, return cost child nya
            if node.childs:
                return self.calculate_cost(node.childs[0])
            return {"cost": 0, "estimated_records": 0}
    
    def plan_query(self, parsed_query: ParsedQuery) -> dict:
        """
        Main entry point untuk cost planning.
        Returns complete cost breakdown.
        """
        if not parsed_query.query_tree:
            return {
                "query": parsed_query.query,
                "error": "No query tree available",
                "total_cost": 0
            }
        
        cost_info = self.calculate_cost(parsed_query.query_tree)
        
        return {
            "query": parsed_query.query,
            "total_cost": cost_info.get("cost", 0),
            "estimated_records": cost_info.get("estimated_records", 0),
            "blocks_read": cost_info.get("blocks_read", 0),
            "details": cost_info
        }
    
    def print_cost_breakdown(self, cost_plan: dict):
        """
        Pretty print cost breakdown.
        """
        print("=" * 60)
        print("QUERY COST PLAN")
        print("=" * 60)
        print(f"Query: {cost_plan.get('query', 'N/A')}")
        print(f"Total Cost: {cost_plan.get('total_cost', 0):.2f}")
        print(f"Estimated Records: {cost_plan.get('estimated_records', 0)}")
        print(f"Blocks Read: {cost_plan.get('blocks_read', 0)}")
        print("=" * 60)
        
        if 'details' in cost_plan:
            self._print_details(cost_plan['details'], indent=0)
    
    def _print_details(self, details: dict, indent: int):
        """
        Helper untuk print nested details.
        """
        prefix = "  " * indent
        print(f"{prefix}Operation: {details.get('operation', 'Unknown')}")
        print(f"{prefix}Cost: {details.get('cost', 0):.2f}")
        print(f"{prefix}Description: {details.get('description', 'N/A')}")
        print()


# # Example usage:
# if __name__ == "__main__":
#     # Create a simple query tree
#     # SELECT name FROM employees WHERE salary > 50000
    
#     table_node = QueryTree("TABLE", "employees")
#     select_node = QueryTree("SIGMA", "salary > 50000")
#     select_node.add_child(table_node)
#     project_node = QueryTree("PROJECT", "name")
#     project_node.add_child(select_node)
    
#     parsed_query = ParsedQuery(
#         "SELECT name FROM employees WHERE salary > 50000",
#         project_node
#     )
    
#     # Plan the query
#     planner = CostPlanner()
#     cost_plan = planner.plan_query(parsed_query)
#     planner.print_cost_breakdown(cost_plan)