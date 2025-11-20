"""
File ini tabel-tabelnya masih pakai dummy, jalankan python tests/UnitTestingCostPlaner.py atau python tests/test_or_with_v_a_r.py
- n_r: number of tuples in relation r
- b_r: number of blocks containing tuples of r
- l_r: size of a tuple of r
- f_r: blocking factor of r (number of tuples that fit in one block)
- V(A,r): number of distinct values for attribute A in relation r
"""

from model.query_tree import QueryTree, ConditionNode, LogicalNode, ColumnNode
from model.parsed_query import ParsedQuery
import math

class CostPlanner:
    def __init__(self, storage_manager=None):
        self.storage_manager = storage_manager

        # TODO ==================== [HAPUS SAAT INTEGRASI] ====================
        self.BLOCK_SIZE = 4096 
        self.PAGE_SIZE = 4096 
         # ====================================================================
        
        # Cache untuk menyimpan statistik temporary tables (hasil join, selection, dll)
        # Key: identifier string, Value: dict dengan n_r, b_r, f_r, v_a_r
        self.temp_table_stats = {}
        
    # =================== HELPER FUNCTIONS STATISTIK ===================
    
    def get_table_stats(self, table_name: str) -> dict:
        """
        mendapatkan statistik tabel dari storage manager atau temporary cache.
        parameter:
            table_name (str): nama tabel yang akan diambil statistiknya
        return:
            dict: {'n_r': int, 'b_r': int, 'l_r': int, 'f_r': int, 'v_a_r': dict}
        
        dipanggil oleh:
            cost_table_scan
        
        integrasi dengan SM: hapus bagian [HAPUS SAAT INTEGRASI] 
        dan uncomment bagian [UNCOMMENT SAAT INTEGRASI]
        """
        # Cek apakah ini temporary table (hasil join/selection)
        if table_name in self.temp_table_stats:
            return self.temp_table_stats[table_name]
        
        # TODO ==================== [UNCOMMENT SAAT INTEGRASI] ====================
        # Ketika SM  ready, UNCOMMENT blok di bawah ini:
        # memakai get_stats dari Storage Manager
        # 
        # if self.storage_manager:
        #     stats = self.storage_manager.get_stats(table_name)
        #     return {
        #         'n_r': stats.n_r,
        #         'b_r': stats.b_r,
        #         'l_r': stats.l_r,
        #         'f_r': stats.f_r,
        #         'v_a_r': stats.v_a_r  # dict: {column_name: distinct_count}
        #     }
        # 
        # # Jika table tidak ditemukan di SM
        # raise ValueError(f"Table '{table_name}' not found in Storage Manager")
        # ====================================================================
        
        # TODO ==================== [HAPUS SAAT INTEGRASI] ====================
        # Seluruh bagian dummy_stats di bawah ini HARUS DIHAPUS saat integrasi
        # Dummy statistics (untuk testing tanpa SM)
        dummy_stats = {
            "students": {
                'n_r': 10000,
                'b_r': 500,
                'l_r': 200,
                'f_r': 10,
                'v_a_r': {
                    'student_id': 10000,  # primary key: semuanya unique
                    'name': 9500,         # hampir unique
                    'age': 50,            # range 18-67
                    'gpa': 41,            # range 0.0-4.0 (dengan step 0.1)
                    'major': 20           # 20 jurusan berbeda
                }
            },
            "courses": {
                'n_r': 500,
                'b_r': 50,
                'l_r': 100,
                'f_r': 10,
                'v_a_r': {
                    'course_id': 500,     # primary key
                    'course_name': 500,   # unique
                    'credits': 4,         # 1,2,3,4 credits
                    'department': 15      # 15 departments
                }
            },
            "enrollments": {
                'n_r': 50000,
                'b_r': 2500,
                'l_r': 150,
                'f_r': 20,
                'v_a_r': {
                    'enrollment_id': 50000,  # primary key
                    'student_id': 10000,     # foreign key ke students
                    'course_id': 500,        # foreign key ke courses
                    'grade': 13,             # A+, A, A-, B+, B, B-, C+, C, C-, D, F, W, I
                    'semester': 20           # semester values
                }
            },
            "employees": {
                'n_r': 10000,  # 10,000 tuples
                'b_r': 1000,   # 1,000 blocks
                'l_r': 40,     # 40 bytes per tuple
                'f_r': 10,     # 10 tuples per block
                'v_a_r': {
                    'id': 10000,
                    'name': 9500,
                    'dept_id': 50,
                    'salary': 500
                }
            },
            "departments": {
                'n_r': 1000,
                'b_r': 50,
                'l_r': 80,
                'f_r': 20,
                'v_a_r': {
                    'id': 1000,
                    'name': 950,
                    'manager_id': 800
                }
            },
            "orders": {
                'n_r': 75000,
                'b_r': 5000,
                'l_r': 60,
                'f_r': 15,
                'v_a_r': {
                    'id': 75000,
                    'customer_id': 2000,
                    'status': 5
                }
            },
            "customers": {
                'n_r': 24000,
                'b_r': 2000,
                'l_r': 50,
                'f_r': 12,
                'v_a_r': {
                    'id': 24000,
                    'name': 23000,
                    'city': 200
                }
            },
            "products": {
                'n_r': 20000,
                'b_r': 800,
                'l_r': 100,
                'f_r': 25,
                'v_a_r': {
                    'id': 20000,
                    'name': 19000,
                    'category': 50
                }
            }
        }
        
        # Default stats untuk tabel yang tidak dikenal
        default_stats = {
            'n_r': 10000,
            'b_r': 500,
            'l_r': 80,
            'f_r': 10,
            'v_a_r': {}
        }
        
        # Handle TableReference object - extract name
        if hasattr(table_name, 'name'):
            table_name = table_name.name
        
        return dummy_stats.get(table_name.lower(), default_stats)
        # ==================== [AKHIR BAGIAN HAPUS] ====================
    
    def store_temp_stats(self, table_id: str, n_r: int, b_r: int, f_r: int, v_a_r: dict):
        """
        menyimpan statistik untuk temporary table (hasil join, selection, dll).
        
        parameter:
            table_id (str): identifier unik untuk temporary table
            n_r (int): jumlah tuples
            b_r (int): jumlah blocks
            f_r (int): blocking factor
            v_a_r (dict): distinct values per attribute
        
        return:
            none
        
        dipanggil oleh:
            cost_selection, cost_join
        """
        self.temp_table_stats[table_id] = {
            'n_r': n_r,
            'b_r': b_r,
            'l_r': 0,  # ga perlu untuk temporary
            'f_r': f_r,
            'v_a_r': v_a_r
        }
    
    # ======================= HELPER FUNCTIONS - DISPLAY/FORMATTING =======================
    
    def _calculate_logical_node_selectivity(self, logical_node: LogicalNode, v_a_r: dict) -> float:
        """
        menghitung selectivity untuk logical node secara rekursif.
        mendukung nested and/or.
        
        rumus:
            - and: s1 * s2 * ... * sn (conjunction)
            - or: 1 - (1-s1)*(1-s2)*...*(1-sn) (disjunction)
        
        parameter:
            logical_node (LogicalNode): node dengan operator and/or
            v_a_r (dict): {attribute: distinct_count}
        
        return:
            float: combined selectivity (0.0 - 1.0)
        
        dipanggil oleh:
            cost_selection
        """
        if logical_node.operator == "AND":
            # Conjunction: multiply selectivities
            result = 1.0
            for child in logical_node.childs:
                if isinstance(child, LogicalNode):
                    child_selectivity = self._calculate_logical_node_selectivity(child, v_a_r)
                    result *= child_selectivity
                elif isinstance(child, ConditionNode):
                    child_selectivity = self.estimate_selectivity(child, v_a_r)
                    result *= child_selectivity
            return result
        
        elif logical_node.operator == "OR":
            # Disjunction: 1 - (1-s1)*(1-s2)*...
            product = 1.0
            for child in logical_node.childs:
                if isinstance(child, LogicalNode):
                    child_selectivity = self._calculate_logical_node_selectivity(child, v_a_r)
                    product *= (1.0 - child_selectivity)
                elif isinstance(child, ConditionNode):
                    child_selectivity = self.estimate_selectivity(child, v_a_r)
                    product *= (1.0 - child_selectivity)
            return 1.0 - product
        
        else:
            # Unknown operator
            return 0.5
    
    def _logical_node_to_string(self, logical_node: LogicalNode) -> str:
        """
        konversi logical node ke string untuk display.
        mendukung nested structure.
        
        parameter:
            logical_node (LogicalNode): node yang akan dikonversi
        
        return:
            str: representasi string (contoh: "(age > 18 AND gpa > 3.0)")
        
        dipanggil oleh:
            cost_selection
        """
        parts = []
        for child in logical_node.childs:
            if isinstance(child, LogicalNode):
                parts.append(f"({self._logical_node_to_string(child)})")
            elif isinstance(child, ConditionNode):
                parts.append(self._condition_node_to_string(child))
        
        operator = f" {logical_node.operator} "
        return operator.join(parts)
    
    def _condition_node_to_string(self, cond_node: ConditionNode) -> str:
        """
        konversi condition node ke string untuk display.
        
        parameter:
            cond_node (ConditionNode): node yang akan dikonversi
        
        return:
            str: representasi string (contoh: "age > 18", "gpa = 3.5")
        
        dipanggil oleh:
            cost_selection
        """
        # Extract attribute
        if isinstance(cond_node.attr, ColumnNode):
            attr_str = f"{cond_node.attr.table}.{cond_node.attr.column}" if cond_node.attr.table else cond_node.attr.column
        else:
            attr_str = str(cond_node.attr)
        
        # Extract value
        if isinstance(cond_node.value, ColumnNode):
            # Handle parser bug: decimal jadi ColumnNode(column='5', table='3') untuk 3.5
            if cond_node.value.table and cond_node.value.table.isdigit():
                value_str = f"{cond_node.value.table}.{cond_node.value.column}"
            else:
                value_str = f"{cond_node.value.table}.{cond_node.value.column}" if cond_node.value.table else cond_node.value.column
        else:
            value_str = str(cond_node.value)
        
        return f"{attr_str} {cond_node.op} {value_str}"
    
    # ======================= SELECTIVITY ESTIMATION =======================
    
    def estimate_selectivity(self, condition: ConditionNode, v_a_r: dict = None) -> float:
        """
        estimasi selectivity dari kondisi selection menggunakan v(a,r).
        
        rumus:
            - equality (a = value): selectivity = 1/v(a,r)
            - inequality (a ≠ value): selectivity = 1 - (1/v(a,r))
            - comparison (a > value): selectivity ≈ 0.5 (tanpa histogram)
            - like: selectivity ≈ 0.2
            - in: selectivity ≈ n/v(a,r) dimana n = jumlah values
        
        parameter:
            condition (ConditionNode): node dengan attr, op, value
            v_a_r (dict): {attribute: distinct_count} dari tabel
        
        return:
            float: selectivity (0.0 - 1.0)
        
        dipanggil oleh:
            _calculate_logical_node_selectivity
        """
        if v_a_r is None:
            v_a_r = {}
        
        # Get attribute name dari ConditionNode.attr (ColumnNode)
        if isinstance(condition.attr, ColumnNode):
            attribute = condition.attr.column
        else:
            # Fallback jika attr bukan ColumnNode
            attribute = None
        
        op = condition.op
        
        # Equality condition: σ_A=v(r)
        # Formula: selectivity = 1 / V(A,r)
        if op == "=":
            if attribute and attribute in v_a_r:
                v_a = v_a_r[attribute]
                return 1.0 / v_a if v_a > 0 else 0.1
            return 0.1
        
        # Inequality: σ_A≠v(r)
        elif op in ["!=", "<>"]:
            if attribute and attribute in v_a_r:
                v_a = v_a_r[attribute]
                return 1.0 - (1.0 / v_a) if v_a > 0 else 0.9
            return 0.9
        
        # Comparison operators: >, <, >=, <=
        # Formula ideal: (max - v) / (max - min)
        # Tanpa histogram: asumsi distribusi uniform → 0.5
        elif op in [">", "<", ">=", "<="]:
            return 0.5
        
        # Pattern matching: LIKE
        elif op.upper() == "LIKE":
            return 0.2
        
        # IN clause: σ_A IN (v1,v2,...,vn)(r)
        # Formula: selectivity = n / V(A,r)
        # TODO: Harus detect actual number of values dari condition.value
        # TODO: Gimana kalau IN nya juga bukan dari value distinct tabel, tapi literal?
        elif op.upper() == "IN":
            if attribute and attribute in v_a_r:
                # Simple heuristic: asumsi 5 values
                num_values = 5
                v_a = v_a_r[attribute]
                return min(1.0, num_values / v_a) if v_a > 0 else 0.15
            return 0.15
        
        # Default: konservatif
        return 0.5
    

    
    # ================================================ COST FUNCTIONS ================================================
    
    def cost_table_scan(self, node: QueryTree) -> dict:
        """
        cost untuk full table scan.
        
        rumus:
            cost = b_r (jumlah blocks yang harus dibaca)
        
        parameter:
            node (QueryTree): node dengan type="TABLE"
        
        return:
            dict: {cost, n_r, b_r, f_r, v_a_r, operation, description}
        
        dipanggil oleh:
            calculate_cost
        """
        table_name = node.val
        stats = self.get_table_stats(table_name)
        
        # Extract display name from TableReference if needed
        display_name = table_name.name if hasattr(table_name, 'name') else table_name
        
        return {
            "operation": "TABLE_SCAN",
            "table": display_name,
            "cost": stats['b_r'],
            "n_r": stats['n_r'],
            "b_r": stats['b_r'],
            "f_r": stats['f_r'],
            "v_a_r": stats['v_a_r'],
            "description": f"Full scan of table {display_name}"
        }
    
    def cost_selection(self, node: QueryTree, input_cost: dict) -> dict:
        """
        cost untuk operasi selection (σ - sigma).
        mendukung logical node (and/or) dan condition node.
        
        rumus:
            - output tuples: n_r * selectivity
            - output blocks: ceil(output_tuples / f_r)
            - cost: cost(input) (tidak ada tambahan i/o)
            - v(a, σ_θ(r)): min(v(a,r), n_r(output))
        
        parameter:
            node (QueryTree): node dengan type="SIGMA"
            input_cost (dict): cost info dari child node
        
        return:
            dict: {cost, n_r, b_r, f_r, v_a_r, selectivity, operation, description}
        
        dipanggil oleh:
            calculate_cost
        
        todo: implementasi index scan jika sm ada info tinggi tree
              cost = h_i + (selectivity * b_r)
        """
        condition = node.val
        
        input_n_r = input_cost.get("n_r", 1000)
        input_b_r = input_cost.get("b_r", 100)
        input_f_r = input_cost.get("f_r", 10)
        input_v_a_r = input_cost.get("v_a_r", {})
        
        # Calculate selectivity based on condition type
        if isinstance(condition, LogicalNode):
            # LogicalNode: Use recursive helper for AND/OR (handles nesting)
            selectivity = self._calculate_logical_node_selectivity(condition, input_v_a_r)
            condition_str = self._logical_node_to_string(condition)
        
        elif isinstance(condition, ConditionNode):
            # Single ConditionNode
            selectivity = self.estimate_selectivity(condition, input_v_a_r)
            condition_str = self._condition_node_to_string(condition)
        
        else:
            # Unknown format - should not happen with current parser
            raise ValueError(f"Unexpected condition type: {type(condition)}. Expected LogicalNode or ConditionNode.")
        
        # Estimasi output size
        # Formula: n_r(σ) = n_r(input) * selectivity
        output_n_r = max(1, int(input_n_r * selectivity))
        
        # Estimasi output blocks
        # Formula: b_r = ceil(n_r / f_r)
        output_b_r = max(1, math.ceil(output_n_r / input_f_r)) if input_f_r > 0 else input_b_r
        
        # V(A,r) untuk output
        # Formula dari slide: "If the selection condition θ is of the form A op r
        #                      estimated V(A, σ_θ(r)) = V(A,r) * s"
        # where s is the selectivity of the selection.
        # 
        # "In all the other cases: use approximate estimate of
        #  min(V(A,r), n·σ_θ(r))"
        output_v_a_r = {}
        for attr, v_val in input_v_a_r.items():
            # Formula: min(V(A,r), n_r(σ_θ(r)))
            # Karena kita tidak bisa detect "A op r" secara spesifik,
            # gunakan approximate: min(V(A,r), n_r(output))
            output_v_a_r[attr] = min(v_val, output_n_r)
        
        # Cost = cost input (selection tidak menambah I/O)
        total_cost = input_cost.get("cost", 0)
        
        # Generate unique ID untuk temporary result
        temp_id = f"sigma_{id(node)}"
        self.store_temp_stats(temp_id, output_n_r, output_b_r, input_f_r, output_v_a_r)
        
        return {
            "operation": "SELECTION",
            "condition": condition_str,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": input_f_r,
            "v_a_r": output_v_a_r,
            "selectivity": selectivity,
            "description": f"Filter: {condition_str} (selectivity={selectivity:.2f})"
        }
    
    def cost_projection(self, node: QueryTree, input_cost: dict) -> dict:
        """
        cost untuk operasi projection (π - pi).
        
        rumus:
            - tanpa distinct: size = n_r (sama dengan input)
            - dengan distinct: size = v(a,r)
            - cost: cost(input) (tidak ada tambahan i/o)
        
        parameter:
            node (QueryTree): node dengan type="PROJECT"
            input_cost (dict): cost info dari child node
        
        return:
            dict: {cost, n_r, b_r, f_r, v_a_r, operation, description}
        
        dipanggil oleh:
            calculate_cost
        
        catatan: asumsi tidak ada distinct (belum diimplementasi)
        """
        columns = node.val
        
        # Projection biasanya tidak mengubah jumlah tuples (kecuali DISTINCT)
        # Asumsi: tidak ada DISTINCT (karena tidak ada info di query tree)
        output_n_r = input_cost.get("n_r", 1000)
        output_b_r = input_cost.get("b_r", 100)
        output_f_r = input_cost.get("f_r", 10)
        
        # V(A,r) untuk projected attributes tetap sama
        input_v_a_r = input_cost.get("v_a_r", {})
        output_v_a_r = input_v_a_r.copy()  # preserve distinct values
        
        # Cost = cost input (projection overhead minimal)
        total_cost = input_cost.get("cost", 0)
        
        return {
            "operation": "PROJECTION",
            "columns": columns,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": output_f_r,
            "v_a_r": output_v_a_r,
            "description": f"Project columns: {columns}"
        }
    
    def cost_join(self, node: QueryTree, left_cost: dict, right_cost: dict) -> dict:
        """
        cost untuk operasi join (⋈ - bowtie).
        menggunakan nested-loop join sebagai default.
        
        rumus nested-loop:
            cost = b_r(r) + n_r(r) * b_r(s)
        
        rumus estimasi join size:
            - case 1 (no common): n_r(r ⋈ s) = n_r(r) * n_r(s)
            - case 2 (key): n_r(r ⋈ s) ≤ n_r(s)
            - case 3 (foreign key): n_r(r ⋈ s) = n_r(s) Note: s adalah table dengan foreign key
            - case 4 (not key): n_r(r ⋈ s) = (n_r(r) * n_r(s)) / max(v(a,r), v(a,s))
        
        rumus v(a, r ⋈ s):
            - jika a dari r: v(a, r⋈s) = min(v(a,r), n_r(r⋈s))
            - jika a dari r dan s: gunakan formula kombinasi
        
        parameter:
            node (QueryTree): node dengan type="JOIN"
            left_cost (dict): cost info dari left child
            right_cost (dict): cost info dari right child
        
        return:
            dict: {cost, n_r, b_r, f_r, v_a_r, join_cost, operation, description}
        
        dipanggil oleh:
            calculate_cost
        
        todo: implementasi hash join dan sort-merge join
              - hash join: 3 * (b_r(r) + b_r(s))
              - sort-merge: b_r(r) + b_r(s) + sort_cost
        """
        left_n_r = left_cost.get("n_r", 1000)
        left_b_r = left_cost.get("b_r", 100)
        left_v_a_r = left_cost.get("v_a_r", {})
        
        right_n_r = right_cost.get("n_r", 1000)
        right_b_r = right_cost.get("b_r", 100)
        right_v_a_r = right_cost.get("v_a_r", {})
        
        # === COST CALCULATION (Nested-Loop Join) ===
        # Formula: Cost = b_r(R) + n_r(R) * b_r(S)
        join_cost = left_b_r + (left_n_r * right_b_r)
        total_cost = left_cost.get("cost", 0) + right_cost.get("cost", 0) + join_cost
        
        # === SIZE ESTIMATION ===
        # Karena kita tidak tahu join attribute atau key info,
        # gunakan Case 4: R ∩ S = {A} not a key
        # Formula: n_r(R ⋈ S) = (n_r(R) * n_r(S)) / max(V(A,R), V(A,S))
        
        # Heuristic: asumsi ada common attribute dengan V(A,R) dan V(A,S)
        # Ambil rata-rata dari distinct values sebagai estimasi
        avg_v_left = sum(left_v_a_r.values()) / len(left_v_a_r) if left_v_a_r else 100
        avg_v_right = sum(right_v_a_r.values()) / len(right_v_a_r) if right_v_a_r else 100
        max_v = max(avg_v_left, avg_v_right)
        
        if max_v > 0:
            # Formula: n_r(R ⋈ S) = (n_r(R) * n_r(S)) / max(V(A,R), V(A,S))
            output_n_r = int((left_n_r * right_n_r) / max_v)
        else:
            # Fallback: cartesian product dengan selectivity 0.1
            output_n_r = int(left_n_r * right_n_r * 0.1)
        
        # Estimasi blocking factor untuk join result
        # Asumsi: f_r = average dari kedua input
        output_f_r = (left_cost.get("f_r", 10) + right_cost.get("f_r", 10)) // 2
        
        # Estimasi blocks untuk join result
        # Formula: b_r = ceil(n_r / f_r)
        output_b_r = max(1, math.ceil(output_n_r / output_f_r)) if output_f_r > 0 else (left_b_r + right_b_r)
        
        # === V(A, R ⋈ S) ESTIMATION ===
        # Formula dari slide:
        # 1. "If all attributes in A are from r:
        #     estimated V(A, r ⋈ s) = min(V(A,r), n_r⋈s)"
        # 2. "If A contains attributes A1 from r and A2 from s, then estimated:
        #     V(A,r⋈s) = min(V(A1,r)*V(A2-A1,s), V(A1-A2,r)*V(A2,s), n_r⋈s)"
        #
        # Implementation: Karena kita tidak tahu attribute overlap,
        # gunakan formula 1 untuk attributes dari masing-masing table
        output_v_a_r = {}
        
        # Attributes dari left table (R)
        for attr, v_val in left_v_a_r.items():
            # Formula: V(A, r⋈s) = min(V(A,r), n_r⋈s)
            output_v_a_r[attr] = min(v_val, output_n_r)
        
        # Attributes dari right table (S)
        for attr, v_val in right_v_a_r.items():
            if attr in output_v_a_r:
                # Join attribute (common attribute)
                # Formula: V(A, r⋈s) = min(V(A,r), V(A,s))
                # Karena join on common key
                output_v_a_r[attr] = min(output_v_a_r[attr], v_val, output_n_r)
            else:
                # Attribute only from S
                # Formula: V(A, r⋈s) = min(V(A,s), n_r⋈s)
                output_v_a_r[attr] = min(v_val, output_n_r)
        
        # Store temporary stats
        temp_id = f"join_{id(node)}"
        self.store_temp_stats(temp_id, output_n_r, output_b_r, output_f_r, output_v_a_r)
        
        return {
            "operation": "JOIN",
            "join_type": node.val if node.val else "INNER",
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": output_f_r,
            "v_a_r": output_v_a_r,
            "join_cost": join_cost,
            "description": f"Nested-Loop Join (cost={join_cost})"
        }
    
    def cost_sort(self, node: QueryTree, input_cost: dict) -> dict:
        """
        cost untuk operasi sort (order by).
        menggunakan external merge sort.
        
        rumus:
            - in-memory (b_r ≤ m): cost = b_r
            - external: cost = 2 * b_r * (1 + ⌈log_{m-1}(b_r/m)⌉)
            - m = jumlah blocks di memory buffer
        
        parameter:
            node (QueryTree): node dengan type="SORT"
            input_cost (dict): cost info dari child node
        
        return:
            dict: {cost, n_r, b_r, f_r, v_a_r, sort_cost, operation, description}
        
        dipanggil oleh:
            calculate_cost
        
        catatan: asumsi m = 100 blocks (harus diganti dengan config dari sm)
        """
        input_b_r = input_cost.get("b_r", 100)
        input_n_r = input_cost.get("n_r", 1000)
        
        # TODO: Asumsi memory buffer size dari Storage Manager
        # Seharusnya didapat dari storage_manager.get_buffer_pool_size() atau config
        # Asumsi sementara: memory dapat hold 100 blocks
        M = 100
        
        if input_b_r <= M:
            # In-memory sort: hanya satu pass
            sort_cost = input_b_r
        else:
            # External merge sort
            # Formula: 2 * b_r * (1 + ⌈log_{M-1}(b_r/M)⌉)
            num_runs = math.ceil(input_b_r / M)
            num_passes = math.ceil(math.log(num_runs, M - 1)) if M > 1 else 1
            sort_cost = 2 * input_b_r * (1 + num_passes)
        
        total_cost = input_cost.get("cost", 0) + sort_cost
        
        # Sort tidak mengubah n_r, b_r, atau v_a_r
        return {
            "operation": "SORT",
            "sort_key": node.val,
            "cost": total_cost,
            "n_r": input_cost.get("n_r", 1000),
            "b_r": input_cost.get("b_r", 100),
            "f_r": input_cost.get("f_r", 10),
            "v_a_r": input_cost.get("v_a_r", {}),
            "sort_cost": sort_cost,
            "description": f"External Merge Sort (cost={sort_cost})"
        }
    
    def cost_limit(self, node: QueryTree, input_cost: dict) -> dict:
        """
        cost untuk operasi limit.
        mendukung early termination.
        
        rumus:
            - output tuples: min(limit, n_r)
            - cost reduction: cost * (limit / n_r)
        
        parameter:
            node (QueryTree): node dengan type="LIMIT"
            input_cost (dict): cost info dari child node
        
        return:
            dict: {cost, n_r, b_r, f_r, v_a_r, operation, description}
        
        dipanggil oleh:
            calculate_cost
        """
        # Handle different types for limit value
        if isinstance(node.val, int):
            limit_val = node.val
        elif isinstance(node.val, str) and node.val.isdigit():
            limit_val = int(node.val)
        else:
            limit_val = 100  # default
            
        input_n_r = input_cost.get("n_r", 1000)
        
        # Output limited to min(limit, n_r)
        output_n_r = min(limit_val, input_n_r)
        
        # Cost reduction dengan early termination
        if input_n_r > 0:
            reduction_factor = min(1.0, output_n_r / input_n_r)
        else:
            reduction_factor = 1.0
        
        total_cost = input_cost.get("cost", 0) * reduction_factor
        
        # Blocks juga reduced
        output_b_r = max(1, int(input_cost.get("b_r", 100) * reduction_factor))
        
        return {
            "operation": "LIMIT",
            "limit": limit_val,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": input_cost.get("f_r", 10),
            "v_a_r": input_cost.get("v_a_r", {}),
            "description": f"Limit to {limit_val} records"
        }
    
    def cost_aggregation(self, node: QueryTree, input_cost: dict) -> dict:
        """
        cost untuk operasi aggregation (group by, count, sum, avg, dll).
        menggunakan hash-based aggregation.
        
        rumus:
            - output size: v(a,r) untuk group by a
            - cost: cost(input) + b_r (build hash table)
            - v(a,r) untuk min/max: min(v(a,r), v(g,r))
        
        parameter:
            node (QueryTree): node dengan type="GROUP" atau "AGGREGATE"
            input_cost (dict): cost info dari child node
        
        return:
            dict: {cost, n_r, b_r, f_r, v_a_r, agg_cost, operation, description}
        
        dipanggil oleh:
            calculate_cost
        """
        input_n_r = input_cost.get("n_r", 1000)
        input_b_r = input_cost.get("b_r", 100)
        input_v_a_r = input_cost.get("v_a_r", {})
        
        # Estimasi output size
        # Formula: output = V(A,r) untuk GROUP BY A
        # Heuristic: asumsi 10% dari input tuples (jika tidak tahu attribute)
        if input_v_a_r:
            # Ambil average distinct values sebagai estimasi output groups
            avg_v = sum(input_v_a_r.values()) / len(input_v_a_r)
            output_n_r = int(min(avg_v, input_n_r * 0.1))
        else:
            output_n_r = max(1, int(input_n_r * 0.1))
        
        # Hash table build cost
        agg_cost = input_b_r
        total_cost = input_cost.get("cost", 0) + agg_cost
        
        # Output blocks
        output_f_r = input_cost.get("f_r", 10)
        output_b_r = max(1, math.ceil(output_n_r / output_f_r)) if output_f_r > 0 else input_b_r
        
        # V(A,r) untuk aggregated values
        # "For min(A) and max(A), the number of distinct values can be estimated as 
        #  min(V(A,r), V(G,r)) where G denotes grouping attributes"
        output_v_a_r = {}
        for attr, v_val in input_v_a_r.items():
            output_v_a_r[attr] = min(v_val, output_n_r)
        
        return {
            "operation": "AGGREGATION",
            "aggregate": node.val,
            "cost": total_cost,
            "n_r": output_n_r,
            "b_r": output_b_r,
            "f_r": output_f_r,
            "v_a_r": output_v_a_r,
            "agg_cost": agg_cost,
            "description": f"Aggregation: {node.val} (cost={agg_cost})"
        }
    
    # =================================================================== MAIN COST PLANNING ======================================================================
    
    def calculate_cost(self, node: QueryTree) -> dict:
        """
        menghitung cost untuk query tree secara rekursif.
        bottom-up approach: hitung children dulu, lalu parent.
        
        parameter:
            node (QueryTree): node untuk dihitung costnya
        
        return:
            dict: {operation, cost, n_r, b_r, f_r, v_a_r, description}
        
        dipanggil oleh:
            get_cost
        """
        if node.type == "TABLE":
            return self.cost_table_scan(node)
        
        elif node.type == "SIGMA" or node.type == "SELECT":
            # Selection operation
            # NOTE: Sekarang support LogicalNode (AND/OR) dan ConditionNode
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_selection(node, child_cost)
        
        elif node.type == "PROJECT":
            # Projection operation
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_projection(node, child_cost)
        
        elif node.type == "JOIN":
            # Join operation
            if len(node.childs) < 2:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            left_cost = self.calculate_cost(node.childs[0])
            right_cost = self.calculate_cost(node.childs[1])
            return self.cost_join(node, left_cost, right_cost)
        
        elif node.type == "SORT" or node.type == "ORDER":
            # Sort operation
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_sort(node, child_cost)
        
        elif node.type == "LIMIT":
            # Limit operation
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_limit(node, child_cost)
        
        elif node.type in ["GROUP", "AGGREGATE", "COUNT", "SUM", "AVG"]:
            # Aggregation operations
            if not node.childs:
                return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
            child_cost = self.calculate_cost(node.childs[0])
            return self.cost_aggregation(node, child_cost)
        
        else:
            # Unknown operation, just pass through child cost
            if node.childs:
                return self.calculate_cost(node.childs[0])
            return {"cost": 0, "n_r": 0, "b_r": 0, "f_r": 1, "v_a_r": {}}
    

    def get_cost(self, query: ParsedQuery) -> int:
        """
        fungsi utama untuk mendapatkan cost dari query.
        
        parameter:
            query (ParsedQuery): object dengan query_tree
        
        return:
            int: total cost (dalam unit block i/o)
        
        dipanggil oleh:
            OptimizationEngine.get_cost (QueryOptimizer.py)
        
        usage:
            planner = CostPlanner()
            cost = planner.get_cost(parsed_query)
        """
        if not query.query_tree:
            raise ValueError("No query tree available in ParsedQuery")
        
        cost_info = self.calculate_cost(query.query_tree)
        return int(cost_info.get("cost", 0))
    
    
    def plan_query(self, parsed_query: ParsedQuery) -> dict:
        """
        mendapatkan detailed cost breakdown dari query.
        untuk cost integer saja, gunakan get_cost().
        
        parameter:
            parsed_query (ParsedQuery): object dengan query_tree
        
        return:
            dict: {query, total_cost, estimated_records, blocks_read, details}
        
        dipanggil oleh:
            user code (untuk debugging/analysis)
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
            "estimated_records": cost_info.get("n_r", 0),
            "blocks_read": cost_info.get("b_r", 0),
            "details": cost_info
        }
    
    def print_cost_breakdown(self, cost_plan: dict):
        """
        print cost breakdown dengan format yang rapi.
        
        parameter:
            cost_plan (dict): hasil dari plan_query()
        
        return:
            none (print ke stdout)
        
        dipanggil oleh:
            user code (untuk debugging)
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
        helper untuk print nested details dengan indentasi.
        
        parameter:
            details (dict): cost breakdown details
            indent (int): level indentasi
        
        return:
            none (print ke stdout)
        
        dipanggil oleh:
            print_cost_breakdown (line ~912)
        """
        prefix = "  " * indent
        print(f"{prefix}Operation: {details.get('operation', 'Unknown')}")
        print(f"{prefix}Cost: {details.get('cost', 0):.2f}")
        print(f"{prefix}Description: {details.get('description', 'N/A')}")
        print()



