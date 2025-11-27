from model.parsed_query import ParsedQuery
from model.query_tree import (
    QueryTree,
    SetClause,
    TableReference,
    InsertData,
    CreateTableData,
    DropTableData
)
from helper.helper import (
    fold_selection_with_cartesian,
    merge_selection_into_join,
    make_join_commutative,
    associate_natural_join,
    associate_theta_join,
    choose_best,
    build_join_tree,
    plan_cost,
    _tables_under,
    _some_permutations,
    _get_columns_from_select,
    validate_query,
    _get_condition_from_where,
    _get_limit,
    _get_column_from_group_by,
    _parse_from_clause,
    _extract_set_conditions,
    _extract_table_update,
    _extract_table_delete,
    _extract_table_insert,
    _extract_columns_insert,
    _extract_values_insert,
    decompose_conjunctive_selection,
    swap_selection_order,
    eliminate_redundant_projections,
    push_selection_through_join_single,
    push_selection_through_join_split,
    push_projection_through_join_simple,
    push_projection_through_join_with_join_attrs,
    _get_order_by_info,
    _parse_drop_table,
    _parse_create_table,
    parse_where_condition,
    parse_columns_from_string,
    parse_order_by_string,
    parse_group_by_string,
    parse_insert_columns_string,
    parse_insert_values_string,
    _theta_pred
)

from helper.stats import get_stats

class OptimizationEngine:
    
    # parse sql query string dan return ParsedQuery object
    def parse_query(self, query: str) -> ParsedQuery:
        if not query:
            raise Exception("Query is empty")
        
        is_valid, message = validate_query(query)
        if not is_valid:
            raise Exception(f"Query validation failed: {message}")
        
        q = query.strip().rstrip(';').strip()
        
        parse_result = ParsedQuery(query=query)
        
        try:
            if q.upper().startswith("SELECT"):
                current_root = None
                last_node = None
                
                # 1. parse PROJECT (SELECT columns)
                columns_str = _get_columns_from_select(q)
                if columns_str != "*":
                    columns_list = parse_columns_from_string(columns_str)
                    proj = QueryTree(type="PROJECT", val=columns_list)
                    current_root = proj
                    last_node = proj
                
                # 2. parse LIMIT
                if "LIMIT" in q.upper():
                    limit_val = _get_limit(q)
                    lim = QueryTree(type="LIMIT", val=limit_val)
                    
                    if last_node:
                        last_node.add_child(lim)
                    else:
                        current_root = lim
                    last_node = lim
                
                # 3. parse ORDER BY
                if "ORDER BY" in q.upper():
                    order_info_str = _get_order_by_info(q)
                    order_by_list = parse_order_by_string(order_info_str)
                    
                    sort = QueryTree(type="SORT", val=order_by_list)
                    
                    if last_node:
                        last_node.add_child(sort)
                    else:
                        current_root = sort
                    last_node = sort
                
                # 4. parse GROUP BY
                if "GROUP BY" in q.upper():
                    group_col_str = _get_column_from_group_by(q)
                    group_by_list = parse_group_by_string(group_col_str)
                    
                    group = QueryTree(type="GROUP", val=group_by_list)
                    
                    if last_node:
                        last_node.add_child(group)
                    else:
                        current_root = group
                    last_node = group
                
                # 5. parse WHERE (SIGMA)
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=condition)
                    
                    if last_node:
                        last_node.add_child(sigma)
                    else:
                        current_root = sigma
                    last_node = sigma
                
                # 6. parse FROM
                if last_node:
                    from_node = _parse_from_clause(q)
                    last_node.add_child(from_node)
                else:
                    from_node = _parse_from_clause(q)
                    current_root = from_node
                
                parse_result.query_tree = current_root if current_root else from_node
          
            elif q.upper().startswith("UPDATE"):
                current_root = None
                last_node = None
                
                # 1. parse SET 
                set_conditions_list = _extract_set_conditions(q)
                
                set_clauses = []
                for set_cond in set_conditions_list:
                    if '=' in set_cond:
                        eq_pos = set_cond.find('=')
                        column = set_cond[:eq_pos].strip()
                        value = set_cond[eq_pos + 1:].strip()
                        set_clauses.append(SetClause(column, value))
                
                update_node = QueryTree(type="UPDATE", val=set_clauses)
                
                current_root = update_node
                last_node = update_node
                
                # 2. parse WHERE (optional)
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=condition)
                    
                    last_node.add_child(sigma)
                    last_node = sigma
                
                # 3. parse table name
                table_str = _extract_table_update(q)
                table_ref = TableReference(table_str)
                
                table_node = QueryTree(type="TABLE", val=table_ref)
                
                last_node.add_child(table_node)
                
                parse_result.query_tree = current_root

            elif q.upper().startswith("DELETE"):
                current_root = None
                last_node = None
                
                # 1. DELETE node
                delete_node = QueryTree(type="DELETE", val=None)
                current_root = delete_node
                last_node = delete_node
                
                # 2. parse WHERE
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=condition)
                    
                    last_node.add_child(sigma)
                    last_node = sigma
                
                # 3. parse table
                table_str = _extract_table_delete(q)
                table_ref = TableReference(table_str)
                
                table_node = QueryTree(type="TABLE", val=table_ref)
                
                last_node.add_child(table_node)
                parse_result.query_tree = current_root
            
            elif q.upper().startswith("INSERT"):
                table_name = _extract_table_insert(q)
                columns_str = _extract_columns_insert(q)
                values_str = _extract_values_insert(q)
                
                columns_list = parse_insert_columns_string(columns_str)
                values_list = parse_insert_values_string(values_str)
                
                insert_data = InsertData(table_name, columns_list, values_list)
                insert_node = QueryTree(type="INSERT", val=insert_data)
                
                parse_result.query_tree = insert_node
            
            elif q.upper().startswith("CREATE"):
                table_name, columns, primary_key, foreign_keys = _parse_create_table(q)
                
                create_data = CreateTableData(table_name, columns, primary_key, foreign_keys)
                create_node = QueryTree(type="CREATE_TABLE", val=create_data)
                
                parse_result.query_tree = create_node

            elif q.upper().startswith("DROP"):
                table_name, is_cascade = _parse_drop_table(q)
                
                drop_data = DropTableData(table_name, is_cascade)
                drop_node = QueryTree(type="DROP_TABLE", val=drop_data)
                
                parse_result.query_tree = drop_node

            elif q.upper().startswith("BEGIN"):
                begin_node = QueryTree(type="BEGIN_TRANSACTION", val=None)
                parse_result.query_tree = begin_node
            
            elif q.upper().startswith("COMMIT"):
                commit_node = QueryTree(type="COMMIT", val=None)
                parse_result.query_tree = commit_node
            
            elif q.upper().startswith("ROLLBACK"):
                rollback_node = QueryTree(type="ROLLBACK", val=None)
                parse_result.query_tree = rollback_node
            
            else:
                raise Exception(f"Unsupported query type: {q[:20]}")
                
        except Exception as e:
            raise Exception(f"Error parsing query: {str(e)}")
        
        return parse_result

    def optimize_query(self, parsed_query: ParsedQuery) -> ParsedQuery:
        if not parsed_query or not parsed_query.query_tree:
            return parsed_query

        # 1) START WITH ORIGINAL ROOT
        root = parsed_query.query_tree

        # 2) APPLY NON-JOIN RULES (push-down & simplify)
        # do fixed-point iterations
        changed = True
        max_iter = 5
        while changed and max_iter > 0:
            prev = repr(root)
            # recursive application
            root = self._apply_non_join_rules(root)
            changed = (repr(root) != prev)
            max_iter -= 1

        # 3) APPLY JOIN RULES (fold selection, assoc, commutative)
        root = fold_selection_with_cartesian(root)
        root = merge_selection_into_join(root)
        root = make_join_commutative(root)
        root = associate_natural_join(root)
        root = associate_theta_join(root)

        # 4) TABLE EXTRACTION
        tables = list(_tables_under(root)) if root else []
        if len(tables) <= 1:
            return ParsedQuery(parsed_query.query, root)

        # 5) BUILD JOIN CONDITIONS FROM CURRENT TREE
        join_conditions = self._extract_join_conditions_from_tree(root)

        # 6) ORDER ENUMERATION & PLAN GENERATION
        orders = _some_permutations(tables, max_count=10)
        plans = []
        for order in orders:
            plan = build_join_tree(order, join_conditions)
            if plan:
                plans.append(plan)

        if not plans:
            return ParsedQuery(parsed_query.query, root)

        # 7) COST MODEL: PICK BEST PLAN
        stats = get_stats()
        best = choose_best(plans, stats)

        # 8) RETURN BEST PLAN AS FINAL OPTIMIZED QUERY TREE
        return ParsedQuery(parsed_query.query, best)

    def get_cost(self, parsed_query: ParsedQuery) -> int:
        if not parsed_query or not parsed_query.query_tree:
            return 0
        
        # TODO: ==================== [UNCOMMENT SAAT INTEGRASI SM] ====================
        # Uncomment blok di bawah untuk menggunakan StorageManager:
        #
        # try:
        #     from StorageManager import StorageManager  # sesuaikan path SM
        #     from helper.cost import CostPlanner
        #     
        #     # Inisialisasi StorageManager
        #     storage_manager = StorageManager(base_path='data')  # sesuaikan path data nya
        #     
        #     # Inisialisasi CostPlanner dengan SM
        #     cost_planner = CostPlanner(storage_manager=storage_manager)
        #     
        #     # Return cost dari CostPlanner
        #     return cost_planner.get_cost(parsed_query)
        # 
        # except ImportError:
        #     # Fallback ke dummy stats jika SM tidak tersedia
        #     pass
        # ===========================================================================
        
        # ini nanti hapus aja setelah integrasi SM
        root = parsed_query.query_tree
        stats = get_stats()
        return plan_cost(root, stats)
    
    def optimize_query_non_join(self, pq: ParsedQuery) -> ParsedQuery:
        if not pq or not pq.query_tree:
            return pq
        
        root = pq.query_tree
        # nyobain aja max iterasi 5
        max_iterations = 5
        for _ in range(max_iterations):
            old_root = root
            
            root = self._apply_non_join_rules(root)
            
            if root == old_root:
                break
        
        return ParsedQuery(pq.query, root)

    def _apply_non_join_rules(self, node: QueryTree) -> QueryTree:
        if not node:
            return node
        
        # rekursif
        for i, child in enumerate(node.childs):
            node.childs[i] = self._apply_non_join_rules(child)
        
        node = decompose_conjunctive_selection(node)
        node = eliminate_redundant_projections(node)
        node = swap_selection_order(node)
        node = push_selection_through_join_single(node)
        node = push_selection_through_join_split(node)
        node = push_projection_through_join_simple(node)
        node = push_projection_through_join_with_join_attrs(node)
        
        return node
    
    def _extract_join_conditions_from_tree(self, node: QueryTree) -> dict:
        mapping = {}

        def walk(n):
            if n is None:
                return

            if n.type == "JOIN":
                pred = ""

                # coba ambil menggunakan _theta_pred
                try:
                    pred = _theta_pred(n)
                except:
                    pred = ""

                # fallback
                if not pred:
                    if hasattr(n.val, "condition"):
                        pred = str(n.val.condition)
                    elif isinstance(n.val, str):
                        s = n.val.strip()
                        if s.upper().startswith("THETA:"):
                            pred = s.split(":",1)[1].strip()
                        elif s.upper() != "CARTESIAN":
                            pred = s

                if pred:
                    left_list = list(_tables_under(n.childs[0]))
                    right_list = list(_tables_under(n.childs[1]))

                    if left_list and right_list:
                        # pasangan utama
                        key = frozenset({left_list[0], right_list[0]})
                        mapping[key] = pred

                        # pasangan tambahan (mencegah predicate hilang)
                        for lt in left_list:
                            for rt in right_list:
                                mapping.setdefault(frozenset({lt, rt}), pred)

            # recursive
            for c in getattr(n, "childs", []):
                walk(c)

        walk(node)
        return mapping