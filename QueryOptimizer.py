from model.parsed_query import ParsedQuery
from model.query_tree import QueryTree
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
    _get_column_from_order_by,
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
    _parse_create_table
)

from helper.stats import get_stats

class OptimizationEngine:
    
    def parse_query(self, query: str) -> ParsedQuery:

        if not query:
            raise Exception("Query is empty")
        
        # Query validation
        is_valid, message = validate_query(query)
        if not is_valid:
            raise Exception(f"Query validation failed: {message}")
        
        # Remove semicolon and extra whitespaces
        q = query.strip().rstrip(';').strip()
        
        parse_result = ParsedQuery(query=query)
        
        try:
            # Parse SELECT statement
            if q.upper().startswith("SELECT"):
                # Init nodes
                current_root = None
                last_node = None
                
                # 1. Parse PROJECT (SELECT columns)
                columns = _get_columns_from_select(q)
                if columns != "*":  # only create PROJECT node if not selecting all columns
                    proj = QueryTree(type="PROJECT", val=columns)
                    current_root = proj
                    last_node = proj
                
                # 2. Parse LIMIT
                if "LIMIT" in q.upper():
                    limit_val = _get_limit(q)
                    lim = QueryTree(type="LIMIT", val=str(limit_val))
                    
                    if last_node:
                        last_node.add_child(lim)
                    else:
                        current_root = lim
                    last_node = lim
                
                # 3. Parse ORDER BY
                if "ORDER BY" in q.upper():
                    order_info = _get_order_by_info(q)
                    sort = QueryTree(type="SORT", val=order_info)
                    
                    if last_node:
                        last_node.add_child(sort)
                    else:
                        current_root = sort
                    last_node = sort
                
                # 4. Parse GROUP BY
                if "GROUP BY" in q.upper():
                    group_col = _get_column_from_group_by(q)
                    group = QueryTree(type="GROUP", val=group_col)
                    
                    if last_node:
                        last_node.add_child(group)
                    else:
                        current_root = group
                    last_node = group
                
                # 5. Parse WHERE (SIGMA)
                if "WHERE" in q.upper():
                    where_cond = _get_condition_from_where(q)
                    
                    has_and = " AND " in where_cond.upper()
                    has_or = " OR " in where_cond.upper()
                    
                    # Case 1: Mixed (still not supported)
                    if has_and and has_or:
                        raise Exception("Mixed AND/OR clauses are not supported")
                    
                    # Case 2: OR-only
                    elif has_or:
                        or_node = QueryTree(type="OR")
                        if last_node:
                            last_node.add_child(or_node)
                        else:
                            current_root = or_node
                        
                        or_conditions = where_cond.split(" OR ")

                        for cond in or_conditions:
                            sigma_node = QueryTree(type="SIGMA", val=cond.strip())
                            or_node.add_child(sigma_node)
                        
                        last_node = or_node

                    # Case 3: AND-only (atau kondisi tunggal)
                    elif has_and or (not has_and and not has_or and where_cond):
                        where_split = where_cond.split(" AND ")
                        
                        first_sigma = QueryTree(type="SIGMA", val=where_split[0].strip())
                        
                        if last_node:
                            last_node.add_child(first_sigma)
                        else:
                            current_root = first_sigma
                        
                        temp_sigma = first_sigma
                        for cond in where_split[1:]:
                            next_sigma = QueryTree(type="SIGMA", val=cond.strip())
                            temp_sigma.add_child(next_sigma)
                            temp_sigma = next_sigma
                        
                        last_node = temp_sigma
                        
                # 6. Parse FROM (TABLE/JOIN with AS support)
                if last_node:
                    if last_node.type == "OR":
                        for child in last_node.childs:
                            if child.type == "SIGMA":
                                from_node_new = _parse_from_clause(q)
                                child.add_child(from_node_new)
                    
                    else:
                        from_node = _parse_from_clause(q)
                        last_node.add_child(from_node)
                
                else:
                    from_node = _parse_from_clause(q)
                    current_root = from_node
                
                parse_result.query_tree = current_root
            
            # Parse UPDATE statement
            elif q.upper().startswith("UPDATE"):
                # Init nodes
                current_root = None
                last_node = None
                
                # 1. Parse SET conditions 
                set_conditions = _extract_set_conditions(q)
                
                for set_cond in set_conditions:
                    update_node = QueryTree(type="UPDATE", val=set_cond)
                    
                    if last_node:
                        last_node.add_child(update_node)
                    else:
                        current_root = update_node
                    last_node = update_node
                
                # 2. Parse WHERE conditions (optional)
                if "WHERE" in q.upper():
                    where_cond = _get_condition_from_where(q)
                    where_split = where_cond.split(" AND ")
                    
                    # Create first SIGMA node
                    first_sigma = QueryTree(type="SIGMA", val=where_split[0].strip())
                    last_node.add_child(first_sigma)
                    
                    # Chain additional SIGMA nodes
                    temp_sigma = first_sigma
                    for cond in where_split[1:]:
                        next_sigma = QueryTree(type="SIGMA", val=cond.strip())
                        temp_sigma.add_child(next_sigma)
                        temp_sigma = next_sigma
                    
                    last_node = temp_sigma
                
                # 3. Parse table name
                table_name = _extract_table_update(q)
                table_node = QueryTree(type="TABLE", val=table_name)
                last_node.add_child(table_node)
                
                parse_result.query_tree = current_root
            
            # Parse DELETE statement
            elif q.upper().startswith("DELETE"):
                # Init nodes
                current_root = None
                last_node = None
                
                # 1. Create DELETE node
                delete_node = QueryTree(type="DELETE", val="")
                current_root = delete_node
                last_node = delete_node
                
                # 2. Parse WHERE conditions (optional)
                if "WHERE" in q.upper():
                    where_cond = _get_condition_from_where(q)
                    where_split = where_cond.split(" AND ")
                    
                    # Create first SIGMA node
                    first_sigma = QueryTree(type="SIGMA", val=where_split[0].strip())
                    last_node.add_child(first_sigma)
                    
                    # Chain additional SIGMA nodes
                    temp_sigma = first_sigma
                    for cond in where_split[1:]:
                        next_sigma = QueryTree(type="SIGMA", val=cond.strip())
                        temp_sigma.add_child(next_sigma)
                        temp_sigma = next_sigma
                    
                    last_node = temp_sigma
                
                # 3. Parse table name
                table_name = _extract_table_delete(q)
                table_node = QueryTree(type="TABLE", val=table_name)
                last_node.add_child(table_node)
                
                parse_result.query_tree = current_root
            
            # Parse INSERT statement
            elif q.upper().startswith("INSERT"):
                # Parse components
                table_name = _extract_table_insert(q)
                columns = _extract_columns_insert(q)
                values = _extract_values_insert(q)
                
                # Create INSERT node with format: "table_name|columns|values"
                insert_val = f"{table_name}|{columns}|{values}"
                insert_node = QueryTree(type="INSERT", val=insert_val)
                
                parse_result.query_tree = insert_node
            
             # Parse DROP TABLE statement
            elif q.upper().startswith("CREATE"):
                create_val = _parse_create_table(q)
                create_node = QueryTree(type="CREATE_TABLE", val=create_val)
                parse_result.query_tree = create_node

             # Parse DROP TABLE statement
            elif q.upper().startswith("DROP"):
                drop_val = _parse_drop_table(q)
                drop_node = QueryTree(type="DROP_TABLE", val=drop_val)
                parse_result.query_tree = drop_node

            # Parse BEGIN TRANSACTION statement
            elif q.upper().startswith("BEGIN"):
                # Masih simple node without children
                begin_node = QueryTree(type="BEGIN_TRANSACTION", val="")
                parse_result.query_tree = begin_node
            
            # Parse COMMIT statement
            elif q.upper().startswith("COMMIT"):
                # Masih simple node without children
                commit_node = QueryTree(type="COMMIT", val="")
                parse_result.query_tree = commit_node
            
            # Parse ROLLBACK statement
            elif q.upper().startswith("ROLLBACK"):
                # Masih simple node without children
                rollback_node = QueryTree(type="ROLLBACK", val="")
                parse_result.query_tree = rollback_node
                
            else:
                raise Exception(f"Unsupported query type: {q[:20]}")
        except Exception as e:
            raise Exception(f"Error parsing query: {str(e)}")
        
        return parse_result

    def optimize_query(self, parsed_query: ParsedQuery) -> ParsedQuery:
        root = parsed_query.query_tree

        # aturan logis
        root = fold_selection_with_cartesian(root)
        root = merge_selection_into_join(root)
        root = make_join_commutative(root)
        root = associate_natural_join(root)
        root = associate_theta_join(root)

        # ekstrak tabel dari query tree
        tables = list(_tables_under(root))
        
        # jika hanya 1 tabel atau tidak ada join, return as is
        if len(tables) <= 1:
            return ParsedQuery(parsed_query.query, root)
        
        # generate beberapa kandidat urutan join
        orders = _some_permutations(tables, max_count=5)
        
        # map kondisi join (untuk saat ini kosong, bisa diperluas)
        join_conditions = {}
        
        # build join trees untuk setiap urutan
        plans = []
        for order in orders:
            plan = build_join_tree(order, join_conditions)
            if plan:
                plans.append(plan)
        
        # jika tidak ada plan yang dihasilkan, return root asli
        if not plans:
            return ParsedQuery(parsed_query.query, root)
        
        # pilih plan terbaik berdasarkan cost
        stats = get_stats()
        best = choose_best(plans, stats)

        return ParsedQuery(parsed_query.query, best)

    def get_cost(self, parsed_query: ParsedQuery) -> int:
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