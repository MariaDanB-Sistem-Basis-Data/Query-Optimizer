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
    _parse_create_table,
    parse_where_condition,
    parse_columns_from_string,
    parse_order_by_string,
    parse_group_by_string,
    parse_table_with_alias,
    parse_set_clauses_string,
    parse_insert_columns_string,
    parse_insert_values_string
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
                current_root = None
                last_node = None
                
                # 1. Parse PROJECT (SELECT columns)
                columns_str = _get_columns_from_select(q)
                if columns_str != "*":
                    columns_list = parse_columns_from_string(columns_str)
                    
                    proj = QueryTree(type="PROJECT", val=columns_str)
                    proj.columns = columns_list  # Structured data
                    
                    current_root = proj
                    last_node = proj
                
                # 2. Parse LIMIT
                if "LIMIT" in q.upper():
                    limit_val = _get_limit(q)
                    
                    lim = QueryTree(type="LIMIT", val=str(limit_val))
                    lim.limit_value = int(limit_val)  # Structured data
                    
                    if last_node:
                        last_node.add_child(lim)
                    else:
                        current_root = lim
                    last_node = lim
                
                # 3. Parse ORDER BY
                if "ORDER BY" in q.upper():
                    order_info_str = _get_order_by_info(q)
                    order_by_list = parse_order_by_string(order_info_str)
                    
                    sort = QueryTree(type="SORT", val=order_info_str)
                    sort.order_by = order_by_list  # Structured data
                    
                    if last_node:
                        last_node.add_child(sort)
                    else:
                        current_root = sort
                    last_node = sort
                
                # 4. Parse GROUP BY
                if "GROUP BY" in q.upper():
                    group_col_str = _get_column_from_group_by(q)
                    group_by_list = parse_group_by_string(group_col_str)
                    
                    group = QueryTree(type="GROUP", val=group_col_str)
                    group.group_by = group_by_list  # Structured data
                    
                    if last_node:
                        last_node.add_child(group)
                    else:
                        current_root = group
                    last_node = group
                
                # 5. Parse WHERE (SIGMA), udah support mixed and & or
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition_dict = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=where_cond_str)
                    sigma.condition = condition_dict  # Structured data
                    
                    if last_node:
                        last_node.add_child(sigma)
                    else:
                        current_root = sigma
                    last_node = sigma
                
                # 6. Parse FROM
                if last_node:
                    from_node = _parse_from_clause(q)
                    last_node.add_child(from_node)
                else:
                    from_node = _parse_from_clause(q)
                    current_root = from_node
                
                parse_result.query_tree = current_root
          
            # Parse UPDATE statement
            elif q.upper().startswith("UPDATE"):
                current_root = None
                last_node = None
                
                # 1. Parse SET 
                set_conditions_list = _extract_set_conditions(q)
                
                set_dict = {}
                for set_cond in set_conditions_list:
                    if '=' in set_cond:
                        eq_pos = set_cond.find('=')
                        column = set_cond[:eq_pos].strip()
                        value = set_cond[eq_pos + 1:].strip()
                        set_dict[column] = value
                
                set_conditions_str = ", ".join(set_conditions_list)
                
                update_node = QueryTree(type="UPDATE", val=set_conditions_str)
                update_node.set_clauses = set_dict  # Structured data
                
                current_root = update_node
                last_node = update_node
                
                # 2. Parse WHERE (optional)
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition_dict = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=where_cond_str)
                    sigma.condition = condition_dict  # Structured data
                    
                    last_node.add_child(sigma)
                    last_node = sigma
                
                # 3. Parse table name (support alias)
                table_str = _extract_table_update(q)
                table_name, table_alias = parse_table_with_alias(table_str)
                
                table_node = QueryTree(type="TABLE", val=table_str)
                table_node.table_name = table_name      # Structured data
                table_node.table_alias = table_alias    # Structured data
                
                last_node.add_child(table_node)
                
                parse_result.query_tree = current_root

            # Parse DELETE statement
            elif q.upper().startswith("DELETE"):
                current_root = None
                last_node = None
                
                # 1. DELETE node
                delete_node = QueryTree(type="DELETE", val="")
                current_root = delete_node
                last_node = delete_node
                
                # 2. Parse WHERE
                if "WHERE" in q.upper():
                    where_cond_str = _get_condition_from_where(q)
                    condition_dict = parse_where_condition(where_cond_str)
                    
                    sigma = QueryTree(type="SIGMA", val=where_cond_str)
                    sigma.condition = condition_dict  # Structured data
                    
                    last_node.add_child(sigma)
                    last_node = sigma
                
                # 3. Parse table
                table_str = _extract_table_delete(q)
                table_name, table_alias = parse_table_with_alias(table_str)
                
                table_node = QueryTree(type="TABLE", val=table_str)
                table_node.table_name = table_name      # Structured data
                table_node.table_alias = table_alias    # Structured data
                
                last_node.add_child(table_node)
                parse_result.query_tree = current_root
            
            # Parse INSERT statement
            elif q.upper().startswith("INSERT"):
                table_name = _extract_table_insert(q)
                columns_str = _extract_columns_insert(q)
                values_str = _extract_values_insert(q)
                
                columns_list = parse_insert_columns_string(columns_str)
                values_list = parse_insert_values_string(values_str)
                
                insert_val = f"{table_name}|{columns_str}|{values_str}"
                insert_node = QueryTree(type="INSERT", val=insert_val)
                
                # Structured data
                insert_node.insert_table = table_name
                insert_node.insert_columns = columns_list
                insert_node.insert_values = values_list
                
                parse_result.query_tree = insert_node
            
            # Parse CREATE TABLE statement
            elif q.upper().startswith("CREATE"):
                create_val = _parse_create_table(q)
                create_node = QueryTree(type="CREATE_TABLE", val=create_val)
                
                # Extract table name if possible
                if '|' in create_val:
                    parts = create_val.split('|')
                    if len(parts) >= 1:
                        create_node.create_table_name = parts[0]
                
                parse_result.query_tree = create_node

            # Parse DROP TABLE statement
            elif q.upper().startswith("DROP"):
                drop_str = _parse_drop_table(q)
                is_cascade = "CASCADE" in q.upper()
                table_name = drop_str.replace("CASCADE", "").replace("cascade", "").strip()
                
                drop_node = QueryTree(type="DROP_TABLE", val=drop_str)
                drop_node.drop_table_name = table_name  # Structured data
                drop_node.drop_cascade = is_cascade     # Structured data
                
                parse_result.query_tree = drop_node

            # Parse BEGIN TRANSACTION statement
            elif q.upper().startswith("BEGIN"):
                begin_node = QueryTree(type="BEGIN_TRANSACTION", val="")
                parse_result.query_tree = begin_node
            
            # Parse COMMIT statement
            elif q.upper().startswith("COMMIT"):
                commit_node = QueryTree(type="COMMIT", val="")
                parse_result.query_tree = commit_node
            
            # Parse ROLLBACK statement
            elif q.upper().startswith("ROLLBACK"):
                rollback_node = QueryTree(type="ROLLBACK", val="")
                parse_result.query_tree = rollback_node
            
            # Unsupported query type
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