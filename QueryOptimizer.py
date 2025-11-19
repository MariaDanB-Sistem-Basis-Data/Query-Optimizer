# ===============================================================
# QueryOptimizer.py (FINAL STABLE VERSION)
# ===============================================================

from model.parsed_query import ParsedQuery
from model.query_tree import QueryTree

# --- import helpers ---
from helper.helper import (
    # basic parsers
    validate_query,
    _get_columns_from_select,
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
    _parse_drop_table,
    _parse_create_table,

    # condition tree builder
    ConditionNode,
    LogicalNode,
    build_logical_tree_from_where,
    _stringify_condition_tree,

    # rewrite rules
    fold_selection_with_cartesian,
    merge_selection_into_join,
    associate_natural_join,
    associate_theta_join,
    decompose_conjunctive_selection,
    swap_selection_order,
    eliminate_redundant_projections,
    push_selection_through_join_single,
    push_selection_through_join_split,
    push_projection_through_join_simple,
    push_projection_through_join_with_join_attrs,
    canonical_join_order,

    # cost utilities
    plan_cost,
    choose_best,
    _tables_under,
)

from helper.stats import get_stats


# ===============================================================
# TREE SIGNATURE (cycle detection)
# ===============================================================

def _tree_signature(node: QueryTree) -> str:
    """Deterministic lightweight signature of the whole tree."""
    parts = []

    def cv(val):
        if isinstance(val, str):
            return val
        try:
            return _stringify_condition_tree(val)
        except:
            return str(val)

    def dfs(n):
        parts.append(f"[{n.type}:{cv(n.val)}]")
        for c in n.childs:
            dfs(c)

    dfs(node)
    return "|".join(parts)


# ===============================================================
# MAIN OPTIMIZER CLASS
# ===============================================================

class OptimizationEngine:

    # -----------------------------------------------------------
    # PARSER
    # -----------------------------------------------------------
    def parse_query(self, query: str) -> ParsedQuery:

        if not query:
            raise Exception("Empty query")

        ok, msg = validate_query(query)
        if not ok:
            raise Exception(f"Query validation failed: {msg}")

        q = query.strip().rstrip(";").strip()
        parsed = ParsedQuery(query)

        try:
            root = None
            last = None

            # ------------------ SELECT ---------------------
            if q.upper().startswith("SELECT"):

                # PROJECT
                cols = _get_columns_from_select(q)
                if cols != "*":
                    proj = QueryTree("PROJECT", cols)
                    root = proj
                    last = proj

                # LIMIT
                if "LIMIT" in q.upper():
                    lv = _get_limit(q)
                    lm = QueryTree("LIMIT", str(lv))
                    if last: last.add_child(lm)
                    else: root = lm
                    last = lm

                # ORDER BY
                if "ORDER BY" in q.upper():
                    col = _get_column_from_order_by(q)
                    ob = QueryTree("SORT", col)
                    if last: last.add_child(ob)
                    else: root = ob
                    last = ob

                # GROUP BY
                if "GROUP BY" in q.upper():
                    col = _get_column_from_group_by(q)
                    gb = QueryTree("GROUP", col)
                    if last: last.add_child(gb)
                    else: root = gb
                    last = gb

                # WHERE → Logical Nodes
                if "WHERE" in q.upper():
                    wcond = _get_condition_from_where(q)
                    cond_tree = build_logical_tree_from_where(wcond)

                    # top-level OR → OR node with multiple SIGMA
                    if isinstance(cond_tree, LogicalNode) and cond_tree.operator == "OR":
                        ornode = QueryTree("OR")
                        if last: last.add_child(ornode)
                        else: root = ornode
                        for branch in cond_tree.childs:
                            s = QueryTree("SIGMA", branch)
                            ornode.add_child(s)
                        last = ornode
                    else:
                        s = QueryTree("SIGMA", cond_tree)
                        if last: last.add_child(s)
                        else: root = s
                        last = s

                # FROM (tables / joins)
                fromnode = _parse_from_clause(q)
                if last and last.type == "OR":
                    for c in last.childs:
                        c.add_child(fromnode)
                elif last:
                    last.add_child(fromnode)
                else:
                    root = fromnode

                parsed.query_tree = root
                return parsed

            # ------------------ UPDATE ---------------------
            if q.upper().startswith("UPDATE"):
                root = None
                last = None

                sets = _extract_set_conditions(q)
                for s in sets:
                    u = QueryTree("UPDATE", s)
                    if last: last.add_child(u)
                    else: root = u
                    last = u

                if "WHERE" in q.upper():
                    wcond = _get_condition_from_where(q)
                    cond = build_logical_tree_from_where(wcond)
                    s = QueryTree("SIGMA", cond)
                    last.add_child(s)
                    last = s

                table = _extract_table_update(q)
                tnode = QueryTree("TABLE", table)
                last.add_child(tnode)

                parsed.query_tree = root
                return parsed

            # ------------------ DELETE ---------------------
            if q.upper().startswith("DELETE"):
                d = QueryTree("DELETE", "")
                root = d
                last = d

                if "WHERE" in q.upper():
                    wcond = _get_condition_from_where(q)
                    cond = build_logical_tree_from_where(wcond)
                    s = QueryTree("SIGMA", cond)
                    last.add_child(s)
                    last = s

                table = _extract_table_delete(q)
                t = QueryTree("TABLE", table)
                last.add_child(t)

                parsed.query_tree = root
                return parsed

            # ------------------ INSERT ---------------------
            if q.upper().startswith("INSERT"):
                t = _extract_table_insert(q)
                cols = _extract_columns_insert(q)
                vals = _extract_values_insert(q)
                val = f"{t}|{cols}|{vals}"
                parsed.query_tree = QueryTree("INSERT", val)
                return parsed

            # ------------------ CREATE TABLE ----------------
            if q.upper().startswith("CREATE"):
                v = _parse_create_table(q)
                parsed.query_tree = QueryTree("CREATE_TABLE", v)
                return parsed

            # ------------------ DROP TABLE ------------------
            if q.upper().startswith("DROP"):
                t = _parse_drop_table(q)
                parsed.query_tree = QueryTree("DROP_TABLE", t)
                return parsed

        except Exception as e:
            raise Exception(f"Error during parsing: {str(e)}")

        return parsed


    # ===========================================================
    # OPTIMIZATION ENGINE
    # ===========================================================
    def optimize_query(self, parsed: ParsedQuery) -> ParsedQuery:

        if not parsed or not parsed.query_tree:
            return parsed

        root = parsed.query_tree

        seen = set()
        max_passes = 80

        for _ in range(max_passes):

            sig = _tree_signature(root)
            if sig in seen:
                break
            seen.add(sig)

            new_root = self._apply_rules(root)

            # If no change
            if new_root is root:
                new2 = self._apply_rules(root)
                if new2 is root:
                    break
                root = new2
            else:
                root = new_root

        parsed.query_tree = root
        return parsed


    # -----------------------------------------------------------
    # APPLY RULES (bottom-up)
    # -----------------------------------------------------------
    def _apply_rules(self, node: QueryTree) -> QueryTree:

        # bottom-up apply first
        for i, child in enumerate(node.childs):
            r = self._apply_rules(child)
            if r is not child:
                node.childs[i] = r
                r.parent = node

        # --- RULES START ---
        r = decompose_conjunctive_selection(node)
        if r is not node: return r

        r = fold_selection_with_cartesian(node)
        if r is not node: return r

        r = merge_selection_into_join(node)
        if r is not node: return r

        r = push_selection_through_join_single(node)
        if r is not node: return r

        r = push_selection_through_join_split(node)
        if r is not node: return r

        r = push_projection_through_join_simple(node)
        if r is not node: return r

        r = push_projection_through_join_with_join_attrs(node)
        if r is not node: return r

        r = eliminate_redundant_projections(node)
        if r is not node: return r

        r = associate_natural_join(node)
        if r is not node: return r

        r = associate_theta_join(node)
        if r is not node: return r

        # canonical join order (prevents flip-flop)
        if node.type == "JOIN":
            r = canonical_join_order(node)
            if r is not node: return r

        # safe sigma reorder
        r = swap_selection_order(node)
        if r is not node: return r

        return node


    # ===========================================================
    # COST API
    # ===========================================================
    def get_cost(self, parsed: ParsedQuery) -> int:
        if not parsed or not parsed.query_tree:
            return 0
        stats = get_stats()
        return plan_cost(parsed.query_tree, stats)
