# ===============================================================
# helper.py (FINAL — full, stable version)
# ===============================================================

import re
from model.query_tree import QueryTree


# ===============================================================
#  CONDITION TREE STRUCTURES
# ===============================================================

class ConditionNode:
    """
    Simple atomic condition: attr op value
    Example:   age > 18
    """
    def __init__(self, attr, op, value):
        self.attr = attr.strip()
        self.op = op.strip()
        self.value = value.strip()

    def __repr__(self):
        return f"ConditionNode({self.attr} {self.op} {self.value})"


class LogicalNode:
    """
    Logical AND/OR: operator + list of children
    children = ConditionNode | LogicalNode
    """
    def __init__(self, operator, childs):
        self.operator = operator.upper().strip()
        self.childs = list(childs)

    def __repr__(self):
        return f"LogicalNode({self.operator}, {self.childs})"


# ===============================================================
#  CONDITION PARSER UTILITIES
# ===============================================================

_comparison_ops = ['>=', '<=', '<>', '!=', '=', '>', '<']

def _strip_outer_paren(s):
    """Remove outer parentheses only if they wrap whole expression."""
    s = s.strip()
    while s.startswith("(") and s.endswith(")"):
        depth = 0
        ok = True
        for i, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if depth == 0 and i < len(s) - 1:
                ok = False
                break
        if not ok:
            break
        s = s[1:-1].strip()
    return s


def _split_top_level(s, sep):
    """Split string by sep (AND/OR) but NOT inside parentheses."""
    parts = []
    cur = ""
    depth = 0
    i = 0
    L = len(s)
    sep_upper = sep.upper()

    while i < L:
        ch = s[i]
        if ch == "(":
            depth += 1
            cur += ch
            i += 1
            continue
        if ch == ")":
            depth -= 1
            cur += ch
            i += 1
            continue
        if depth == 0:
            segment = s[i:i+len(sep_upper)].upper()
            if segment == sep_upper:
                prev_ok = i == 0 or not s[i-1].isalnum()
                next_ok = i+len(sep_upper) >= L or not s[i+len(sep_upper)].isalnum()
                if prev_ok and next_ok:
                    parts.append(cur.strip())
                    cur = ""
                    i += len(sep_upper)
                    continue
        cur += ch
        i += 1
    if cur.strip():
        parts.append(cur.strip())
    return parts


def _parse_simple_condition(s):
    """Parse atomic condition → ConditionNode."""
    s = s.strip()

    # IN
    m = re.search(r"\bIN\b", s, flags=re.IGNORECASE)
    if m:
        left = s[:m.start()].strip()
        right = s[m.end():].strip()
        return ConditionNode(left, "IN", right)

    # LIKE
    m = re.search(r"\bLIKE\b", s, flags=re.IGNORECASE)
    if m:
        left = s[:m.start()].strip()
        right = s[m.end():].strip()
        return ConditionNode(left, "LIKE", right)

    for op in sorted(_comparison_ops, key=lambda x: -len(x)):
        if op in s:
            left, right = s.split(op, 1)
            return ConditionNode(left.strip(), op, right.strip())

    return ConditionNode(s, "UNKNOWN", "")


def build_logical_tree_from_where(expr):
    """Build nested AND/OR/condition tree."""
    if not expr:
        return None

    s = _strip_outer_paren(expr.strip())

    # OR
    parts = _split_top_level(s, "OR")
    if len(parts) > 1:
        return LogicalNode("OR", [build_logical_tree_from_where(p) for p in parts])

    # AND
    parts = _split_top_level(s, "AND")
    if len(parts) > 1:
        return LogicalNode("AND", [build_logical_tree_from_where(p) for p in parts])

    # atomic
    ss = _strip_outer_paren(s)
    if ss != s:
        return build_logical_tree_from_where(ss)

    return _parse_simple_condition(ss)


# ===============================================================
#  CONDITION TREE → STRING (for printing)
# ===============================================================

def _stringify_condition_tree(node):
    if isinstance(node, ConditionNode):
        return f"{node.attr} {node.op} {node.value}"

    if isinstance(node, LogicalNode):
        parts = [f"({_stringify_condition_tree(c)})" for c in node.childs]
        return (" " + node.operator + " ").join(parts)[1:-1]

    return str(node)


# ===============================================================
#  TABLE EXTRACTION
# ===============================================================

def _tables_under(node):
    results = []
    def dfs(n):
        if n.type == "TABLE":
            results.append(n.val)
        for c in n.childs:
            dfs(c)
    dfs(node)
    return set(results)


# ===============================================================
#  JOIN HELPERS
# ===============================================================

def _is_cartesian(j):
    return j.type == "JOIN" and (j.val == "" or j.val.upper() == "CARTESIAN")

def _is_theta(j):
    return j.type == "JOIN" and isinstance(j.val, str) and j.val.upper().startswith("THETA:")

def _is_natural(j):
    return j.type == "JOIN" and j.val.upper() == "NATURAL"

def _theta_pred(j):
    return j.val.split(":", 1)[1].strip() if _is_theta(j) else ""

def _mk_theta(pred):
    return f"THETA:{pred.strip()}"


# ===============================================================
# FROM + JOIN PARSER (FINAL)
# ===============================================================

def _parse_single_table(raw):
    parts = raw.split()
    if len(parts) == 1:
        return QueryTree("TABLE", parts[0])
    if len(parts) == 2:
        return QueryTree("TABLE", f"{parts[0]} AS {parts[1]}")
    if len(parts) == 3 and parts[1].upper() == "AS":
        return QueryTree("TABLE", f"{parts[0]} AS {parts[2]}")
    raise Exception(f"Invalid table syntax: {raw}")


def _parse_from_clause(q):
    pattern = r"\bFROM\b(.+?)(WHERE|GROUP BY|ORDER BY|LIMIT|$)"
    block = re.search(pattern, q, flags=re.IGNORECASE | re.DOTALL)
    if not block:
        raise Exception("FROM clause not found")

    from_block = block.group(1).strip().replace("\n", " ")
    from_block = re.sub(r"\s+", " ", from_block)

    tokens = re.split(r"\b(JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN)\b",
                      from_block, flags=re.IGNORECASE)

    base_raw = tokens[0].strip()
    root = _parse_single_table(base_raw)

    i = 1
    while i < len(tokens) - 1:
        join_type = tokens[i].strip().upper()
        right_part = tokens[i+1].strip()

        m = re.match(r"(.+?)\bON\b(.+)", right_part, flags=re.IGNORECASE)
        if not m:
            raise Exception(f"Invalid JOIN syntax: {right_part}")

        table_raw = m.group(1).strip()
        cond_raw = m.group(2).strip()

        right_table = _parse_single_table(table_raw)
        join = QueryTree("JOIN", f"THETA:{cond_raw}")
        join.add_child(root)
        join.add_child(right_table)
        root = join

        i += 2

    return root


# ===============================================================
# REWRITE RULES (FINAL)
# ===============================================================

def fold_selection_with_cartesian(node):
    if node.type == "SIGMA" and node.childs and _is_cartesian(node.childs[0]):
        join = node.childs[0]
        pred = node.val
        join.val = _mk_theta(_stringify_condition_tree(pred))
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        else:
            join.parent = None
        return join
    return node


def merge_selection_into_join(node):
    if node.type == "SIGMA" and node.childs and _is_theta(node.childs[0]):
        join = node.childs[0]
        old = _theta_pred(join)
        new = _stringify_condition_tree(node.val)
        merged = f"{new} AND {old}" if old else new
        join.val = _mk_theta(merged)
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        else:
            join.parent = None
        return join
    return node


# -----------------------------------------------
# Canonical join commutativity (safe)
# -----------------------------------------------
def canonical_join_order(join):
    if join.type != "JOIN" or len(join.childs) != 2:
        return join
    L, R = join.childs

    from helper.helper import _tables_under
    def key(x):
        t = _tables_under(x)
        if t:
            return sorted([v.lower() for v in t])[0]
        return x.type.lower()

    if key(L) > key(R):
        join.childs[0], join.childs[1] = R, L
        R.parent = join
        L.parent = join

    return join


# -----------------------------------------------
# Natural join associativity
# -----------------------------------------------
def associate_natural_join(node):
    if node.type == "JOIN" and _is_natural(node):
        L, R = node.childs
        if L.type == "JOIN" and _is_natural(L):
            A, B = L.childs
            C = R
            inner = QueryTree("JOIN", "NATURAL", [B, C])
            B.parent = inner; C.parent = inner
            res = QueryTree("JOIN", "NATURAL", [A, inner])
            A.parent = res; inner.parent = res
            return res
        if R.type == "JOIN" and _is_natural(R):
            B, C = R.childs
            A = L
            inner = QueryTree("JOIN", "NATURAL", [A, B])
            A.parent = inner; B.parent = inner
            res = QueryTree("JOIN", "NATURAL", [inner, C])
            inner.parent = res; C.parent = res
            return res
    return node


# -----------------------------------------------
# Theta join associativity
# -----------------------------------------------
def associate_theta_join(node):
    if node.type != "JOIN" or not _is_theta(node):
        return node
    L, R = node.childs
    if L.type == "JOIN" and _is_theta(L):
        A, B = L.childs
        C = R
        inner = QueryTree("JOIN", L.val, [B, C])
        B.parent = inner; C.parent = inner
        res = QueryTree("JOIN", node.val, [A, inner])
        A.parent = res; inner.parent = res
        return res
    if R.type == "JOIN" and _is_theta(R):
        B, C = R.childs
        A = L
        inner = QueryTree("JOIN", R.val, [B, C])
        B.parent = inner; C.parent = inner
        res = QueryTree("JOIN", node.val, [A, inner])
        A.parent = res; inner.parent = res
        return res
    return node


# ===============================================================
#  SELECTION DECOMPOSITION (σ(A AND B AND C))
# ===============================================================

def decompose_conjunctive_selection(node):
    if node.type != "SIGMA" or not isinstance(node.val, LogicalNode):
        return node

    cond = node.val
    if cond.operator != "AND":
        return node

    child = node.childs[0]
    curr = child
    for c in reversed(cond.childs):
        sigma = QueryTree("SIGMA", c)
        if curr:
            sigma.add_child(curr)
        curr = sigma

    if node.parent:
        node.parent.replace_child(node, curr)
        curr.parent = node.parent
    else:
        curr.parent = None

    return curr


# ===============================================================
#  SELECTION PUSH-DOWN (Rule 4)
# ===============================================================

def _extract_attributes_from_condition(c):
    if isinstance(c, ConditionNode):
        return [c.attr]
    if isinstance(c, LogicalNode):
        out = []
        for ch in c.childs:
            out.extend(_extract_attributes_from_condition(ch))
        return out
    if isinstance(c, str):
        return re.findall(r"\b\w+\.\w+\b", c)
    return []


def push_selection_through_join_single(node):
    if node.type != "SIGMA" or not node.childs:
        return node
    join = node.childs[0]
    if join.type != "JOIN":
        return node

    L, R = join.childs
    Lset = _tables_under(L)
    Rset = _tables_under(R)

    attrs = _extract_attributes_from_condition(node.val)
    belongL = any(a.split('.')[0] in Lset for a in attrs if '.' in a)
    belongR = any(a.split('.')[0] in Rset for a in attrs if '.' in a)

    if belongL and not belongR:
        sigmaL = QueryTree("SIGMA", node.val)
        sigmaL.add_child(L)
        join.childs[0] = sigmaL
        sigmaL.parent = join
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        return join

    if belongR and not belongL:
        sigmaR = QueryTree("SIGMA", node.val)
        sigmaR.add_child(R)
        join.childs[1] = sigmaR
        sigmaR.parent = join
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        return join

    return node


def push_selection_through_join_split(node):
    if node.type != "SIGMA" or not node.childs:
        return node
    join = node.childs[0]
    if join.type != "JOIN":
        return node
    cond = node.val
    if not isinstance(cond, LogicalNode) or cond.operator != "AND":
        return node

    L, R = join.childs
    Lset = _tables_under(L)
    Rset = _tables_under(R)

    lefts, rights, mixed = [], [], []

    for c in cond.childs:
        attrs = _extract_attributes_from_condition(c)
        bl = any(a.split('.')[0] in Lset for a in attrs if '.' in a)
        br = any(a.split('.')[0] in Rset for a in attrs if '.' in a)
        if bl and not br: lefts.append(c)
        elif br and not bl: rights.append(c)
        else: mixed.append(c)

    changed = False
    if lefts:
        newval = lefts[0] if len(lefts) == 1 else LogicalNode("AND", lefts)
        sL = QueryTree("SIGMA", newval); sL.add_child(L)
        join.childs[0] = sL; sL.parent = join; changed = True

    if rights:
        newval = rights[0] if len(rights) == 1 else LogicalNode("AND", rights)
        sR = QueryTree("SIGMA", newval); sR.add_child(R)
        join.childs[1] = sR; sR.parent = join; changed = True

    if changed:
        if mixed:
            node.val = mixed[0] if len(mixed)==1 else LogicalNode("AND", mixed)
            return node
        else:
            if node.parent:
                node.parent.replace_child(node, join)
                join.parent = node.parent
            return join

    return node


# ===============================================================
#  PROJECTION PUSH-DOWN (Rule 5)
# ===============================================================

def push_projection_through_join_simple(node):
    if node.type != "PROJECT" or not node.childs:
        return node
    join = node.childs[0]
    if join.type != "JOIN":
        return node

    L, R = join.childs
    Lset = _tables_under(L)
    Rset = _tables_under(R)

    cols = [c.strip() for c in node.val.split(",")]
    Lcols = [c for c in cols if c.split('.')[0] in Lset]
    Rcols = [c for c in cols if c.split('.')[0] in Rset]

    if Lcols and Rcols:
        left_proj = QueryTree("PROJECT", ", ".join(Lcols))
        left_proj.add_child(L)
        join.childs[0] = left_proj; left_proj.parent = join

        right_proj = QueryTree("PROJECT", ", ".join(Rcols))
        right_proj.add_child(R)
        join.childs[1] = right_proj; right_proj.parent = join

        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        else:
            join.parent = None

        return join

    return node


def push_projection_through_join_with_join_attrs(node):
    if node.type != "PROJECT" or not node.childs:
        return node
    join = node.childs[0]
    if join.type != "JOIN" or not _is_theta(join):
        return node

    L, R = join.childs
    Lset = _tables_under(L)
    Rset = _tables_under(R)

    cols = [c.strip() for c in node.val.split(",")]
    theta = _theta_pred(join)
    join_attrs = re.findall(r"\b\w+\.\w+\b", theta)

    Lcols = [c for c in cols if c.split('.')[0] in Lset]
    Rcols = [c for c in cols if c.split('.')[0] in Rset]

    Lj = [a for a in join_attrs if a.split('.')[0] in Lset and a not in Lcols]
    Rj = [a for a in join_attrs if a.split('.')[0] in Rset and a not in Rcols]

    if Lcols or Lj:
        lp = QueryTree("PROJECT", ", ".join(Lcols + Lj))
        lp.add_child(L)
        join.childs[0] = lp; lp.parent = join

    if Rcols or Rj:
        rp = QueryTree("PROJECT", ", ".join(Rcols + Rj))
        rp.add_child(R)
        join.childs[1] = rp; rp.parent = join

    return node


# ===============================================================
#  REDUNDANT PROJECTION REMOVAL
# ===============================================================

def eliminate_redundant_projections(node):
    if node.type != "PROJECT" or not node.childs:
        return node
    c = node.childs[0]
    if c.type == "PROJECT":
        # collapse nested projects
        while c.childs and c.childs[0].type == "PROJECT":
            c = c.childs[0]
        if c.childs:
            node.childs = [c.childs[0]]
            c.childs[0].parent = node
        return node
    return node


# ===============================================================
#  SELECTION SWAP ORDER
# ===============================================================

def swap_selection_order(node):
    if node.type != "SIGMA":
        return node
    if not node.childs or node.childs[0].type != "SIGMA":
        return node

    s1_val = node.val
    s2 = node.childs[0]
    s2_val = s2.val
    deeper = s2.childs[0] if s2.childs else None

    new_s1 = QueryTree("SIGMA", s1_val)
    if deeper:
        new_s1.add_child(deeper)

    new_s2 = QueryTree("SIGMA", s2_val)
    new_s2.add_child(new_s1)

    if node.parent:
        node.parent.replace_child(node, new_s2)
        new_s2.parent = node.parent
    else:
        new_s2.parent = None

    return new_s2


# ===============================================================
#  COST HELPERS
# ===============================================================

def plan_cost(node, stats):
    """
    Very rough cost for join order testing (not final cost model).
    Just sum recursively.
    """
    if node.type == "TABLE":
        return stats.get(node.val, {}).get("b_r", 100)

    if node.type == "SIGMA":
        return plan_cost(node.childs[0], stats) if node.childs else 0

    if node.type == "JOIN":
        L, R = node.childs
        return plan_cost(L, stats) + plan_cost(R, stats) + 10

    cost = 0
    for c in node.childs:
        cost += plan_cost(c, stats)
    return cost


def choose_best(plans, stats):
    best, best_c = None, None
    for p in plans:
        c = plan_cost(p, stats)
        if best is None or c < best_c:
            best, best_c = p, c
    return best


# ===============================================================
# ORDER BY, GROUP BY, LIMIT, UPDATE, DELETE, INSERT PARSERS
# ===============================================================

def validate_query(q):
    if not q.strip().endswith(";"):
        return False, "Missing semicolon"
    if not q.strip():
        return False, "Empty query"
    QC = q.strip().upper()
    if not any(QC.startswith(x) for x in ["SELECT", "UPDATE", "DELETE", "INSERT", "CREATE", "DROP"]):
        return False, "Unsupported query type"
    return True, "OK"


def _get_columns_from_select(q):
    m = re.search(r"SELECT\s+(.+?)\s+FROM", q, flags=re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else "*"


def _get_condition_from_where(q):
    m = re.search(r"\bWHERE\b(.+)", q, flags=re.IGNORECASE | re.DOTALL)
    if not m: return ""
    s = m.group(1)
    s = re.split(r"\bORDER BY\b|\bGROUP BY\b|\bLIMIT\b", s, flags=re.IGNORECASE)[0]
    return s.strip()


def _get_limit(q):
    m = re.search(r"\bLIMIT\s+(\d+)", q, flags=re.IGNORECASE)
    return int(m.group(1)) if m else None


def _get_column_from_order_by(q):
    m = re.search(r"ORDER BY\s+(.+)", q, flags=re.IGNORECASE)
    return m.group(1).strip() if m else None


def _get_column_from_group_by(q):
    m = re.search(r"GROUP BY\s+(.+)", q, flags=re.IGNORECASE)
    return m.group(1).strip() if m else None


def _extract_set_conditions(q):
    m = re.search(r"SET\s+(.+?)\s+WHERE", q, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        m = re.search(r"SET\s+(.+)", q, flags=re.IGNORECASE | re.DOTALL)
    if not m: return []
    return [x.strip() for x in m.group(1).split(",")]


def _extract_table_update(q):
    m = re.search(r"UPDATE\s+(\w+)", q, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _extract_table_delete(q):
    m = re.search(r"DELETE\s+FROM\s+(\w+)", q, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _extract_table_insert(q):
    m = re.search(r"INSERT\s+INTO\s+(\w+)", q, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _extract_columns_insert(q):
    m = re.search(r"\((.*?)\)", q)
    return [x.strip() for x in m.group(1).split(",")] if m else []


def _extract_values_insert(q):
    m = re.search(r"VALUES\s*\((.*?)\)", q, flags=re.IGNORECASE)
    return [x.strip() for x in m.group(1).split(",")] if m else []


def _parse_drop_table(q):
    m = re.search(r"DROP\s+TABLE\s+(\w+)", q, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _parse_create_table(q):
    m = re.search(r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)", q,
                  flags=re.IGNORECASE | re.DOTALL)
    if not m: return ""
    table = m.group(1)
    defs = m.group(2)
    return f"{table}|{defs}"


def _get_order_by_info(q):
    m = re.search(r"ORDER BY\s+(.+)$", q, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""
