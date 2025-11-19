from model.query_tree import (
    QueryTree, 
    ConditionNode, 
    LogicalNode, 
    ColumnNode, 
    OrderByItem,
    SetClause,
    ColumnDefinition,
    ForeignKeyDefinition,
    TableReference,
    NaturalJoin,
    ThetaJoin
)
import re

# util kecil
def _is_cartesian(join_node: QueryTree) -> bool:
    if join_node.type != "JOIN":
        return False
    if join_node.val is None or join_node.val == "":
        return True
    if isinstance(join_node.val, str) and join_node.val.upper() == "CARTESIAN":
        return True
    return False

def _is_theta(join_node: QueryTree) -> bool:
    if join_node.type != "JOIN":
        return False
    if isinstance(join_node.val, ThetaJoin):
        return True
    if isinstance(join_node.val, str) and join_node.val.upper().startswith("THETA:"):
        return True
    return False

def _is_natural(join_node: QueryTree) -> bool:
    if join_node.type != "JOIN":
        return False
    if isinstance(join_node.val, NaturalJoin):
        return True
    if isinstance(join_node.val, str) and join_node.val.upper() == "NATURAL":
        return True
    return False

def _theta_pred(join_node: QueryTree) -> str:
    if not _is_theta(join_node):
        return ""
    if isinstance(join_node.val, ThetaJoin):
        # return string representation of condition
        return str(join_node.val.condition)
    return join_node.val.split(":", 1)[1].strip()

def _mk_theta(pred: str) -> str:
    return f"THETA:{pred.strip()}"

def _find_node(tree: QueryTree, node_type: str):
    """Find first node of given type in query tree"""
    if tree.type == node_type:
        return tree
    for child in tree.childs:
        result = _find_node(child, node_type)
        if result:
            return result
    return None

def _tables_under(node: QueryTree):
    """Extract all table names from a query tree"""
    out = []
    def dfs(n):
        if n.type == "TABLE":
            if isinstance(n.val, TableReference):
                out.append(n.val.name)
            else:
                out.append(n.val)
        for c in n.childs:
            dfs(c)
    dfs(node)
    return set(out)

# σθ(E1 × E2)  ⇒  E1 ⋈θ E2
def fold_selection_with_cartesian(node: QueryTree):
    if node.type == "SIGMA" and node.childs and _is_cartesian(node.childs[0]):
        join = node.childs[0]
        pred = node.val  # seluruh predicate disimpan di val
        join.val = _mk_theta(pred)
        # ganti sigma dengan join
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        else:
            join.parent = None
        return join
    return node

# σθ(E1 ⋈θ2 E2)  ⇒  E1 ⋈(θ ∧ θ2) E2
def merge_selection_into_join(node: QueryTree):
    if node.type == "SIGMA" and node.childs and _is_theta(node.childs[0]):
        join = node.childs[0]
        p_old = _theta_pred(join)
        p_new = node.val
        merged = p_new if not p_old else f"{p_new} AND {p_old}"
        join.val = _mk_theta(merged)
        # angkat join, hilangkan sigma
        if node.parent:
            node.parent.replace_child(node, join)
            join.parent = node.parent
        else:
            join.parent = None
        return join
    return node

# Komutatif: E1 ⋈ E2 = E2 ⋈ E1
def make_join_commutative(join_node: QueryTree):
    if join_node.type == "JOIN" and len(join_node.childs) == 2:
        join_node.childs[0], join_node.childs[1] = join_node.childs[1], join_node.childs[0]
        join_node.childs[0].parent = join_node
        join_node.childs[1].parent = join_node
    return join_node

# Natural join asosiatif: (E1 ⋈ E2) ⋈ E3 = E1 ⋈ (E2 ⋈ E3)
def associate_natural_join(node: QueryTree) -> QueryTree:
    if node.type == "JOIN" and _is_natural(node):
        L, R = node.childs
        if L.type == "JOIN" and _is_natural(L):
            # (A ⋈ B) ⋈ C  =>  A ⋈ (B ⋈ C)
            A = L.childs[0]; B = L.childs[1]; C = R
            inner = QueryTree("JOIN", "NATURAL", [B, C]); B.parent = inner; C.parent = inner
            rot = QueryTree("JOIN", "NATURAL", [A, inner]); A.parent = rot; inner.parent = rot
            return rot
        if R.type == "JOIN" and _is_natural(R):
            # A ⋈ (B ⋈ C)  =>  (A ⋈ B) ⋈ C   (bentuk lain, tapi kita sediakan juga)
            B = R.childs[0]; C = R.childs[1]; A = L
            inner = QueryTree("JOIN", "NATURAL", [A, B]); A.parent = inner; B.parent = inner
            rot = QueryTree("JOIN", "NATURAL", [inner, C]); inner.parent = rot; C.parent = rot
            return rot
    return node

# Theta join asosiatif (syarat θ2 hanya atribut E2 dan E3)
# (E1 ⋈θ1 E2) ⋈θ12 E3 = E1 ⋈θ12 (E2 ⋈θ2 E3)
def associate_theta_join(node: QueryTree) -> QueryTree:
    if node.type != "JOIN" or not _is_theta(node):
        return node
    L, R = node.childs
    # kasus (E1⋈θ1E2) ⋈θ12 E3
    if L.type == "JOIN" and _is_theta(L):
        A, B = L.childs
        C = R
        inner = QueryTree("JOIN", L.val, [B, C]); B.parent = inner; C.parent = inner
        rot = QueryTree("JOIN", node.val, [A, inner]); A.parent = rot; inner.parent = rot
        return rot
    # kasus E1 ⋈θ12 (E2⋈θ2E3)
    if R.type == "JOIN" and _is_theta(R):
        A = L
        B, C = R.childs
        inner = QueryTree("JOIN", R.val, [B, C]); B.parent = inner; C.parent = inner
        rot = QueryTree("JOIN", node.val, [A, inner]); A.parent = rot; inner.parent = rot
        return rot
    return node

def plan_cost(node: QueryTree, stats: dict) -> int:
    """Biaya sederhana: 
       TABLE: b_r
       JOIN:  cost(left)+cost(right) + left_rows * right_blocks + left_blocks
       (mengarah ke left-deep yang memanfaatkan tabel kecil/seleksi awal)"""
    if node.type == "TABLE":
        t = node.val
        if isinstance(t, TableReference):
            t = t.name
        return stats.get(t, {}).get("b_r", 1000)

    if node.type == "SIGMA":
        return plan_cost(node.childs[0], stats) if node.childs else 0

    if node.type == "JOIN":
        L, R = node.childs
        cl = plan_cost(L, stats)
        cr = plan_cost(R, stats)
        def rows(n):
            if n.type == "TABLE":
                t = n.val
                if isinstance(t, TableReference):
                    t = t.name
                return stats.get(t, {}).get("n_r", 1000)
            if n.type == "JOIN":
                return max(rows(n.childs[0]), rows(n.childs[1]))
            if n.type == "SIGMA":
                return max(1, rows(n.childs[0]) // 2)
            return 1000
        def blocks(n):
            if n.type == "TABLE":
                t = n.val
                if isinstance(t, TableReference):
                    t = t.name
                return stats.get(t, {}).get("b_r", 100)
            if n.type == "JOIN":
                return blocks(n.childs[0]) + blocks(n.childs[1])
            if n.type == "SIGMA":
                return max(1, blocks(n.childs[0]) // 2)
            return 100
        nl = rows(L) * blocks(R) + blocks(L)  # nested-loop approx
        return cl + cr + nl

    # node lain: jumlahkan anak
    return sum(plan_cost(c, stats) for c in node.childs)

def choose_best(plans, stats: dict) -> QueryTree:
    best = None
    best_cost = None
    for p in plans:
        c = plan_cost(p, stats)
        if best is None or c < best_cost:
            best, best_cost = p, c
    return best

def build_join_tree(order, join_conditions: dict = None) -> QueryTree:
    if join_conditions is None:
        join_conditions = {}

    if not order:
        return None

    cur = QueryTree("TABLE", order[0])
    for i in range(1, len(order)):
        name = order[i]
        right = QueryTree("TABLE", name)

        # find representative table name for left subtree
        try:
            cur_table = _first_table(cur)
        except Exception:
            cur_table = order[0]

        next_table = name

        # direct key lookup
        key = frozenset({cur_table, next_table})
        pred = join_conditions.get(key, "")

        # if not found, try any table in left subtree paired with next_table
        if not pred:
            left_tables = list(_tables_under(cur))
            for lt in left_tables:
                k2 = frozenset({lt, next_table})
                if k2 in join_conditions:
                    pred = join_conditions[k2]
                    break

        # if still not found, try symmetrical: any table in right subtree vs any in left
        if not pred:
            for jt_key in join_conditions.keys():
                try:
                    if isinstance(jt_key, (set, frozenset)) and next_table in jt_key:
                        if any(lt in jt_key for lt in list(_tables_under(cur))):
                            pred = join_conditions[jt_key]
                            break
                except Exception:
                    continue

        val = _mk_theta(pred) if pred else "CARTESIAN"
        cur = QueryTree("JOIN", val, [cur, right])
        cur.childs[0].parent = cur
        right.parent = cur

    return cur

def _first_table(node: QueryTree) -> str:
    if node.type == "TABLE": 
        if isinstance(node.val, TableReference):
            return node.val.name
        return node.val
    return _first_table(node.childs[0])

# Pipeline: dari ParsedQuery → best join plan
def join_order_optimize(pq, stats: dict):
    """Ambil tabel dari pohon (dummy SELECT/ FROM) lalu buat 3-5 kandidat urutan,
       pilih yang biaya terendah."""
    tables = list(_tables_under(pq.query_tree))
    if len(tables) <= 1:
        return pq  # tidak ada join

    # buat beberapa kandidat (tanpa itertools)
    orders = _some_permutations(tables, max_count=5)
    # map kondisi join dummy (bisa diisi dari parser logis jika sudah)
    join_map = {}  # {frozenset({'A','B'}): 'A.x=B.y', ...}
    plans = [build_join_tree(o[:], join_map) for o in orders]
    best = choose_best(plans, stats)
    return pq.__class__(pq.query, best)

def _some_permutations(items, max_count=5):
    res = []
    used = [False]*len(items)
    cur = []
    def bt():
        if len(cur) == len(items):
            res.append(cur[:])
            return
        if len(res) >= max_count: return
        for i in range(len(items)):
            if not used[i]:
                used[i] = True
                cur.append(items[i])
                bt()
                cur.pop()
                used[i] = False
    bt()
    return res if res else [items]


# aturan 1: operasi seleksi konjunktif dapat diuraikan menjadi urutan seleksi
# σ₁∧₂(E) = σ₁(σ₂(E))
def decompose_conjunctive_selection(tree: QueryTree) -> QueryTree:
    sigma_node = _find_node(tree, "SIGMA")
    if not sigma_node or sigma_node.val is None:
        return tree
    
    # hanya proses jika val adalah LogicalNode dengan AND
    if not isinstance(sigma_node.val, LogicalNode) or sigma_node.val.operator != "AND":
        return tree
    
    conditions = sigma_node.val.childs
    
    current = sigma_node.childs[0] if sigma_node.childs else None
    
    for cond in reversed(conditions):
        sigma = QueryTree("SIGMA", cond)
        if current:
            sigma.add_child(current)
        current = sigma
    
    if sigma_node.parent:
        sigma_node.parent.replace_child(sigma_node, current)
        current.parent = sigma_node.parent
        return tree
    else:
        current.parent = None
        return current

# aturan 2: operasi seleksi bersifat komutatif
# σ₁(σ₂(E)) = σ₂(σ₁(E))
def swap_selection_order(tree: QueryTree) -> QueryTree:
    sigma1 = _find_node(tree, "SIGMA")
    if not sigma1 or not sigma1.childs or sigma1.childs[0].type != "SIGMA":
        return tree
    
    # sigma1 adalah outer SIGMA dengan nilai θ1
    # sigma2 adalah inner SIGMA dengan nilai θ2
    # Struktur awal: σ₁(σ₂(E))
    # Hasil: σ₂(σ₁(E))
    
    sigma1_val = sigma1.val  # θ1
    sigma2 = sigma1.childs[0]
    sigma2_val = sigma2.val  # θ2
    child_of_sigma2 = sigma2.childs[0] if sigma2.childs else None  # E
    
    # Buat struktur baru: σ₂(σ₁(E))
    # Inner sigma: σ₁(E)
    new_inner_sigma = QueryTree("SIGMA", sigma1_val)
    if child_of_sigma2:
        new_inner_sigma.add_child(child_of_sigma2)
    
    # Outer sigma: σ₂(σ₁(E))
    new_outer_sigma = QueryTree("SIGMA", sigma2_val)
    new_outer_sigma.add_child(new_inner_sigma)

    if sigma1.parent:
        sigma1.parent.replace_child(sigma1, new_outer_sigma)
        new_outer_sigma.parent = sigma1.parent
        return tree
    else:
        new_outer_sigma.parent = None
        return new_outer_sigma

# aturan 3: hanya proyeksi terakhir dalam urutan proyeksi yang diperlukan
# ΠL₁(ΠL₂(...ΠLn(E))) = ΠL₁(E)
def eliminate_redundant_projections(tree: QueryTree) -> QueryTree:
    proj_node = _find_node(tree, "PROJECT")
    if not proj_node or not proj_node.childs or proj_node.childs[0].type != "PROJECT":
        return tree
    
    current = proj_node.childs[0]
    
    while current.childs and current.childs[0].type == "PROJECT":
        current = current.childs[0]
    
    if current.childs:
        proj_node.childs = [current.childs[0]]
        current.childs[0].parent = proj_node
    
    return tree

# aturan 4a: distribusi seleksi terhadap join (kondisi hanya untuk satu tabel)
# σθ₀(E₁⋈E₂) = (σθ₀(E₁)) ⋈ E₂
def push_selection_through_join_single(tree: QueryTree) -> QueryTree:
    sigma_node = _find_node(tree, "SIGMA")
    if not sigma_node or not sigma_node.childs or sigma_node.childs[0].type != "JOIN":
        return tree
    
    # hanya proses jika val adalah ConditionNode (bukan LogicalNode)
    if not isinstance(sigma_node.val, ConditionNode):
        return tree
    
    join_node = sigma_node.childs[0]
    if len(join_node.childs) < 2:
        return tree
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    
    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    
    cond_tables = _get_tables_from_condition(sigma_node.val)
    
    belongs_to_left = cond_tables.issubset(left_tables) if cond_tables else False
    belongs_to_right = cond_tables.issubset(right_tables) if cond_tables else False
    
    if belongs_to_left and not belongs_to_right:
        sigma_left = QueryTree("SIGMA", sigma_node.val)
        sigma_left.add_child(left_table)
        
        join_node.childs[0] = sigma_left
        sigma_left.parent = join_node
        
        if sigma_node.parent:
            sigma_node.parent.replace_child(sigma_node, join_node)
            join_node.parent = sigma_node.parent
            return tree
        else:
            join_node.parent = None
            return join_node
    
    elif belongs_to_right and not belongs_to_left:
        sigma_right = QueryTree("SIGMA", sigma_node.val)
        sigma_right.add_child(right_table)
        
        join_node.childs[1] = sigma_right
        sigma_right.parent = join_node
        
        if sigma_node.parent:
            sigma_node.parent.replace_child(sigma_node, join_node)
            join_node.parent = sigma_node.parent
            return tree
        else:
            join_node.parent = None
            return join_node
    
    return tree

# aturan 4b: distribusi seleksi terhadap join (kondisi untuk kedua tabel)
# σ(θ₁∧θ₂)(E₁⋈E₂) = (σθ₁(E₁)) ⋈ (σθ₂(E₂))
def push_selection_through_join_split(tree: QueryTree) -> QueryTree:
    sigma_node = _find_node(tree, "SIGMA")
    if not sigma_node or not sigma_node.childs or sigma_node.childs[0].type != "JOIN":
        return tree
    
    # hanya proses jika val adalah LogicalNode dengan AND
    if not isinstance(sigma_node.val, LogicalNode) or sigma_node.val.operator != "AND":
        return tree
    
    join_node = sigma_node.childs[0]
    if len(join_node.childs) < 2:
        return tree
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    
    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    
    left_conditions = []
    right_conditions = []
    mixed_conditions = []
    
    for cond in sigma_node.val.childs:
        cond_tables = _get_tables_from_condition(cond)
        
        belongs_to_left = cond_tables.issubset(left_tables) if cond_tables else False
        belongs_to_right = cond_tables.issubset(right_tables) if cond_tables else False
        
        if belongs_to_left and not belongs_to_right:
            left_conditions.append(cond)
        elif belongs_to_right and not belongs_to_left:
            right_conditions.append(cond)
        else:
            mixed_conditions.append(cond)
    
    if left_conditions or right_conditions:
        if left_conditions:
            if len(left_conditions) == 1:
                left_cond = left_conditions[0]
            else:
                left_cond = LogicalNode("AND", left_conditions)
            sigma_left = QueryTree("SIGMA", left_cond)
            sigma_left.add_child(left_table)
            join_node.childs[0] = sigma_left
            sigma_left.parent = join_node
        
        if right_conditions:
            if len(right_conditions) == 1:
                right_cond = right_conditions[0]
            else:
                right_cond = LogicalNode("AND", right_conditions)
            sigma_right = QueryTree("SIGMA", right_cond)
            sigma_right.add_child(right_table)
            join_node.childs[1] = sigma_right
            sigma_right.parent = join_node
        
        if mixed_conditions:
            if len(mixed_conditions) == 1:
                sigma_node.val = mixed_conditions[0]
            else:
                sigma_node.val = LogicalNode("AND", mixed_conditions)
            return tree
        else:
            if sigma_node.parent:
                sigma_node.parent.replace_child(sigma_node, join_node)
                join_node.parent = sigma_node.parent
                return tree
            else:
                join_node.parent = None
                return join_node
    
    return tree

# aturan 5a: distribusi proyeksi terhadap join (simple case)
# ΠL₁∪L₂(E₁⋈E₂) = (ΠL₁(E₁)) ⋈ (ΠL₂(E₂))
def push_projection_through_join_simple(tree: QueryTree) -> QueryTree:
    proj_node = _find_node(tree, "PROJECT")
    if not proj_node or not proj_node.childs or proj_node.childs[0].type != "JOIN":
        return tree
    
    # hanya proses jika val adalah list of ColumnNode
    if not isinstance(proj_node.val, list) or proj_node.val == "*":
        return tree
    
    join_node = proj_node.childs[0]
    if len(join_node.childs) < 2:
        return tree
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    
    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    
    left_cols = []
    right_cols = []
    unassigned_cols = []
    
    for col in proj_node.val:
        if not isinstance(col, ColumnNode):
            continue
        
        if col.table:
            if col.table in left_tables:
                left_cols.append(col)
            elif col.table in right_tables:
                right_cols.append(col)
            else:
                unassigned_cols.append(col)
        else:
            unassigned_cols.append(col)
    
    if left_cols and right_cols and not unassigned_cols:
        left_proj = QueryTree("PROJECT", left_cols)
        left_proj.add_child(left_table)
        join_node.childs[0] = left_proj
        left_proj.parent = join_node
        
        right_proj = QueryTree("PROJECT", right_cols)
        right_proj.add_child(right_table)
        join_node.childs[1] = right_proj
        right_proj.parent = join_node
        
        if proj_node.parent:
            proj_node.parent.replace_child(proj_node, join_node)
            join_node.parent = proj_node.parent
            return tree
        else:
            join_node.parent = None
            return join_node
    
    return tree

# aturan 5b: distribusi proyeksi terhadap join (dengan atribut join)
# ΠL₁∪L₂(E₁⋈θE₂) = ΠL₁∪L₂((ΠL₁∪L₃(E₁)) ⋈θ (ΠL₂∪L₄(E₂)))
def push_projection_through_join_with_join_attrs(tree: QueryTree) -> QueryTree:
    proj_node = _find_node(tree, "PROJECT")
    if not proj_node or not proj_node.childs or proj_node.childs[0].type != "JOIN":
        return tree
    
    # hanya proses jika val adalah list of ColumnNode
    if not isinstance(proj_node.val, list) or proj_node.val == "*":
        return tree
    
    join_node = proj_node.childs[0]
    if len(join_node.childs) < 2:
        return tree
    
    if not _is_theta(join_node):
        return tree
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    
    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    
    theta_condition = _theta_pred(join_node)
    join_attrs = _extract_attributes_from_condition(theta_condition)
    
    left_cols = []
    right_cols = []
    left_join_attrs = []
    right_join_attrs = []
    
    for col in proj_node.val:
        if not isinstance(col, ColumnNode):
            continue
        
        if col.table:
            if col.table in left_tables:
                left_cols.append(col)
            elif col.table in right_tables:
                right_cols.append(col)
    
    for attr in join_attrs:
        if '.' in attr:
            table, column = attr.split('.', 1)
            col_node = ColumnNode(column, table)
            
            if table in left_tables:
                if not any(c.column == column and c.table == table for c in left_cols):
                    left_join_attrs.append(col_node)
            elif table in right_tables:
                if not any(c.column == column and c.table == table for c in right_cols):
                    right_join_attrs.append(col_node)
    
    if left_cols or right_cols:
        left_all = left_cols + left_join_attrs
        if left_all:
            left_proj = QueryTree("PROJECT", left_all)
            left_proj.add_child(left_table)
            join_node.childs[0] = left_proj
            left_proj.parent = join_node
        
        right_all = right_cols + right_join_attrs
        if right_all:
            right_proj = QueryTree("PROJECT", right_all)
            right_proj.add_child(right_table)
            join_node.childs[1] = right_proj
            right_proj.parent = join_node
        
        return tree
    
    return tree

# helper untuk extract atribut dari string kondisi
def _extract_attributes_from_condition(condition: str) -> list:
    if not condition:
        return []
    
    pattern = r'\b\w+\.\w+\b'
    matches = re.findall(pattern, condition)
    return matches

# helper untuk extract tables dari condition node
def _get_tables_from_condition(cond) -> set:
    tables = set()
    
    if isinstance(cond, ConditionNode):
        if isinstance(cond.attr, dict) and cond.attr.get('table'):
            tables.add(cond.attr['table'])
        elif isinstance(cond.attr, ColumnNode) and cond.attr.table:
            tables.add(cond.attr.table)
        
        if isinstance(cond.value, dict) and cond.value.get('table'):
            tables.add(cond.value['table'])
        elif isinstance(cond.value, ColumnNode) and cond.value.table:
            tables.add(cond.value.table)
    
    elif isinstance(cond, LogicalNode):
        for child in cond.childs:
            tables.update(_get_tables_from_condition(child))
    
    return tables

def validate_query(query: str) -> tuple:
    # validasi sintaks query sql
    query = query.strip()
    
    if not query.endswith(";"):
        return False, "Query must end with a semicolon."
    
    q_clean = query.rstrip(';').strip()
    if not q_clean:
        return False, "Query is empty."
    
    select_pattern = re.compile(
        r'^\s*SELECT\s+.+?\s+FROM\s+.+?' 
        r'(\s+JOIN\s+.+?\s+ON\s+.+?)?'
        r'(\s+NATURAL\s+JOIN\s+.+?)?'
        r'(\s+WHERE\s+.+?)?' 
        r'(\s+GROUP\s+BY\s+.+?)?'
        r'(\s+ORDER\s+BY\s+.+?)?'
        r'(\s+LIMIT\s+\d+)?' 
        r'\s*;$',
        re.IGNORECASE | re.DOTALL
    )
    
    other_patterns = {
        "UPDATE": re.compile(
            r'^\s*UPDATE\s+\w+\s+SET\s+.+?(\s+WHERE\s+.+?)?\s*;$',
            re.IGNORECASE | re.DOTALL
        ),
        "DELETE": re.compile(
            r'^\s*DELETE\s+FROM\s+\w+(\s+WHERE\s+.+?)?\s*;$',
            re.IGNORECASE
        ),
        "INSERT": re.compile(
            r'^\s*INSERT\s+INTO\s+\w+\s*\(.+?\)\s+VALUES\s*\(.+?\)\s*;$',
            re.IGNORECASE
        ),
        "CREATE": re.compile(
            r'^\s*CREATE\s+TABLE\s+\w+\s*\(.+?\)\s*;$',
            re.IGNORECASE | re.DOTALL
        ),
        "DROP": re.compile(
            r'^\s*DROP\s+TABLE\s+\w+\s*(CASCADE|RESTRICT)?\s*;$',
            re.IGNORECASE
        ),
        "BEGIN": re.compile(
            r'^\s*BEGIN\s+TRANSACTION\s*;$',
            re.IGNORECASE
        ),
        "COMMIT": re.compile(
            r'^\s*COMMIT\s*;$',
            re.IGNORECASE
        ),
        "ROLLBACK": re.compile(
            r'^\s*ROLLBACK\s*;$',
            re.IGNORECASE
        )
    }
    
    query_type = q_clean.split(maxsplit=1)[0].upper()
    
    if query_type == "SELECT":
        if select_pattern.match(query):
            clause_order = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
            last_seen_index = -1
            
            for clause in clause_order:
                if clause == "GROUP BY":
                    clause_pos = query.upper().find("GROUP BY")
                elif clause == "ORDER BY":
                    clause_pos = query.upper().find("ORDER BY")
                else:
                    clause_pos = query.upper().find(clause)
                    
                if clause_pos != -1:
                    if clause_pos < last_seen_index:
                        return False, f"Invalid clause order: {clause} appears out of sequence."
                    last_seen_index = clause_pos
            
            return True, "Valid SELECT query."
        else:
            return False, "Invalid SELECT query syntax."
    
    if query_type in other_patterns:
        if other_patterns[query_type].match(query):
            return True, f"Valid {query_type} query."
        else:
            return False, f"Invalid {query_type} query syntax."
    
    return False, f"Unsupported query type: {query_type}"


# helper untuk extract columns dari SELECT clause
def _get_columns_from_select(query: str) -> str:
    q_upper = query.upper()
    select_idx = q_upper.find("SELECT") + 6
    from_idx = q_upper.find("FROM")
    
    if from_idx == -1:
        columns = query[select_idx:].strip()
    else:
        columns = query[select_idx:from_idx].strip()
    
    return columns

# helper untuk extract table dari FROM clause
def _get_from_table(query: str) -> str:
    q_upper = query.upper()
    from_idx = q_upper.find("FROM") + 4
    
    end_keywords = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        if keyword == "GROUP BY":
            idx = q_upper.find("GROUP BY", from_idx)
        elif keyword == "ORDER BY":
            idx = q_upper.find("ORDER BY", from_idx)
        else:
            idx = q_upper.find(keyword, from_idx)
        
        if idx != -1 and idx < end_idx:
            end_idx = idx
    
    return query[from_idx:end_idx].strip()

# helper untuk extract condition dari WHERE clause
def _get_condition_from_where(query: str) -> str:
    q_upper = query.upper()
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        return ""
    
    where_idx += 5
    
    end_keywords = ["GROUP BY", "ORDER BY", "LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        if keyword == "GROUP BY":
            idx = q_upper.find("GROUP BY", where_idx)
        elif keyword == "ORDER BY":
            idx = q_upper.find("ORDER BY", where_idx)
        else:
            idx = q_upper.find(keyword, where_idx)
        
        if idx != -1 and idx < end_idx:
            end_idx = idx
    
    return query[where_idx:end_idx].strip()

# helper untuk extract limit value
def _get_limit(query: str) -> int:
    q_upper = query.upper()
    limit_idx = q_upper.find("LIMIT") + 5
    
    limit_str = query[limit_idx:].strip().split()[0]
    return int(limit_str)

# helper untuk extract order by info
def _get_order_by_info(query: str) -> str:
    q_upper = query.upper()
    order_idx = q_upper.find("ORDER BY") + 8
    
    end_keywords = ["LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        idx = q_upper.find(keyword, order_idx)
        if idx != -1:
            end_idx = idx
            break
    
    order_clause = query[order_idx:end_idx].strip()
    
    if "DESC" in order_clause.upper():
        return order_clause
    elif "ASC" in order_clause.upper():
        return order_clause
    else:
        return f"{order_clause} ASC"

# helper untuk extract group by column
def _get_column_from_group_by(query: str) -> str:
    q_upper = query.upper()
    group_idx = q_upper.find("GROUP BY") + 8
    
    end_keywords = ["ORDER BY", "LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        if keyword == "ORDER BY":
            idx = q_upper.find("ORDER BY", group_idx)
        else:
            idx = q_upper.find(keyword, group_idx)
        
        if idx != -1:
            end_idx = idx
            break
    
    return query[group_idx:end_idx].strip()

# parse from clause dan return query tree node
def _parse_from_clause(query: str) -> QueryTree:
    from_tables = _get_from_table(query)
    q_upper = from_tables.upper()
    
    # case 1: NATURAL JOIN
    if "NATURAL JOIN" in q_upper:
        join_split = re.split(r'\s+NATURAL\s+JOIN\s+', from_tables, flags=re.IGNORECASE)
        
        left_table = _parse_table_with_alias(join_split[0].strip())
        
        for right_table_str in join_split[1:]:
            right_table = _parse_table_with_alias(right_table_str.strip())
            
            join_node = QueryTree(type="JOIN", val=NaturalJoin())
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # case 2: regular JOIN with ON
    elif "JOIN" in q_upper and "ON" in q_upper:
        join_split = re.split(r'\s+JOIN\s+', from_tables, flags=re.IGNORECASE)
        
        left_table = _parse_table_with_alias(join_split[0].strip())
        
        for join_part in join_split[1:]:
            temp = re.split(r'\s+ON\s+', join_part, flags=re.IGNORECASE)
            right_table_str = temp[0].strip()
            join_condition_str = temp[1].strip() if len(temp) > 1 else ""
            
            right_table = _parse_table_with_alias(right_table_str)
            
            join_condition_str = join_condition_str.replace("(", "").replace(")", "")
            
            # parse join condition to ConditionNode
            join_cond = _parse_single_condition(join_condition_str)
            
            join_node = QueryTree(type="JOIN", val=ThetaJoin(join_cond))
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # case 3: comma-separated tables (cartesian product)
    elif "," in from_tables:
        tables = [t.strip() for t in from_tables.split(",")]
        
        left_table = _parse_table_with_alias(tables[0])
        
        for table_str in tables[1:]:
            right_table = _parse_table_with_alias(table_str)
            
            join_node = QueryTree(type="JOIN", val="CARTESIAN")
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # case 4: single table
    else:
        return _parse_table_with_alias(from_tables.strip())

# parse table string dengan optional alias dan return TABLE node
def _parse_table_with_alias(table_str: str) -> QueryTree:
    if " AS " in table_str.upper():
        parts = re.split(r'\s+AS\s+', table_str, flags=re.IGNORECASE)
        table_name = parts[0].strip()
        alias = parts[1].strip()
        table_ref = TableReference(table_name, alias)
        return QueryTree(type="TABLE", val=table_ref)
    else:
        table_ref = TableReference(table_str)
        return QueryTree(type="TABLE", val=table_ref)

# helper untuk extract SET conditions dari UPDATE
def _extract_set_conditions(query: str) -> list:
    q_upper = query.upper()
    set_idx = q_upper.find("SET") + 3
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        set_part = query[set_idx:].strip()
    else:
        set_part = query[set_idx:where_idx].strip()
    
    conditions = [c.strip() for c in set_part.split(",")]
    return conditions

# helper untuk extract table name dari UPDATE
def _extract_table_update(query: str) -> str:
    q_upper = query.upper()
    update_idx = q_upper.find("UPDATE") + 6
    set_idx = q_upper.find("SET")
    
    return query[update_idx:set_idx].strip()

# helper untuk extract table name dari DELETE
def _extract_table_delete(query: str) -> str:
    q_upper = query.upper()
    from_idx = q_upper.find("FROM") + 4
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        return query[from_idx:].strip()
    else:
        return query[from_idx:where_idx].strip()

# helper untuk extract table name dari INSERT
def _extract_table_insert(query: str) -> str:
    q_upper = query.upper()
    into_idx = q_upper.find("INTO") + 4
    
    paren_idx = query.find("(", into_idx)
    
    return query[into_idx:paren_idx].strip()

# helper untuk extract columns dari INSERT
def _extract_columns_insert(query: str) -> str:
    start_idx = query.find("(")
    end_idx = query.find(")", start_idx)
    
    columns = query[start_idx+1:end_idx]
    return columns

# helper untuk extract values dari INSERT
def _extract_values_insert(query: str) -> str:
    q_upper = query.upper()
    values_idx = q_upper.find("VALUES")
    
    if values_idx == -1:
        raise Exception("INSERT query must contain VALUES clause")
    
    start_idx = query.find("(", values_idx)
    end_idx = query.find(")", start_idx)
    
    values = query[start_idx+1:end_idx]
    return values

# parse DROP TABLE statement
def _parse_drop_table(query: str) -> tuple:
    q_upper = query.upper()
    
    drop_idx = q_upper.find("DROP TABLE") + 10
    table_part = query[drop_idx:].strip().rstrip(';').strip()
    
    mode = "RESTRICT"
    
    if "CASCADE" in table_part.upper():
        mode = "CASCADE"
        table_name = table_part[:table_part.upper().find("CASCADE")].strip()
    elif "RESTRICT" in table_part.upper():
        mode = "RESTRICT"
        table_name = table_part[:table_part.upper().find("RESTRICT")].strip()
    else:
        table_name = table_part
    
    return table_name, mode == "CASCADE"

# parse CREATE TABLE statement
def _parse_create_table(query: str) -> tuple:
    q_upper = query.upper()
    
    create_idx = q_upper.find("CREATE TABLE") + 12
    paren_idx = query.find("(", create_idx)
    table_name = query[create_idx:paren_idx].strip()
    
    content_start = paren_idx + 1
    content_end = query.rfind(")")
    content = query[content_start:content_end].strip()
    
    parts = []
    current_part = ""
    paren_depth = 0
    
    for char in content:
        if char == '(':
            paren_depth += 1
            current_part += char
        elif char == ')':
            paren_depth -= 1
            current_part += char
        elif char == ',' and paren_depth == 0:
            parts.append(current_part.strip())
            current_part = ""
        else:
            current_part += char
    
    if current_part.strip():
        parts.append(current_part.strip())
    
    columns = []
    primary_key = []
    foreign_keys = []
    
    for part in parts:
        part_upper = part.upper()
        
        if part_upper.startswith("PRIMARY KEY"):
            pk_content = part[part.find("(")+1:part.find(")")].strip()
            primary_key = [c.strip() for c in pk_content.split(",")]
        
        elif part_upper.startswith("FOREIGN KEY"):
            fk_match = re.match(
                r'FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)',
                part,
                re.IGNORECASE
            )
            if fk_match:
                fk = ForeignKeyDefinition(
                    fk_match.group(1),
                    fk_match.group(2),
                    fk_match.group(3)
                )
                foreign_keys.append(fk)
        
        else:
            tokens = part.split()
            if len(tokens) >= 2:
                col_name = tokens[0]
                col_type = tokens[1].lower()
                
                size = None
                if "(" in col_type:
                    type_match = re.match(r'(\w+)\((\d+)\)', col_type)
                    if type_match:
                        col_type = type_match.group(1)
                        size = int(type_match.group(2))
                
                col_def = ColumnDefinition(col_name, col_type, size)
                columns.append(col_def)
    
    return table_name, columns, primary_key, foreign_keys

# parse WHERE condition string dan return ConditionNode atau LogicalNode
def parse_where_condition(where_str):
    if not where_str or not where_str.strip():
        return None
    
    where_str = where_str.strip()
    
    # split by OR (lowest precedence)
    or_parts = _split_by_keyword(where_str, ' OR ')
    
    if len(or_parts) > 1:
        childs = [parse_where_condition(part) for part in or_parts]
        return LogicalNode("OR", childs)
    
    # split by AND
    and_parts = _split_by_keyword(where_str, ' AND ')
    
    if len(and_parts) > 1:
        childs = [_parse_single_condition(part) for part in and_parts]
        return LogicalNode("AND", childs)
    
    # single comparison
    return _parse_single_condition(where_str)

# split string by keyword while respecting nesting
def _split_by_keyword(text, keyword):
    parts = []
    current = ""
    i = 0
    
    while i < len(text):
        if text[i:i+len(keyword)].upper() == keyword.upper():
            parts.append(current.strip())
            current = ""
            i += len(keyword)
        else:
            current += text[i]
            i += 1
    
    if current.strip():
        parts.append(current.strip())
    
    return parts if len(parts) > 1 else [text]

# parse single comparison condition string dan return ConditionNode
def _parse_single_condition(condition_str):
    condition_str = condition_str.strip()
    
    operators = ['<>', '>=', '<=', '!=', '=', '>', '<']
    
    for op in operators:
        if op in condition_str:
            parts = condition_str.split(op, 1)
            if len(parts) == 2:
                left_str = parts[0].strip()
                right_str = parts[1].strip()
                
                left = _parse_column_reference(left_str)
                right = _parse_value_or_column(right_str)
                
                return ConditionNode(left, op, right)
    
    raise Exception(f"Cannot parse condition: {condition_str}")

# parse column reference string dan return ColumnNode
def _parse_column_reference(col_str):
    col_str = col_str.strip()
    
    if '.' in col_str:
        parts = col_str.split('.', 1)
        return ColumnNode(parts[1].strip(), parts[0].strip())
    else:
        return ColumnNode(col_str)

# parse value atau column reference
def _parse_value_or_column(value_str):
    value_str = value_str.strip()
    
    # string literal
    if (value_str.startswith("'") and value_str.endswith("'")) or \
       (value_str.startswith('"') and value_str.endswith('"')):
        return value_str[1:-1]
    
    # column reference (tapi cek angka dulu, siapa tau float)
    try:
        if '.' in value_str:
            return float(value_str)
        else:
            return int(value_str)
    except ValueError:
        pass
    
    # try number
    try:
        if '.' not in value_str:
            return int(value_str)
        else:
            return float(value_str)
    except ValueError:
        return value_str

# parse columns string dan return list of ColumnNode atau "*"
def parse_columns_from_string(columns_str):
    if columns_str.strip() == "*":
        return "*"
    
    columns = []
    parts = columns_str.split(',')
    
    for part in parts:
        part = part.strip()
        if part:
            col_node = _parse_column_reference(part)
            columns.append(col_node)
    
    return columns

# parse ORDER BY string dan return list of OrderByItem
def parse_order_by_string(order_str):
    if not order_str or not order_str.strip():
        return []
    
    result = []
    parts = order_str.split(',')
    
    for part in parts:
        part = part.strip()
        
        if part.upper().endswith(' DESC'):
            col_str = part[:-5].strip()
            direction = 'DESC'
        elif part.upper().endswith(' ASC'):
            col_str = part[:-4].strip()
            direction = 'ASC'
        else:
            col_str = part
            direction = 'ASC'
        
        col_node = _parse_column_reference(col_str)
        result.append(OrderByItem(col_node, direction))
    
    return result

# parse GROUP BY string dan return list of ColumnNode
def parse_group_by_string(group_str):
    if not group_str or not group_str.strip():
        return []
    
    result = []
    parts = group_str.split(',')
    
    for part in parts:
        part = part.strip()
        if part:
            col_node = _parse_column_reference(part)
            result.append(col_node)
    
    return result

# parse INSERT columns string dan return list of strings
def parse_insert_columns_string(columns_str):
    if not columns_str or not columns_str.strip():
        return []
    
    return [col.strip() for col in columns_str.split(',')]

# parse INSERT values string dan return list of values
def parse_insert_values_string(values_str):
    if not values_str or not values_str.strip():
        return []
    
    result = []
    current = ""
    in_string = False
    quote_char = None
    
    for i, char in enumerate(values_str):
        if char in ["'", '"'] and (i == 0 or values_str[i-1] != '\\'):
            if not in_string:
                in_string = True
                quote_char = char
            elif char == quote_char:
                in_string = False
                result.append(current)
                current = ""
                quote_char = None
            else:
                current += char
        elif char == ',' and not in_string:
            if current.strip():
                val_str = current.strip()
                try:
                    if '.' in val_str:
                        result.append(float(val_str))
                    else:
                        result.append(int(val_str))
                except ValueError:
                    result.append(val_str)
                current = ""
        else:
            if in_string or char != ' ' or current:
                current += char
    
    if current.strip():
        val_str = current.strip()
        try:
            if '.' in val_str:
                result.append(float(val_str))
            else:
                result.append(int(val_str))
        except ValueError:
            result.append(val_str)
    
    return result