from model.query_tree import QueryTree
import re

# util kecil
def _is_cartesian(join_node: QueryTree) -> bool:
    return join_node.type == "JOIN" and (join_node.val == "" or join_node.val.upper() == "CARTESIAN")

def _is_theta(join_node: QueryTree) -> bool:
    return join_node.type == "JOIN" and join_node.val.upper().startswith("THETA:")

def _is_natural(join_node: QueryTree) -> bool:
    return join_node.type == "JOIN" and join_node.val.upper() == "NATURAL"

def _theta_pred(join_node: QueryTree) -> str:
    if not _is_theta(join_node): return ""
    return join_node.val.split(":", 1)[1].strip()

def _mk_theta(pred: str) -> str:
    return f"THETA:{pred.strip()}"

def _tables_under(node: QueryTree):
    """Extract all table names from a query tree"""
    out = []
    def dfs(n):
        if n.type == "TABLE":
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
        return stats.get(t, {}).get("b_r", 1000)

    if node.type == "SIGMA":
        return plan_cost(node.childs[0], stats) if node.childs else 0

    if node.type == "JOIN":
        L, R = node.childs
        cl = plan_cost(L, stats)
        cr = plan_cost(R, stats)
        def rows(n):
            if n.type == "TABLE":
                return stats.get(n.val, {}).get("n_r", 1000)
            if n.type == "JOIN":
                return max(rows(n.childs[0]), rows(n.childs[1]))
            if n.type == "SIGMA":
                return max(1, rows(n.childs[0]) // 2)
            return 1000
        def blocks(n):
            if n.type == "TABLE":
                return stats.get(n.val, {}).get("b_r", 100)
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
    """order: ['A','B','C']
       join_conditions: {frozenset({'A','B'}): 'A.x=B.y', ...}
       Buat left-deep: (((A ⋈ B) ⋈ C) ...)
       Gunakan THETA jika ada predicate, selain itu CARTESIAN."""
    if join_conditions is None:
        join_conditions = {}
    
    if not order:
        return None
    
    cur = QueryTree("TABLE", order[0])
    for i in range(1, len(order)):
        name = order[i]
        right = QueryTree("TABLE", name)
        # cari predicate join
        key = frozenset({order[0], name})
        pred = join_conditions.get(key, "")
        val = _mk_theta(pred) if pred else "CARTESIAN"
        cur = QueryTree("JOIN", val, [cur, right])
        cur.childs[0].parent = cur
        right.parent = cur
    return cur

def _first_table(node: QueryTree) -> str:
    if node.type == "TABLE": return node.val
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


# Aturan 1: Operasi seleksi konjungtif dapat diuraikan menjadi urutan seleksi
# σ₁∧₂(E) = σ₁(σ₂(E))
def decompose_conjunctive_selection(node: QueryTree) -> QueryTree:
    if node.type != "SIGMA" or not node.val:
        return node
    
    if " AND " in node.val.upper():
        conditions = re.split(r'\s+AND\s+', node.val, flags=re.IGNORECASE)
        
        current = node.childs[0] if node.childs else None
        
        for cond in reversed(conditions):
            sigma = QueryTree("SIGMA", cond.strip())
            if current:
                sigma.add_child(current)
            current = sigma
        
        if node.parent:
            node.parent.replace_child(node, current)
            current.parent = node.parent
        else:
            current.parent = None
        
        return current
    return node

# Aturan 2: Operasi seleksi bersifat komutatif
# σ₁(σ₂(E)) = σ₂(σ₁(E))
def swap_selection_order(node: QueryTree) -> QueryTree:
    if node.type == "SIGMA" and node.childs and node.childs[0].type == "SIGMA":
        sigma1_val = node.val
        sigma2 = node.childs[0]
        sigma2_val = sigma2.val
        child_of_sigma2 = sigma2.childs[0] if sigma2.childs else None
        
        new_sigma1 = QueryTree("SIGMA", sigma1_val)
        if child_of_sigma2:
            new_sigma1.add_child(child_of_sigma2)
        
        new_sigma2 = QueryTree("SIGMA", sigma2_val)
        new_sigma2.add_child(new_sigma1)

        if node.parent:
            node.parent.replace_child(node, new_sigma2)
            new_sigma2.parent = node.parent
        else:
            new_sigma2.parent = None
        
        return new_sigma2
    
    return node

# Aturan 3: Hanya proyeksi terakhir dalam urutan proyeksi yang diperlukan
# ΠL₁(ΠL₂(...ΠLn(E))) = ΠL₁(E)
def eliminate_redundant_projections(node: QueryTree) -> QueryTree:
    if node.type == "PROJECT" and node.childs and node.childs[0].type == "PROJECT":
        current = node.childs[0]
        
        while current.childs and current.childs[0].type == "PROJECT":
            current = current.childs[0]
        
        if current.childs:
            node.childs = [current.childs[0]]
            current.childs[0].parent = node
        
        return node
    
    return node

# Aturan 4a: Distribusi seleksi terhadap join (kondisi hanya untuk satu tabel)
# σθ₀(E₁⋈E₂) = (σθ₀(E₁)) ⋈ E₂
# WIP ini, masih bingung cara ambil atribut mana punya tabel siapa
def push_selection_through_join_single(node: QueryTree) -> QueryTree:
    if node.type != "SIGMA" or not node.childs or node.childs[0].type != "JOIN":
        return node
    
    join_node = node.childs[0]
    if len(join_node.childs) < 2:
        return node
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    

    #masih kayak gini
    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    condition = node.val
    belongs_to_left = any(table in condition for table in left_tables)
    belongs_to_right = any(table in condition for table in right_tables)
    
    if belongs_to_left and not belongs_to_right:
        sigma_left = QueryTree("SIGMA", condition)
        sigma_left.add_child(left_table)
        
        join_node.childs[0] = sigma_left
        sigma_left.parent = join_node
        

        if node.parent:
            node.parent.replace_child(node, join_node)
            join_node.parent = node.parent
        else:
            join_node.parent = None
        
        return join_node
    
    elif belongs_to_right and not belongs_to_left:
        sigma_right = QueryTree("SIGMA", condition)
        sigma_right.add_child(right_table)
        
        join_node.childs[1] = sigma_right
        sigma_right.parent = join_node
        
        if node.parent:
            node.parent.replace_child(node, join_node)
            join_node.parent = node.parent
        else:
            join_node.parent = None
        
        return join_node
    
    return node

# Aturan 4b: Distribusi seleksi terhadap join (kondisi untuk kedua tabel)
# σ(θ₁∧θ₂)(E₁⋈E₂) = (σθ₁(E₁)) ⋈ (σθ₂(E₂))
# sama masih WIP
def push_selection_through_join_split(node: QueryTree) -> QueryTree:
    if node.type != "SIGMA" or not node.childs or node.childs[0].type != "JOIN":
        return node
    
    if " AND " not in node.val.upper():
        return node
    
    join_node = node.childs[0]
    if len(join_node.childs) < 2:
        return node
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    
    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    
    conditions = re.split(r'\s+AND\s+', node.val, flags=re.IGNORECASE)
    
    left_conditions = []
    right_conditions = []
    mixed_conditions = []
    
    for cond in conditions:
        belongs_to_left = any(table in cond for table in left_tables)
        belongs_to_right = any(table in cond for table in right_tables)
        
        if belongs_to_left and not belongs_to_right:
            left_conditions.append(cond.strip())
        elif belongs_to_right and not belongs_to_left:
            right_conditions.append(cond.strip())
        else:
            mixed_conditions.append(cond.strip())
    
    if left_conditions or right_conditions:
        if left_conditions:
            left_cond = " AND ".join(left_conditions)
            sigma_left = QueryTree("SIGMA", left_cond)
            sigma_left.add_child(left_table)
            join_node.childs[0] = sigma_left
            sigma_left.parent = join_node
        
        if right_conditions:
            right_cond = " AND ".join(right_conditions)
            sigma_right = QueryTree("SIGMA", right_cond)
            sigma_right.add_child(right_table)
            join_node.childs[1] = sigma_right
            sigma_right.parent = join_node
        
        if mixed_conditions:
            node.val = " AND ".join(mixed_conditions)
            return node
        else:
            if node.parent:
                node.parent.replace_child(node, join_node)
                join_node.parent = node.parent
            else:
                join_node.parent = None
            return join_node
    
    return node

# Aturan 5a: Distribusi proyeksi terhadap join (simple case)
# ΠL₁∪L₂(E₁⋈E₂) = (ΠL₁(E₁)) ⋈ (ΠL₂(E₂))
def push_projection_through_join_simple(node: QueryTree) -> QueryTree:
    if node.type != "PROJECT" or not node.childs or node.childs[0].type != "JOIN":
        return node
    
    join_node = node.childs[0]
    if len(join_node.childs) < 2:
        return node
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    
    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    
    proj_cols = [c.strip() for c in node.val.split(",")]
    
    left_cols = []
    right_cols = []
    
    for col in proj_cols:
        belongs_to_left = any(table in col for table in left_tables)
        belongs_to_right = any(table in col for table in right_tables)
        
        if belongs_to_left:
            left_cols.append(col)
        if belongs_to_right:
            right_cols.append(col)
    
    if left_cols and right_cols:
        if left_cols:
            left_proj = QueryTree("PROJECT", ", ".join(left_cols))
            left_proj.add_child(left_table)
            join_node.childs[0] = left_proj
            left_proj.parent = join_node
        
        if right_cols:
            right_proj = QueryTree("PROJECT", ", ".join(right_cols))
            right_proj.add_child(right_table)
            join_node.childs[1] = right_proj
            right_proj.parent = join_node
        
        if node.parent:
            node.parent.replace_child(node, join_node)
            join_node.parent = node.parent
        else:
            join_node.parent = None
        
        return join_node
    
    return node

# Aturan 5b: Distribusi proyeksi terhadap join (dengan atribut join)
# ΠL₁∪L₂(E₁⋈θE₂) = ΠL₁∪L₂((ΠL₁∪L₃(E₁)) ⋈θ (ΠL₂∪L₄(E₂)))
def push_projection_through_join_with_join_attrs(node: QueryTree) -> QueryTree:
    if node.type != "PROJECT" or not node.childs or node.childs[0].type != "JOIN":
        return node
    
    join_node = node.childs[0]
    if len(join_node.childs) < 2:
        return node
    
    if not _is_theta(join_node):
        return node
    
    left_table = join_node.childs[0]
    right_table = join_node.childs[1]
    

    left_tables = _tables_under(left_table)
    right_tables = _tables_under(right_table)
    
    proj_cols = [c.strip() for c in node.val.split(",")]
    

    theta_condition = _theta_pred(join_node)
    join_attrs = _extract_attributes_from_condition(theta_condition)
    
    left_cols = []
    right_cols = []
    left_join_attrs = []
    right_join_attrs = []
    
    for col in proj_cols:
        belongs_to_left = any(table in col for table in left_tables)
        belongs_to_right = any(table in col for table in right_tables)
        
        if belongs_to_left:
            left_cols.append(col)
        if belongs_to_right:
            right_cols.append(col)
    
    for attr in join_attrs:
        belongs_to_left = any(table in attr for table in left_tables)
        belongs_to_right = any(table in attr for table in right_tables)
        
        if belongs_to_left and attr not in left_cols:
            left_join_attrs.append(attr)
        if belongs_to_right and attr not in right_cols:
            right_join_attrs.append(attr)
    
    if left_cols or right_cols:
        # Left projection
        left_all = left_cols + left_join_attrs
        if left_all:
            left_proj = QueryTree("PROJECT", ", ".join(left_all))
            left_proj.add_child(left_table)
            join_node.childs[0] = left_proj
            left_proj.parent = join_node
        
        # Right projection
        right_all = right_cols + right_join_attrs
        if right_all:
            right_proj = QueryTree("PROJECT", ", ".join(right_all))
            right_proj.add_child(right_table)
            join_node.childs[1] = right_proj
            right_proj.parent = join_node
        
        return node
    
    return node

# HELPER SEMENTARA
def _extract_attributes_from_condition(condition: str) -> list:
    if not condition:
        return []
    
    pattern = r'\b\w+\.\w+\b'
    matches = re.findall(pattern, condition)
    return matches

def validate_query(query: str) -> tuple:

    query = query.strip()
    
    # Check semicolon
    if not query.endswith(";"):
        return False, "Query must end with a semicolon."
    
    q_clean = query.rstrip(';').strip()
    if not q_clean:
        return False, "Query is empty."
    
    
    # SELECT pattern 
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
    
    # Other query patterns
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
    
    # Detect query type
    query_type = q_clean.split(maxsplit=1)[0].upper()
    
    # Validate SELECT query
    if query_type == "SELECT":
        if select_pattern.match(query):
            # Check clause order
            clause_order = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
            last_seen_index = -1
            
            for clause in clause_order:
                # Handle multi-word clauses
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
    
    # Validate other query types
    if query_type in other_patterns:
        if other_patterns[query_type].match(query):
            return True, f"Valid {query_type} query."
        else:
            return False, f"Invalid {query_type} query syntax."
    
    return False, f"Unsupported query type: {query_type}"

# SELECT Helpers
def _get_columns_from_select(query: str) -> str:
    q_upper = query.upper()
    select_idx = q_upper.find("SELECT") + 6
    from_idx = q_upper.find("FROM")
    
    if from_idx == -1:
        columns = query[select_idx:].strip()
    else:
        columns = query[select_idx:from_idx].strip()
    
    return columns

def _get_from_table(query: str) -> str:
    q_upper = query.upper()
    from_idx = q_upper.find("FROM") + 4
    
    # Find next keyword
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

def _get_condition_from_where(query: str) -> str:
    q_upper = query.upper()
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        return ""
    
    where_idx += 5  # len("WHERE")
    
    # Find next keyword
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

def _get_limit(query: str) -> int:
    q_upper = query.upper()
    limit_idx = q_upper.find("LIMIT") + 5
    
    limit_str = query[limit_idx:].strip().split()[0]
    return int(limit_str)

def _get_info_from_order_by(query: str) -> str: # new: ASC and DESC
    q_upper = query.upper()
    order_idx = q_upper.find("ORDER BY") + 8
    
    # Find next keyword
    end_keywords = ["LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        idx = q_upper.find(keyword, order_idx)
        if idx != -1:
            end_idx = idx
            break
    
    order_clause = query[order_idx:end_idx].strip()
    
    # Check for DESC/ASC
    if "DESC" in order_clause.upper():
        return order_clause
    elif "ASC" in order_clause.upper():
        return order_clause
    else:
        # Default to ASC
        return f"{order_clause} ASC"

def _get_column_from_group_by(query: str) -> str:
    q_upper = query.upper()
    group_idx = q_upper.find("GROUP BY") + 8
    
    # Find next keyword
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

def _parse_from_clause(query: str) -> QueryTree:
    from_tables = _get_from_table(query)
    q_upper = from_tables.upper()
    
    # Case 1: NATURAL JOIN
    if "NATURAL JOIN" in q_upper:
        join_split = re.split(r'\s+NATURAL\s+JOIN\s+', from_tables, flags=re.IGNORECASE)
        
        # Parse first table (may have alias)
        left_table = _parse_table_with_alias(join_split[0].strip())
        
        # Chain NATURAL JOIN nodes
        for right_table_str in join_split[1:]:
            right_table = _parse_table_with_alias(right_table_str.strip())
            
            join_node = QueryTree(type="JOIN", val="NATURAL")
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # Case 2: Regular JOIN with ON
    elif "JOIN" in q_upper and "ON" in q_upper:
        join_split = re.split(r'\s+JOIN\s+', from_tables, flags=re.IGNORECASE)
        
        # Parse first table (may have alias)
        left_table = _parse_table_with_alias(join_split[0].strip())
        
        # Process each JOIN
        for join_part in join_split[1:]:
            temp = re.split(r'\s+ON\s+', join_part, flags=re.IGNORECASE)
            right_table_str = temp[0].strip()
            join_condition = temp[1].strip() if len(temp) > 1 else ""
            
            # Parse right table (may have alias)
            right_table = _parse_table_with_alias(right_table_str)
            
            # Clean condition
            join_condition = join_condition.replace("(", "").replace(")", "")
            
            # Create JOIN node with THETA format
            join_node = QueryTree(type="JOIN", val=f"THETA:{join_condition}")
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # Case 3: Comma-separated tables (Cartesian product)
    elif "," in from_tables:
        tables = [t.strip() for t in from_tables.split(",")]
        
        # Parse first table (may have alias)
        left_table = _parse_table_with_alias(tables[0])
        
        # Chain CARTESIAN JOIN nodes
        for table_str in tables[1:]:
            right_table = _parse_table_with_alias(table_str)
            
            join_node = QueryTree(type="JOIN", val="CARTESIAN")
            join_node.add_child(left_table)
            join_node.add_child(right_table)
            left_table = join_node
        
        return left_table
    
    # Case 4: Single table (may have alias)
    else:
        return _parse_table_with_alias(from_tables.strip())

def _parse_table_with_alias (table_str: str) -> QueryTree:
    # Check for AS keyword
    if " AS " in table_str.upper():
        parts = re.split(r'\s+AS\s+', table_str, flags=re.IGNORECASE)
        table_name = parts[0].strip()
        alias = parts[1].strip()
        # Store as "table_name AS alias"
        return QueryTree(type="TABLE", val=f"{table_name} AS {alias}")
    else:
        # No alias, just table name
        return QueryTree(type="TABLE", val=table_str)

## UPDATE Helpers
def _extract_set_conditions(query: str) -> list:
    q_upper = query.upper()
    set_idx = q_upper.find("SET") + 3
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        set_part = query[set_idx:].strip()
    else:
        set_part = query[set_idx:where_idx].strip()
    
    # Split by comma
    conditions = [c.strip() for c in set_part.split(",")]
    return conditions

def _extract_table_update(query: str) -> str:
    q_upper = query.upper()
    update_idx = q_upper.find("UPDATE") + 6
    set_idx = q_upper.find("SET")
    
    return query[update_idx:set_idx].strip()

## DELETE Helpers
def _extract_table_delete(query: str) -> str:
    q_upper = query.upper()
    from_idx = q_upper.find("FROM") + 4
    where_idx = q_upper.find("WHERE")
    
    if where_idx == -1:
        return query[from_idx:].strip()
    else:
        return query[from_idx:where_idx].strip()

## INSERT Helpers
def _extract_table_insert(query: str) -> str:
    q_upper = query.upper()
    into_idx = q_upper.find("INTO") + 4
    
    # Find opening parenthesis for columns
    paren_idx = query.find("(", into_idx)
    
    return query[into_idx:paren_idx].strip()

def _extract_columns_insert(query: str) -> str:
    # Find first parenthesis (columns)
    start_idx = query.find("(")
    end_idx = query.find(")", start_idx)
    
    columns = query[start_idx:end_idx+1]  # Include parentheses
    return columns

def _extract_values_insert(query: str) -> str:
    q_upper = query.upper()
    values_idx = q_upper.find("VALUES")
    
    if values_idx == -1:
        raise Exception("INSERT query must contain VALUES clause")
    
    # Find parenthesis after VALUES
    start_idx = query.find("(", values_idx)
    end_idx = query.find(")", start_idx)
    
    values = query[start_idx:end_idx+1]  # Include parentheses
    return values

def _get_column_from_order_by(query: str) -> str:
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
    
    # Default ASC jika tidak disebutkan
    if "DESC" in order_clause.upper():
        return order_clause  # e.g., "salary DESC"
    elif "ASC" in order_clause.upper():
        return order_clause  # e.g., "salary ASC"
    else:
        return f"{order_clause} ASC"  # default

def _get_order_by_info(query: str) -> str:
    q_upper = query.upper()
    order_idx = q_upper.find("ORDER BY") + 8
    
    # Find next keyword
    end_keywords = ["LIMIT"]
    end_idx = len(query)
    
    for keyword in end_keywords:
        idx = q_upper.find(keyword, order_idx)
        if idx != -1:
            end_idx = idx
            break
    
    order_clause = query[order_idx:end_idx].strip()
    
    # Check for DESC/ASC
    if "DESC" in order_clause.upper():
        return order_clause
    elif "ASC" in order_clause.upper():
        return order_clause
    else:
        # Default to ASC
        return f"{order_clause} ASC"
    
def _parse_drop_table(query: str) -> str:
    q_upper = query.upper()
    
    drop_idx = q_upper.find("DROP TABLE") + 10
    table_part = query[drop_idx:].strip().rstrip(';').strip()
    
    # Check for CASCADE or RESTRICT
    mode = "RESTRICT"  # default
    
    if "CASCADE" in table_part.upper():
        mode = "CASCADE"
        table_name = table_part[:table_part.upper().find("CASCADE")].strip()
    elif "RESTRICT" in table_part.upper():
        mode = "RESTRICT"
        table_name = table_part[:table_part.upper().find("RESTRICT")].strip()
    else:
        table_name = table_part
    
    return f"{table_name}|{mode}"

def _parse_create_table(query: str) -> str:
    q_upper = query.upper()
    
    # Extract table name
    create_idx = q_upper.find("CREATE TABLE") + 12
    paren_idx = query.find("(", create_idx)
    table_name = query[create_idx:paren_idx].strip()
    
    # Extract content inside parentheses
    content_start = paren_idx + 1
    content_end = query.rfind(")")
    content = query[content_start:content_end].strip()
    
    # Split by comma (careful with nested parens)
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
    
    # Parse each part
    columns = []
    primary_key = []
    foreign_keys = []
    
    for part in parts:
        part_upper = part.upper()
        
        if part_upper.startswith("PRIMARY KEY"):
            # Extract column(s): PRIMARY KEY(col1, col2)
            pk_content = part[part.find("(")+1:part.find(")")].strip()
            primary_key = [c.strip() for c in pk_content.split(",")]
        
        elif part_upper.startswith("FOREIGN KEY"):
            # FOREIGN KEY(col) REFERENCES table(ref_col)
            fk_match = re.match(
                r'FOREIGN\s+KEY\s*\((\w+)\)\s+REFERENCES\s+(\w+)\s*\((\w+)\)',
                part,
                re.IGNORECASE
            )
            if fk_match:
                foreign_keys.append(
                    f"{fk_match.group(1)}:{fk_match.group(2)}:{fk_match.group(3)}"
                )
        
        else:
            # Regular column: col_name type [size]
            tokens = part.split()
            if len(tokens) >= 2:
                col_name = tokens[0]
                col_type = tokens[1].lower()
                
                # Extract size: CHAR(10) or VARCHAR(255)
                size = ""
                if "(" in col_type:
                    type_match = re.match(r'(\w+)\((\d+)\)', col_type)
                    if type_match:
                        col_type = type_match.group(1)
                        size = type_match.group(2)
                
                columns.append(f"{col_name}:{col_type}:{size}")
    
    # Build result
    columns_str = ",".join(columns)
    pks_str = ",".join(primary_key)
    fks_str = ",".join(foreign_keys)
    
    return f"{table_name}|{columns_str}|{pks_str}|{fks_str}"

def parse_where_condition(where_str):
    if not where_str or not where_str.strip():
        return None
    
    # Step 1: Split by OR (lowest precedence)
    or_parts = split_by_keyword(where_str, ' OR ')
    
    if len(or_parts) > 1:
        # Ada OR, recursive parse each part
        if len(or_parts) == 2:
            return {
                'type': 'logical',
                'operator': 'OR',
                'left': parse_where_condition(or_parts[0]),
                'right': parse_where_condition(or_parts[1])
            }
        else:
            # Multiple OR: chain them
            return {
                'type': 'logical',
                'operator': 'OR',
                'left': parse_where_condition(or_parts[0]),
                'right': parse_where_condition(' OR '.join(or_parts[1:]))
            }
    
    # Step 2: Split by AND
    and_parts = split_by_keyword(where_str, ' AND ')
    
    if len(and_parts) > 1:
        # Ada AND
        return {
            'type': 'and_chain',
            'conditions': [parse_comparison(part) for part in and_parts]
        }
    
    # Step 3: Single comparison
    return parse_comparison(where_str)


def split_by_keyword(text, keyword):
    parts = []
    current = ""
    i = 0
    
    while i < len(text):
        # Check if keyword matches (case insensitive)
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


def parse_comparison(condition_str):
    condition_str = condition_str.strip()
    
    # Operators to check (order matters - check multi-char operators first)
    operators = ['<>', '>=', '<=', '!=', '=', '>', '<']
    
    for op in operators:
        if op in condition_str:
            parts = condition_str.split(op, 1)
            if len(parts) == 2:
                left_str = parts[0].strip()
                right_str = parts[1].strip()
                
                # Parse left (column reference)
                left = parse_column_reference(left_str)
                
                # Parse right (could be column, string, or number)
                right = parse_value_or_column(right_str)
                
                return {
                    'type': 'comparison',
                    'left': left,
                    'operator': op,
                    'right': right
                }
    
    raise Exception(f"Cannot parse condition: {condition_str}")


def parse_column_reference(col_str):
    col_str = col_str.strip()
    
    if '.' in col_str:
        parts = col_str.split('.', 1)
        return {
            'column': parts[1].strip(),
            'table': parts[0].strip()
        }
    else:
        return {
            'column': col_str,
            'table': None
        }


def parse_value_or_column(value_str):
    value_str = value_str.strip()
    
    # Check if it's a string literal
    if (value_str.startswith("'") and value_str.endswith("'")) or \
       (value_str.startswith('"') and value_str.endswith('"')):
        # String value - remove quotes
        return value_str[1:-1]
    
    # Check if it's a column reference (contains dot but no spaces)
    if '.' in value_str and ' ' not in value_str:
        return parse_column_reference(value_str)
    
    # Try to parse as number
    try:
        # Try integer first
        if '.' not in value_str:
            return int(value_str)
        else:
            return float(value_str)
    except ValueError:
        # Not a number, might be a column name or expression
        # For expressions like "1.05 * salary", return as string
        return value_str


def parse_columns_from_string(columns_str):
    if columns_str.strip() == "*":
        return "*"
    
    columns = []
    parts = columns_str.split(',')
    
    for part in parts:
        part = part.strip()
        if part:
            col_ref = parse_column_reference(part)
            columns.append(col_ref)
    
    return columns


def parse_order_by_string(order_str):
    if not order_str or not order_str.strip():
        return []
    
    result = []
    parts = order_str.split(',')
    
    for part in parts:
        part = part.strip()
        
        # Check for ASC/DESC
        if part.upper().endswith(' DESC'):
            col_str = part[:-5].strip()
            direction = 'DESC'
        elif part.upper().endswith(' ASC'):
            col_str = part[:-4].strip()
            direction = 'ASC'
        else:
            col_str = part
            direction = 'ASC'  # default
        
        col_ref = parse_column_reference(col_str)
        result.append((col_ref, direction))
    
    return result


def parse_group_by_string(group_str):
    if not group_str or not group_str.strip():
        return []
    
    result = []
    parts = group_str.split(',')
    
    for part in parts:
        part = part.strip()
        if part:
            col_ref = parse_column_reference(part)
            result.append(col_ref)
    
    return result


def parse_table_with_alias(table_str):
    table_str = table_str.strip()
    
    # Check for AS keyword (case insensitive)
    if ' AS ' in table_str.upper():
        # Find AS keyword position
        upper_str = table_str.upper()
        as_pos = upper_str.find(' AS ')
        
        table_name = table_str[:as_pos].strip()
        alias = table_str[as_pos + 4:].strip()
        
        return (table_name, alias)
    
    # No alias
    return (table_str, None)


def parse_set_clauses_string(set_str):
    if not set_str or not set_str.strip():
        return {}
    
    result = {}
    
    # Split by comma (simple split, tidak handle kurung)
    parts = set_str.split(',')
    
    for part in parts:
        part = part.strip()
        if '=' in part:
            # Split by first '=' only
            eq_pos = part.find('=')
            column = part[:eq_pos].strip()
            value = part[eq_pos + 1:].strip()
            result[column] = value
    
    return result


def parse_insert_columns_string(columns_str):
    if not columns_str or not columns_str.strip():
        return []
    
    return [col.strip() for col in columns_str.split(',')]


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
                # Add the string value (without quotes)
                result.append(current)
                current = ""
                quote_char = None
            else:
                current += char
        elif char == ',' and not in_string:
            if current.strip():
                # Not a string, try to parse as number
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
    
    # Handle last value
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