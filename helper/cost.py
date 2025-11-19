# ===============================================================
# cost.py (Final Version)
# COST MODEL: supports ConditionNode + LogicalNode
# ===============================================================

import math
from model.query_tree import QueryTree

# Import helpers
from helper.helper import (
    ConditionNode,
    LogicalNode,
    _stringify_condition_tree,     # used for debugging / printing
)


# ===============================================================
#  Helper: convert ConditionNode / LogicalNode to list of strings
# ===============================================================

def _cond_to_str(cond):
    """Convert ConditionNode → simple 'A op B' string."""
    if isinstance(cond, ConditionNode):
        return f"{cond.attr} {cond.op} {cond.value}"
    return str(cond)


def _flatten_conditions(obj):
    """
    Flatten LogicalNode tree into a list of condition strings.
    Used for AND and OR selectivity.
    """
    out = []

    if isinstance(obj, ConditionNode):
        out.append(_cond_to_str(obj))
        return out

    if isinstance(obj, LogicalNode):
        for c in obj.childs:
            out.extend(_flatten_conditions(c))
        return out

    # fallback
    if isinstance(obj, str):
        out.append(obj)
    else:
        out.append(str(obj))
    return out


# ===============================================================
# Dummy statistic: estimate per-condition selectivity
# ===============================================================

def estimate_selectivity(cond_str, v_a_r):
    """
    cond_str: "A.x > 5"
    v_a_r   : dict { attribute: V(A,r) } (number of distinct values)

    Simple heuristic:
    - equality: 1 / V(A,r)
    - inequality: 1/3
    - LIKE: 1/4
    - IN: 1/4
    - fallback: 1/2
    """
    s = cond_str.upper()

    # detect attribute name
    attr = cond_str.split()[0] if cond_str.split() else None
    distinct = v_a_r.get(attr, 10)

    # equality
    if "=" in s and "!=" not in s and "<>" not in s:
        return 1.0 / max(distinct, 1)

    # inequality
    if ">" in s or "<" in s:
        return 1.0 / 3.0

    if "LIKE" in s:
        return 1.0 / 4.0

    if "IN" in s:
        return 1.0 / 4.0

    # fallback
    return 0.5


def estimate_conjunction_selectivity(cond_list, v_a_r):
    """
    For AND: product of selectivities.
    cond_list: list[str]
    """
    sel = 1.0
    for c in cond_list:
        sel *= estimate_selectivity(c, v_a_r)
    return sel


def estimate_disjunction_selectivity(cond_list, v_a_r):
    """
    For OR: 1 - Π(1 - si)
    """
    prod = 1.0
    for c in cond_list:
        sel = estimate_selectivity(c, v_a_r)
        prod *= (1 - sel)
    return 1.0 - prod


# ===============================================================
# COST PLANNER
# ===============================================================

class CostPlanner:

    def __init__(self, stats: dict):
        """
        stats = {
            "A": {"n_r": ..., "b_r": ..., ...},
            "B": ...
        }
        """
        self.stats = stats

    # -----------------------------------------------------------
    def get_cost(self, root: QueryTree) -> int:
        """
        External API: compute total cost of query tree.
        """
        _, _, cost = self._compute_cost(root)
        return cost

    # -----------------------------------------------------------
    def _compute_cost(self, node: QueryTree):
        """
        Recursively compute:
            (n_r, b_r, cost-so-far)
        Returns triple.
        """

        t = node.type

        # ---------------------------
        # TABLE
        # ---------------------------
        if t == "TABLE":
            info = self.stats.get(node.val, {})
            n_r = info.get("n_r", 1000)
            b_r = info.get("b_r", 100)
            return n_r, b_r, b_r  # cost = scan blocks

        # ---------------------------
        # SIGMA (selection)
        # ---------------------------
        if t == "SIGMA":

            child = node.childs[0]
            n_in, b_in, cost_in = self._compute_cost(child)

            # default distinct counts
            v_in = { }
            # if child is table we use stats, but for joins we reuse previous cardinality
            # this simplified version sets:
            for k, v in self.stats.get(child.val, {}).items() if child.type=="TABLE" else []:
                if k.startswith("v_"):
                    v_in[k[2:]] = v

            # compute selectivity
            cond = node.val

            if isinstance(cond, ConditionNode):
                sel = estimate_selectivity(_cond_to_str(cond), v_in)

            elif isinstance(cond, LogicalNode):
                conds = _flatten_conditions(cond)
                if cond.operator == "AND":
                    sel = estimate_conjunction_selectivity(conds, v_in)
                else:
                    sel = estimate_disjunction_selectivity(conds, v_in)

            else:
                sel = estimate_selectivity(str(cond), v_in)

            n_out = max(1, int(n_in * sel))
            # blocks scale similarly
            b_out = max(1, int(b_in * sel))

            # cost = cost_in + scan child again
            cost = cost_in + b_in

            return n_out, b_out, cost

        # ---------------------------
        # PROJECT (no size change)
        # ---------------------------
        if t == "PROJECT":
            child = node.childs[0]
            return self._compute_cost(child)

        # ---------------------------
        # LIMIT
        # ---------------------------
        if t == "LIMIT":
            child = node.childs[0]
            n_in, b_in, cost_in = self._compute_cost(child)
            try:
                lim = int(node.val)
            except:
                lim = n_in
            n_out = min(n_in, lim)
            # assume block count proportional
            b_out = max(1, int(b_in * n_out / max(n_in,1)))
            return n_out, b_out, cost_in + b_in

        # ---------------------------
        # SORT
        # ---------------------------
        if t == "SORT":
            child = node.childs[0]
            n_in, b_in, cost_in = self._compute_cost(child)
            # external sort cost: O(b log b)
            cost_sort = b_in * int(math.log2(max(b_in, 2)))
            return n_in, b_in, cost_in + cost_sort

        # ---------------------------
        # GROUP
        # ---------------------------
        if t == "GROUP":
            child = node.childs[0]
            n_in, b_in, cost_in = self._compute_cost(child)
            # treat like sort
            cost_grp = b_in * int(math.log2(max(b_in, 2)))
            return n_in, b_in, cost_in + cost_grp

        # ---------------------------
        # OR node: evaluate cost of each branch as separate scan
        # ---------------------------
        if t == "OR":
            # OR(A,B) = union-like: cost = sum(cost children)
            total_cost = 0
            total_n = 0
            total_b = 0

            for c in node.childs:
                n_c, b_c, cost_c = self._compute_cost(c)
                total_cost += cost_c
                total_n += n_c
                total_b += b_c

            return total_n, total_b, total_cost

        # ---------------------------
        # JOIN
        # ---------------------------
        if t == "JOIN":
            L, R = node.childs
            nl, bl, cost_l = self._compute_cost(L)
            nr, br, cost_r = self._compute_cost(R)

            join_val = node.val.upper()

            # ---- NATURAL JOIN ----
            if join_val == "NATURAL":
                # assume join key matches -> output merges
                # assume reduction
                n_out = max(1, min(nl, nr) // 2)
                # blocks sum
                b_out = bl + br
                # cost nested-loop: bl * br
                cost = cost_l + cost_r + bl * br
                return n_out, b_out, cost

            # ---- THETA JOIN ----
            if join_val.startswith("THETA:"):
                # assume 1/3 reduction
                sel = 1/3
                n_out = max(1, int(min(nl, nr) * sel))
                b_out = max(1, int((bl + br) * sel))
                cost = cost_l + cost_r + bl * br
                return n_out, b_out, cost

            # ---- CARTESIAN ----
            # output = nl * nr
            n_out = max(1, nl * nr)
            b_out = max(1, bl + br)
            cost = cost_l + cost_r + bl * br
            return n_out, b_out, cost

        # ---------------------------
        # Others (UPDATE / DELETE / INSERT / CREATE / DROP)
        # treated as O(1) or negligible
        # ---------------------------
        return 1, 1, 1