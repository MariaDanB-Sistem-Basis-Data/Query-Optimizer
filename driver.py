# ===============================================================
# DRIVER PROGRAM FOR QUERY OPTIMIZER
# ===============================================================

from QueryOptimizer import OptimizationEngine
from helper.stats import get_stats
from model.query_tree import QueryTree
from model.parsed_query import ParsedQuery


# ---------------------------------------------------------------
# Pretty printer for QueryTree (simple recursive)
# ---------------------------------------------------------------
def print_query_tree(node, indent=0):
    space = "  " * indent
    print(f"{space}- {node.type} : {node.val}")

    for c in node.childs:
        print_query_tree(c, indent + 1)


# ---------------------------------------------------------------
# MAIN DRIVER
# ---------------------------------------------------------------
if __name__ == "__main__":

    # TEST QUERY
    query = "SELECT * FROM movies JOIN reviews ON movies.movie_id = reviews.movie_id JOIN movie_directors ON movie_directors.movie_id = movies.movie_id WHERE movies.movie_id = 1 AND movies.genre = 'test';"

    print(">>> RAW QUERY:")
    print(query.strip(), "\n")

    optimizer = OptimizationEngine()

    # -----------------------------------------------------------
    # STEP 1 — PARSE
    # -----------------------------------------------------------
    parsed = optimizer.parse_query(query)
    print(">>> PARSED QUERY TREE:")
    print_query_tree(parsed.query_tree)

    # -----------------------------------------------------------
    # STEP 2 — COST BEFORE OPTIMIZATION
    # -----------------------------------------------------------
    cost_before = optimizer.get_cost(parsed)
    print("\n>>> COST BEFORE OPTIMIZATION:", cost_before)

    # -----------------------------------------------------------
    # STEP 3 — OPTIMIZE
    # -----------------------------------------------------------
    optimized = optimizer.optimize_query(parsed)

    print("\n>>> OPTIMIZED QUERY TREE:")
    print_query_tree(optimized.query_tree)

    # -----------------------------------------------------------
    # STEP 4 — COST AFTER OPTIMIZATION
    # -----------------------------------------------------------
    cost_after = optimizer.get_cost(optimized)
    print("\n>>> COST AFTER OPTIMIZATION:", cost_after)

    # -----------------------------------------------------------
    # STEP 5 — COMPARE
    # -----------------------------------------------------------
    improved = cost_after <= cost_before
    print("\n>>> COST IMPROVED?", improved)
