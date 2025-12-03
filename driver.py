from QueryOptimizer import OptimizationEngine
from helper.stats import get_stats
from model.query_tree import QueryTree, ColumnNode, ConditionNode, LogicalNode
from model.parsed_query import ParsedQuery
import time


# Pretty printer for QueryTree with enhanced formatting
def print_query_tree(node, indent=0, is_last=True, prefix=""):
    """Print query tree with tree-like structure."""
    if node is None:
        return
    
    # Tree characters
    connector = "└── " if is_last else "├── "
    
    # Format node value based on type
    val_str = _format_node_value(node)
    
    print(f"{prefix}{connector}{node.type}: {val_str}")
    
    # Print children
    child_prefix = prefix + ("    " if is_last else "│   ")
    for i, child in enumerate(node.childs):
        is_last_child = (i == len(node.childs) - 1)
        print_query_tree(child, indent + 1, is_last_child, child_prefix)


def _format_node_value(node):
    """Format node value for better readability."""
    if node.val is None:
        return "∅"
    
    if node.type == "TABLE":
        if hasattr(node.val, 'name'):
            if hasattr(node.val, 'alias') and node.val.alias:
                return f"{node.val.name} AS {node.val.alias}"
            return node.val.name
        return str(node.val)
    
    elif node.type == "SIGMA":
        return _format_condition(node.val)
    
    elif node.type == "PROJECT":
        if node.val == "*":
            return "*"
        if isinstance(node.val, list):
            cols = []
            for col in node.val:
                if isinstance(col, ColumnNode):
                    cols.append(f"{col.table}.{col.column}" if col.table else col.column)
                else:
                    cols.append(str(col))
            return ", ".join(cols)
        return str(node.val)
    
    elif node.type == "JOIN":
        if hasattr(node.val, 'condition'):
            return f"THETA: {_format_condition(node.val.condition)}"
        elif isinstance(node.val, str):
            if node.val.upper().startswith("THETA:"):
                return node.val
            elif node.val.upper() == "CARTESIAN":
                return "×"
            elif node.val.upper() == "NATURAL":
                return "⋈"
        return str(node.val)
    
    elif node.type == "SORT":
        if isinstance(node.val, list):
            items = []
            for item in node.val:
                if hasattr(item, 'column') and hasattr(item, 'direction'):
                    col = item.column
                    col_str = f"{col.table}.{col.column}" if hasattr(col, 'table') and col.table else str(col)
                    items.append(f"{col_str} {item.direction}")
            return ", ".join(items)
        return str(node.val)
    
    elif node.type == "LIMIT":
        return str(node.val)
    
    elif node.type == "GROUP":
        if isinstance(node.val, list):
            cols = []
            for col in node.val:
                if isinstance(col, ColumnNode):
                    cols.append(f"{col.table}.{col.column}" if col.table else col.column)
            return ", ".join(cols)
        return str(node.val)
    
    return str(node.val)


def _format_condition(cond):
    """Format condition node for display."""
    if isinstance(cond, ConditionNode):
        attr_str = _format_attr(cond.attr)
        val_str = _format_attr(cond.value)
        return f"{attr_str} {cond.op} {val_str}"
    
    elif isinstance(cond, LogicalNode):
        child_strs = [_format_condition(c) for c in cond.childs]
        return f"({f' {cond.operator} '.join(child_strs)})"
    
    return str(cond)


def _format_attr(attr):
    """Format attribute for display."""
    if isinstance(attr, ColumnNode):
        return f"{attr.table}.{attr.column}" if attr.table else attr.column
    elif isinstance(attr, dict):
        table = attr.get('table', '')
        column = attr.get('column', '')
        return f"{table}.{column}" if table else column
    elif isinstance(attr, str):
        return f"'{attr}'" if not attr.replace('.', '').replace('_', '').isalnum() else attr
    return str(attr)


# Statistics display
def print_statistics():
    """Print available table statistics."""
    stats = get_stats()
    print("\n" + "="*70)
    print("AVAILABLE TABLE STATISTICS")
    print("="*70)
    
    for table_name, table_stats in stats.items():
        print(f"\nTable: {table_name}")
        print(f"  Tuples (n_r):     {table_stats.get('n_r', 'N/A')}")
        print(f"  Blocks (b_r):     {table_stats.get('b_r', 'N/A')}")
        print(f"  Record size (l_r): {table_stats.get('l_r', 'N/A')} bytes")
        print(f"  Blocking factor (f_r): {table_stats.get('f_r', 'N/A')}")
        
        if 'v_a_r' in table_stats and table_stats['v_a_r']:
            print(f"  Distinct values (V(A,r)):")
            for attr, v_val in table_stats['v_a_r'].items():
                print(f"    {attr}: {v_val}")


# Test query execution
def run_test_query(query_name, query, optimizer, verbose=True):
    """Run a single test query and return results."""
    print("\n" + "="*70)
    print(f"TEST: {query_name}")
    print("="*70)
    
    if verbose:
        print(f"\nQuery:\n{query.strip()}\n")
    
    try:
        # Parse
        start_parse = time.time()
        parsed = optimizer.parse_query(query)
        parse_time = time.time() - start_parse
        
        if verbose:
            print(">>> PARSED QUERY TREE:")
            print_query_tree(parsed.query_tree)
        
        # Cost before
        start_cost_before = time.time()
        cost_before = optimizer.get_cost(parsed)
        cost_before_time = time.time() - start_cost_before
        
        print(f"\n>>> COST BEFORE OPTIMIZATION: {cost_before:,}")
        
        # Optimize
        start_optimize = time.time()
        optimized = optimizer.optimize_query(parsed)
        optimize_time = time.time() - start_optimize
        
        if verbose:
            print("\n>>> OPTIMIZED QUERY TREE:")
            print_query_tree(optimized.query_tree)
        
        # Cost after
        start_cost_after = time.time()
        cost_after = optimizer.get_cost(optimized)
        cost_after_time = time.time() - start_cost_after
        
        print(f"\n>>> COST AFTER OPTIMIZATION:  {cost_after:,}")
        
        # Analysis
        improvement = cost_before - cost_after
        improvement_pct = (improvement / cost_before * 100) if cost_before > 0 else 0
        
        print("\n>>> OPTIMIZATION RESULTS:")
        print(f"  Cost reduction:    {improvement:,} blocks")
        print(f"  Improvement:       {improvement_pct:.2f}%")
        print(f"  Parse time:        {parse_time*1000:.2f}ms")
        print(f"  Optimization time: {optimize_time*1000:.2f}ms")
        print(f"  Total time:        {(parse_time + optimize_time)*1000:.2f}ms")
        
        # Determine optimization method used
        tables = list(optimizer._extract_join_conditions_from_tree(parsed.query_tree).keys())
        num_tables = len(set([t for pair in tables for t in pair])) if tables else 1
        method_used = "GA + Heuristic" if num_tables >= optimizer.ga_threshold_tables else "Heuristic"
        print(f"  Method used:       {method_used}")
        print(f"  Number of tables:  {num_tables}")
        
        return {
            'name': query_name,
            'cost_before': cost_before,
            'cost_after': cost_after,
            'improvement': improvement,
            'improvement_pct': improvement_pct,
            'parse_time': parse_time,
            'optimize_time': optimize_time,
            'method': method_used,
            'num_tables': num_tables,
            'success': True
        }
        
    except Exception as e:
        print(f"\n>>> ERROR: {str(e)}")
        return {
            'name': query_name,
            'success': False,
            'error': str(e)
        }


# Summary report
def print_summary(results):
    """Print summary of all test results."""
    print("\n" + "="*70)
    print("SUMMARY REPORT")
    print("="*70)
    
    successful = [r for r in results if r.get('success', False)]
    failed = [r for r in results if not r.get('success', False)]
    
    print(f"\nTotal tests:     {len(results)}")
    print(f"Successful:      {len(successful)}")
    print(f"Failed:          {len(failed)}")
    
    if successful:
        print("\n" + "-"*70)
        print(f"{'Test Name':<30} {'Before':<12} {'After':<12} {'Improvement':<15}")
        print("-"*70)
        
        total_before = 0
        total_after = 0
        
        for r in successful:
            total_before += r['cost_before']
            total_after += r['cost_after']
            improvement = f"{r['improvement_pct']:.2f}%"
            print(f"{r['name']:<30} {r['cost_before']:<12,} {r['cost_after']:<12,} {improvement:<15}")
        
        print("-"*70)
        total_improvement = total_before - total_after
        total_improvement_pct = (total_improvement / total_before * 100) if total_before > 0 else 0
        print(f"{'TOTAL':<30} {total_before:<12,} {total_after:<12,} {total_improvement_pct:.2f}%")
        
        # Method usage statistics
        ga_used = sum(1 for r in successful if 'GA' in r.get('method', ''))
        heuristic_only = sum(1 for r in successful if r.get('method', '') == 'Heuristic')
        
        print(f"\n>>> OPTIMIZATION METHODS:")
        print(f"  GA + Heuristic: {ga_used} queries")
        print(f"  Heuristic only: {heuristic_only} queries")
        
        # Performance statistics
        avg_parse = sum(r['parse_time'] for r in successful) / len(successful)
        avg_optimize = sum(r['optimize_time'] for r in successful) / len(successful)
        
        print(f"\n>>> AVERAGE TIMINGS:")
        print(f"  Parse time:        {avg_parse*1000:.2f}ms")
        print(f"  Optimization time: {avg_optimize*1000:.2f}ms")
        print(f"  Total time:        {(avg_parse + avg_optimize)*1000:.2f}ms")
    
    if failed:
        print("\n>>> FAILED TESTS:")
        for r in failed:
            print(f"  - {r['name']}: {r.get('error', 'Unknown error')}")


# MAIN DRIVER
if __name__ == "__main__":
    
    print("="*70)
    print("QUERY OPTIMIZER - COMPREHENSIVE TEST SUITE")
    print("="*70)
    
    # Show available statistics
    print_statistics()
    
    # Initialize optimizer
    optimizer = OptimizationEngine()
    
    print("\n" + "="*70)
    print("OPTIMIZER CONFIGURATION")
    print("="*70)
    print(f"GA Enabled:        {optimizer.use_ga}")
    print(f"GA Population:     {optimizer.ga_population_size}")
    print(f"GA Generations:    {optimizer.ga_generations}")
    print(f"GA Mutation Rate:  {optimizer.ga_mutation_rate}")
    print(f"GA Crossover Rate: {optimizer.ga_crossover_rate}")
    print(f"GA Threshold:      {optimizer.ga_threshold_tables} tables")
    
    # Test queries
    test_queries = [
        (
            "Simple 2-table join",
            """SELECT * 
               FROM movies 
               JOIN reviews ON movies.movie_id = reviews.movie_id 
               WHERE movies.movie_id = 1;"""
        ),
        (
            "3-table join with selection",
            """SELECT * 
               FROM movies 
               JOIN reviews ON movies.movie_id = reviews.movie_id 
               JOIN movie_directors ON movie_directors.movie_id = movies.movie_id 
               WHERE movies.movie_id = 1 AND movies.genre = 'Action';"""
        ),
        (
            "4-table join (GA threshold)",
            """SELECT m.title, r.rating, d.name, a.award_name
               FROM movies m
               JOIN reviews r ON m.movie_id = r.movie_id
               JOIN movie_directors md ON md.movie_id = m.movie_id
               JOIN directors d ON md.director_id = d.director_id
               WHERE m.genre = 'Drama';"""
        ),
        (
            "5-table complex join",
            """SELECT m.title, r.rating, d.name, ac.name, aw.award_name
               FROM movies m
               JOIN reviews r ON m.movie_id = r.movie_id
               JOIN movie_directors md ON md.movie_id = m.movie_id
               JOIN directors d ON md.director_id = d.director_id
               JOIN awards aw ON aw.movie_id = m.movie_id
               WHERE m.genre = 'Drama' AND r.rating > 7;"""
        ),
        (
            "Selection push-down test",
            """SELECT s.name, c.course_name, e.grade
               FROM students s
               JOIN enrollments e ON s.student_id = e.student_id
               JOIN courses c ON e.course_id = c.course_id
               WHERE s.gpa > 3.5 AND e.grade = 'A';"""
        ),
        (
            "Projection with join",
            """SELECT m.title, r.rating
               FROM movies m
               JOIN reviews r ON m.movie_id = r.movie_id
               WHERE m.genre = 'Action';"""
        ),
        (
            "Multiple conditions",
            """SELECT *
               FROM movies m
               JOIN reviews r ON m.movie_id = r.movie_id
               WHERE m.genre = 'Action' AND r.rating > 7 AND m.age_rating = 'PG-13';"""
        ),
    ]
    
    # Run tests
    results = []
    for i, (name, query) in enumerate(test_queries, 1):
        verbose = (i <= 2)  # Show tree for first 2 queries only
        result = run_test_query(name, query, optimizer, verbose=verbose)
        results.append(result)
    
    # Print summary
    print_summary(results)
    
    print("\n" + "="*70)
    print("TEST SUITE COMPLETED")
    print("="*70)