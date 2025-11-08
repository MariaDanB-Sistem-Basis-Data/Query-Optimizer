# RUN: python -m unittest tests/UnitTesting.py

import unittest
from QueryOptimizer import OptimizationEngine

from model.query_tree import QueryTree
from model.parsed_query import ParsedQuery

class TestOptimizerMilestone1_2(unittest.TestCase):

    def setUp(self):
        self.engine = OptimizationEngine()

    def test_parse_select(self):
        pq = self.engine.parse_query("SELECT * FROM movies")
        self.assertEqual(pq.query_tree.type, "SELECT")

    def test_parse_update(self):
        pq = self.engine.parse_query("UPDATE movies SET title='X'")
        self.assertEqual(pq.query_tree.type, "UPDATE")

    def test_parse_delete(self):
        pq = self.engine.parse_query("DELETE FROM movies WHERE id=1")
        self.assertEqual(pq.query_tree.type, "DELETE")

    def test_parse_insert(self):
        pq = self.engine.parse_query("INSERT INTO movies VALUES (1,'X')")
        self.assertEqual(pq.query_tree.type, "INSERT")

    def test_parse_transaction(self):
        pq = self.engine.parse_query("BEGIN TRANSACTION")
        self.assertEqual(pq.query_tree.type, "BEGIN TRANSACTION")

    def test_optimize_identity(self):
        pq = self.engine.parse_query("SELECT * FROM movies")
        opt = self.engine.optimize_query(pq)
        self.assertEqual(opt.query_tree.type, "SELECT")

    def test_cost_dummy(self):
        pq = self.engine.parse_query("SELECT * FROM movies")
        cost = self.engine.get_cost(pq)
        self.assertEqual(cost, 0)

if __name__ == "__main__":
    unittest.main()
