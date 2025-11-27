from typing import Optional
from model.query_tree import QueryTree

class ParsedQuery:
    def __init__(self, query: str, query_tree: Optional[QueryTree] = None):
        self.query = query
        self.query_tree = query_tree