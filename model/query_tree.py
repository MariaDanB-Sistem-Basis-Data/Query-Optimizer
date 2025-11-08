class QueryTree:
    def __init__(self, type: str, val: str = "", childs=None, parent=None):
        self.type = type             # misal SELECT, UPDATE, JOIN, TABLE
        self.val = val               
        self.childs = childs or []   
        self.parent = parent

    def add_child(self, node):
        node.parent = self
        self.childs.append(node)

    def __repr__(self):
        return f"QueryTree(type={self.type}, val={self.val})"
