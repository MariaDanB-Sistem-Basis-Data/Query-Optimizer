class QueryTree:
    def __init__(self, type: str, val: str = "", childs=None, parent=None):
        self.type = type           # seperti "TABLE", "JOIN", "SIGMA", "PROJECT", "SORT", "LIMIT", "SELECT"
        self.val = (val or "").strip()
        self.childs = list(childs) if childs else []
        self.parent = parent

        # for PROJECT node
        self.columns = None  # List[dict] format: [{'column': 'name', 'table': None}, ...]
        
        # for SIGMA node (WHERE)
        self.condition = None  # dict (nested structure untuk AND/OR)
        
        # for TABLE node
        self.table_name = None  # string - nama tabel asli
        self.table_alias = None  # string - alias (kalau pakai AS)
        
        # for JOIN node
        self.join_type = None  # string: "INNER", "LEFT", "RIGHT", "NATURAL", "CROSS"
        self.join_condition = None  # dict (sama kayak condition di SIGMA)
        self.left_table = None   # dict: {'name': 'table', 'alias': 'alias'}
        self.right_table = None  # dict: {'name': 'table', 'alias': 'alias'}
        
        # for SORT node (ORDER BY)
        self.order_by = None  # List[tuple]: [({'column': 'age', 'table': None}, 'DESC'), ...]
        
        # for GROUP node
        self.group_by = None  # List[dict]: [{'column': 'dept', 'table': None}, ...]
        
        # for LIMIT node
        self.limit_value = None  # int
        
        # for UPDATE node
        self.set_clauses = None  # dict: {'column_name': 'value_expression'}
        
        # for INSERT node
        self.insert_table = None     # string
        self.insert_columns = None   # List[string]
        self.insert_values = None    # List[any]
        
        # for CREATE TABLE node
        self.create_table_name = None  # string
        self.columns_definition = None  # List[dict]
        
        # for DROP TABLE node
        self.drop_table_name = None  # string
        self.drop_cascade = False    # bool

    def add_child(self, node: "QueryTree"):
        node.parent = self
        self.childs.append(node)

    def replace_child(self, old: "QueryTree", new: "QueryTree") -> bool:
        for i, c in enumerate(self.childs):
            if c is old:
                self.childs[i] = new
                new.parent = self
                return True
        return False

    def detach(self):
        if self.parent:
            self.parent.childs = [c for c in self.parent.childs if c is not self]
            self.parent = None

    def __repr__(self):
        return f"QT({self.type}, {self.val})"