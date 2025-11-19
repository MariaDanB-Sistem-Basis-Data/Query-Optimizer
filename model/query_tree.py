class QueryTree:
    def __init__(self, type: str, val=None, childs=None, parent=None):
        self.type = type

        # ---- FIX ----
        # Jika val adalah string → strip
        # Jika bukan string → simpan apa adanya (LogicalNode, ConditionNode, dll)
        if isinstance(val, str):
            self.val = val.strip()
        else:
            self.val = val

        self.childs = list(childs) if childs else []
        self.parent = parent

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
