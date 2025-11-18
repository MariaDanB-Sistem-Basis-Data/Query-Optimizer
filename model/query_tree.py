# condition node - represents a single comparison (e.g., a.id = 5)
class ConditionNode:
    def __init__(self, attr, op, value):
        self.attr = attr      # dict: {'column': str, 'table': str|None}
        self.op = op          # str: '=', '<>', '>', '>=', '<', '<='
        self.value = value    # bisa: int, float, str, atau dict {'column': str, 'table': str|None}
    
    def __repr__(self):
        return f"Cond({self.attr} {self.op} {self.value})"


# logical node - represents AND/OR combination of conditions
class LogicalNode:
    def __init__(self, operator, childs):
        self.operator = operator  # str: "AND" atau "OR"
        self.childs = childs      # list[ConditionNode|LogicalNode]
    
    def __repr__(self):
        return f"Logic({self.operator}, {self.childs})"


# column node - represents a column reference
class ColumnNode:
    def __init__(self, column, table=None):
        self.column = column  # str
        self.table = table    # str|None
    
    def __repr__(self):
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column


# order by item - represents a single ORDER BY element
class OrderByItem:
    def __init__(self, column_node, direction="ASC"):
        self.column = column_node  # ColumnNode
        self.direction = direction  # str: "ASC" atau "DESC"
    
    def __repr__(self):
        return f"{self.column} {self.direction}"


# set clause - represents a single SET assignment in UPDATE
class SetClause:
    def __init__(self, column, value):
        self.column = column  # str
        self.value = value    # str (expression)
    
    def __repr__(self):
        return f"{self.column} = {self.value}"


# column definition - represents a column in CREATE TABLE
class ColumnDefinition:
    def __init__(self, name, data_type, size=None):
        self.name = name          # str
        self.data_type = data_type  # str: 'int', 'float', 'char', 'varchar'
        self.size = size          # int|None
    
    def __repr__(self):
        if self.size:
            return f"{self.name} {self.data_type}({self.size})"
        return f"{self.name} {self.data_type}"


# foreign key definition
class ForeignKeyDefinition:
    def __init__(self, column, ref_table, ref_column):
        self.column = column        # str
        self.ref_table = ref_table  # str
        self.ref_column = ref_column  # str
    
    def __repr__(self):
        return f"FK({self.column} -> {self.ref_table}.{self.ref_column})"


# insert data - represents INSERT statement data
class InsertData:
    def __init__(self, table, columns, values):
        self.table = table        # str
        self.columns = columns    # list[str]
        self.values = values      # list[any]
    
    def __repr__(self):
        cols = ", ".join(self.columns)
        vals = ", ".join(str(v) if not isinstance(v, str) else f"'{v}'" for v in self.values)
        return f"{self.table}({cols}) <- ({vals})"


# create table data - represents CREATE TABLE statement data
class CreateTableData:
    def __init__(self, table, columns, primary_key, foreign_keys):
        self.table = table              # str
        self.columns = columns          # list[ColumnDefinition]
        self.primary_key = primary_key  # list[str]
        self.foreign_keys = foreign_keys # list[ForeignKeyDefinition]
    
    def __repr__(self):
        cols_str = ", ".join(str(c) for c in self.columns)
        pk_str = ", ".join(self.primary_key) if self.primary_key else ""
        result = f"{self.table} [{cols_str}]"
        if pk_str:
            result += f" PK({pk_str})"
        if self.foreign_keys:
            fk_str = ", ".join(str(fk) for fk in self.foreign_keys)
            result += f" {fk_str}"
        return result


# drop table data - represents DROP TABLE statement data
class DropTableData:
    def __init__(self, table, cascade):
        self.table = table      # str
        self.cascade = cascade  # bool
    
    def __repr__(self):
        mode = "CASCADE" if self.cascade else "RESTRICT"
        return f"{self.table} {mode}"


# table reference - represents a table with optional alias
class TableReference:
    def __init__(self, name, alias=None):
        self.name = name    # str
        self.alias = alias  # str|None
    
    def __repr__(self):
        if self.alias:
            return f"{self.name} AS {self.alias}"
        return self.name


class QueryTree:
    def __init__(self, type: str, val=None, childs=None, parent=None):
        self.type = type
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