from lark import Transformer


class SQLTransformer(Transformer):
    
    # bottom-up (depth-first)
    def __init__(self):
        super().__init__()
        self.statement = str()
        self.table = {
            "table_name": str(),
            "column_name_list": list(),  # [(key1, type1), (key2, type2), ...]
            "not_null_key_set": set(),
            "primary_key_list": list(),  # [(key1, key2), ...]
            "foreign_key_dict": dict()  # {referencing_column_name: (referenced_table_name, referenced_column_name))}
        }
        self.record = list()
        
    # assumes the parse tree transforms only one query at a time
    def command(self, items):
        if items[0] == "exit":
            self.statement = items[0]
        return self.statement, self.table, self.record
    
    def query_list(self, items):
        return items[0]
    
    def query(self, items):
        return items[0]
    
    # identifies the type of query and calls the corresponding function (name of node)
    def create_table_query(self, items):
        self.statement = f"{items[0].lower()} {items[1].lower()}"
        self.table["table_name"] = items[2]
        return items
        
    def table_name(self, items) -> str:
        return items[0].value.lower()
    
    def table_element_list(self, items):
        return [item for item in items if item != '(' and item != ')']
    
    def table_element(self, items):
        return items[0]
    
    def column_definition(self, items):
        column_name = items[0]
        data_type = items[1]  # int, char($num), date
        self.table["column_name_list"].append((column_name, data_type))
        if items[-2:] == ["not", "null"]:  # TODO: need to consider case insensitivity
            self.table["not_null_key_set"].add(column_name)
        return items
        
    def column_name(self, items) -> str:
        return items[0].value.lower()
    
    def data_type(self, items) -> str:
        return ''.join([item.value for item in items])
    
    def primary_key_constraint(self, items):
        self.table["primary_key_list"].append(tuple(items[2]))
        return items
    
    def referential_constraint(self, items):
        self.table["foreign_key_dict"][tuple(items[2])] = items[4], tuple(items[5])
        return items
    
    def column_name_list(self, items):
        return [item for item in items if item != '(' and item != ')']
    
    def drop_table_query(self, items):
        self.statement = f"{items[0].lower()} {items[1].lower()}"
        self.table = {
            "table_name": items[2]
        }
        return items

    def explain_query(self, items):
        self.statement = items[0].lower()
        self.table = {
            "table_name": items[1]
        }
        return items

    def describe_query(self, items):
        self.statement = items[0].lower()
        self.table = {
            "table_name": items[1]
        }
        return items

    def desc_query(self, items):
        self.statement = items[0].lower()
        self.table = {
            "table_name": items[1]
        }
        return items

    def show_tables_query(self, items):
        self.statement = f"{items[0].lower()} {items[1].lower()}"
        self.table = None
        return items

    def insert_query(self, items):
        self.statement = items[0].lower()
        self.table = {
            "table_name": items[2],
            "column_name_list": items[3],
        }
        self.record = items[5]
        return items
    
    def value_list(self, items):
        return [item for item in items if item != '(' and item != ')']
    
    def value(self, items):
        value = items[0].value
        if value.startswith("'"):
            value = value[1:]
        if value.endswith("'"):
            value = value[:-1]
        return value
    
    def select_query(self, items):
        self.statement = items[0].lower()
    
    def referred_table(self, items):
        self.table = {
            "table_name": items[0]
        }
        return items[0]
    
    
    # not for project 2-1
    def delete_query(self, items):
        self.statement = items[0].lower()
        return "'DELETE' requested"

    def update_query(self, items):
        self.statement = items[0].lower()
        return "'UPDATE' requested"
    
    