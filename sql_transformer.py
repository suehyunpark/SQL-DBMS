from lark import Transformer


class SQLTransformer(Transformer):
    
    # bottom-up (depth-first)
    def __init__(self):
        super().__init__()
        self.statement = str()
        self.table = {
            "table_name": str(),
            "column_list": list(),  # [(key1, type1), (key2, type2), ...]
            "not_null_key_set": set(),
            "primary_key_list": list(),  # [(key1, key2), ...]
            "foreign_key_dict": dict()  # {referencing_column_name: (referenced_table_name, referenced_column_name))}
        }
        self.record = list()  # values to insert
        self.tables = list()
        self.select_columns = list()  # [(table_name, column_name), ...)] or '*
        self.where = dict()  # [(table_name, column_name, operator, value), ...] up to 4 conditions
        
    # assumes the parse tree transforms only one query at a time
    def command(self, items):
        if items[0] == "exit":
            self.statement = items[0]
        return self.statement, self.table, self.record, self.tables, self.select_columns, self.where
    
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
        self.table["column_list"].append((column_name, data_type))
        not_null_indicators = [keyword.lower() for keyword in items[-2:] if keyword is not None]  # allow uppercase null keyword
        if not_null_indicators == ["not", "null"]:
            self.table["not_null_key_set"].add(column_name)
        return items
        
    def column_name(self, items) -> str:
        return items[0].value.lower()
    
    def data_type(self, items) -> str:
        return ''.join([item.value.lower() for item in items])  # allow uppercase data types
    
    def table_constraint_definition(self, items):
        return items[0]
    
    def primary_key_constraint(self, items):
        self.table["primary_key_list"].append(tuple(items[2]))
        return items
    
    def referential_constraint(self, items):
        self.table["foreign_key_dict"][items[2][0]] = items[4], items[5][0]  # only 1 referencing and referenced column
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
        # type conversion
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.isdigit():
            value = int(value)
        elif value.lower() == "null":
            value = None
        return value
    
    def delete_query(self, items):
        self.statement = items[0].lower()
        self.table = {
            "table_name": items[2]
        }
        self.where = items[3]
        return items
    
    def select_query(self, items):
        self.statement = items[0].lower()
        self.select_columns = items[1]
        self.tables = items[2][0]
        self.where = items[2][1]
        return items
        
    def select_list(self, items):
        return items
    
    def selected_column(self, items):
        return items[0], items[1]  # table_name, column_name
    
    def table_expression(self, items):
        return items
    
    def from_clause(self, items):
        return items[1]  # items[0] == "from"
    
    def table_reference_list(self, items):
        return items
        
    def referred_table(self, items):
        return items[0]  # table_name, no AS
    
    def where_clause(self, items):
        return items[1]  # items[0] == "where"
    
    def boolean_expr(self, items):
        if len(items) == 1:
            return {
                "op": None,
                "boolean_terms": items[0]
            }
        else:
            return {
                "op": items[1].lower(),  # "or"
                "boolean_terms": items[0::2]  # skip "or" in between
            }
    
    def parenthesized_boolean_expr(self, items):
        return items[1]  # items[0] == "(", items[2] == ")"
    
    def boolean_term(self, items):
        if len(items) == 1:
            return {
                "op": None,
                "boolean_factors": items[0]
            }
        else:
            return {
                "op": items[1].lower(),  # "and"
                "boolean_factors": items[0::2]  # skip "and" in between
            }
    
    def boolean_factor(self, items):
        return {
            "op": items[0].lower() if items[0] else items[0],  # "not"
            "boolean_test": items[1]
        }
    
    def boolean_test(self, items):
        return items[0]
    
    def predicate(self, items):
        return items[0]
    
    def comparison_predicate(self, items):
        return {
            "op": items[1],
            "left_operand": items[0],
            "right_operand": items[2]
        }
    
    def comp_operand(self, items):
        if len(items) == 1:
            return (items[0],)  # (comparable_value,)
        elif len(items) == 2:
            return (items[0], items[1])  # (table_name, column_name)
        
    def comp_op(self, items):
        return items[0].value
    
    def comparable_value(self, items):
        value = items[0].value
        # type conversion
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.isdigit():
            value = int(value)
        return value
    
    def null_predicate(self, items):
        null_op, null = items[2]
        return {
            "op": null_op,
            "left_operand": (items[0], items[1]),  # (table_name, column_name)
            "right_operand": null
        }
    
    def null_operation(self, items):
        if items[1]:
            return "is not", None 
        else:
            return "is", None
    
    # not for project 1-2, 1-3
    def update_query(self, items):
        self.statement = items[0].lower()
        return "'UPDATE' requested"