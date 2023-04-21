from lark import Lark, Transformer

PROMPT = "DB_2018-17119> "

class SQLTransformer(Transformer):
    """Transforms the parse tree into a string representation of the query."""
    # assumes the parse tree transforms only one query at a time
    def command(self, items):
        return items[0]
    
    def query_list(self, items):
        return items[0]
    
    def query(self, items):
        return items[0]
    
    # identifies the type of query and calls the corresponding function (name of node)
    def create_table_query(self, items):
        return "'CREATE TABLE' requested"

    def drop_table_query(self, items):
        return "'DROP TABLE' requested"

    def explain_query(self, items):
        return "'EXPLAIN' requested"

    def describe_query(self, items):
        return "'DESCRIBE' requested"

    def desc_query(self, items):
        return "'DESC' requested"

    def insert_query(self, items):
        return "'INSERT' requested"

    def delete_query(self, items):
        return "'DELETE' requested"

    def select_query(self, items):
        return "'SELECT' requested"

    def show_tables_query(self, items):
        return "'SHOW TABLES' requested"

    def update_query(self, items):
        return "'UPDATE' requested"
    

def parse(sql_parser: Lark, sql_transformer, query):
    """Parses the query and returns the transformed parse tree."""
    try:
        output = sql_parser.parse(query)
    except:
        raise SyntaxError("Syntax error")  # raised when the syntax doesn't match the grammar defined in lark
    else:
        transformed = sql_transformer.transform(output)
        return transformed
            

def process_query_sequence(input_query_sequence: str):
    """Processes the input query sequence and returns a list of queries."""
    while True:
        input_query_sequence = input_query_sequence.rstrip()  # remove any trailing whitespaces after the semicolon
        if input_query_sequence.endswith(";"):  # end of query sequence
            break
        else:
            input_query_sequence += " " + input()  # waits for any additional input until the semicolon is found
    query_list = input_query_sequence.split(";")
    return [query.strip() + ';' for query in query_list if query.strip()]  # adds semicolon to each query and remove whitespaces


def main():
    with open('grammar.lark') as file:
        sql_parser = Lark(file.read(), start="command", lexer="basic")
    sql_transformer = SQLTransformer()
    
    exit = False
    while not exit:
        query_list = process_query_sequence(input(PROMPT))
        for query in query_list:
            try:
                result = parse(sql_parser, sql_transformer, query)
                if result == 'exit':
                    exit = True  # end program only when exit query is entered
                    break
                else:
                    print(PROMPT + result)
            except SyntaxError as e:
                print(PROMPT + str(e))
                break
  
            
if __name__ == "__main__":
    main()