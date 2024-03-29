from lark import Lark

from dbms import DBMS
from messages import *
from sql_transformer import SQLTransformer

PROMPT = "DB_2023-12345> "  # personal information

dbms = DBMS()

def main():
    with open('grammar.lark') as file:
        sql_parser = Lark(file.read(), start="command", lexer="basic")
    
    exit = False
    while not exit:
        query_list = parse_query_sequence(input(PROMPT))
        for query in query_list:
            try:
                sql_transformer = SQLTransformer()
                statement, table, record, tables, select_columns, where = parse_query(sql_parser, sql_transformer, query)
                if statement == 'exit':
                    exit = True  # end program only when exit query is entered
                    break
                if statement == "create table":
                    success = dbms.create_table(table)
                    print(PROMPT + str(success))
                elif statement == "drop table":
                    success = dbms.drop_table(table["table_name"])
                    print(PROMPT + str(success))
                elif statement in ("explain", "describe", "desc"):
                    table = dbms.explain_describe_desc(table["table_name"])
                    print(PROMPT + str(table))
                elif statement == "show tables":
                    output = dbms.show_tables()
                    print(PROMPT + output)
                elif statement == "insert":
                    result = dbms.insert(table, record)
                    print(PROMPT + str(result))
                elif statement == "delete":
                    result, extra = dbms.delete(table["table_name"], where)
                    print(PROMPT + str(result))
                    if extra:
                        print(PROMPT + str(extra))
                elif statement == "select":
                    output = dbms.select(tables, select_columns, where)
                    print(PROMPT + output)
            except (SyntaxError, NoSuchTable, DuplicateColumnDefError, DuplicatePrimaryKeyDefError, 
                    ReferenceTypeError, ReferenceNonPrimaryKeyError, ReferenceColumnExistenceError, ReferenceTableExistenceError, 
                    NonExistingColumnDefError, TableExistenceError, CharLengthError, DropReferencedTableError, 
                    InsertTypeMismatchError, InsertColumnExistenceError, InsertColumnNonNullableError,
                    InsertDuplicatePrimaryKeyError, InsertReferentialIntegrityError,
                    SelectTableExistenceError, SelectColumnResolveError, 
                    WhereIncomparableError, WhereTableNotSpecified, WhereColumnNotExist, WhereAmbiguousReference) as e:
                print(PROMPT + str(e))
                break
            

def parse_query_sequence(input_query_sequence: str):
    """Parses the input query sequence and returns a list of queries."""
    while True:
        input_query_sequence = input_query_sequence.rstrip()  # remove any trailing whitespaces after the semicolon
        if input_query_sequence.endswith(";"):  # end of query sequence
            break
        else:
            input_query_sequence += " " + input()  # waits for any additional input until the semicolon is found
    query_list = input_query_sequence.split(";")
    return [query.strip() + ';' for query in query_list if query.strip()]  # adds semicolon to each query and remove whitespaces


def parse_query(sql_parser: Lark, sql_transformer, query):
    """Parses the query and returns the transformed parse tree."""
    try:
        parsed = sql_parser.parse(query)
    except:
        raise SyntaxError()
    else:
        transformed = sql_transformer.transform(parsed)
        return transformed

                

if __name__ == "__main__":
    main()