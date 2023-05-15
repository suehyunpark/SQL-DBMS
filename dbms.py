from pathlib import Path
from typing import Dict, List, Tuple
import itertools
from collections import Counter
from copy import deepcopy

from db_model import Table, Record, DB, MetaDB
from utils import *
from messages import *



class DBMS:
    def __init__(self):
        self.db_dir = Path("./DB")
        self.db_dir.mkdir(exist_ok=True)
        self.meta_db = MetaDB()
        
        
    def create_table(self, table_dict: dict):
        table_name = table_dict["table_name"]
        column_list = table_dict["column_list"]
        not_null_key_set = table_dict["not_null_key_set"]
        primary_key_list = table_dict["primary_key_list"]
        foreign_key_dict = table_dict["foreign_key_dict"]

        # Error within the table info
        if len(set([column_name for column_name, _ in column_list])) < len(column_list):
            raise DuplicateColumnDefError()
        columns = {column_name: column_type for column_name, column_type in column_list}
        
        for data_type in columns.values():
            if data_type.startswith("char"):
                if eval_char_max_len(data_type) < 1:  # hardcoding
                    raise CharLengthError()
        
        if len(primary_key_list) > 1:
            raise DuplicatePrimaryKeyDefError()
        elif len(primary_key_list) == 0:
            primary_key = None
        else:
            primary_key = primary_key_list[0]
            for key in primary_key:
                if key not in columns:
                    raise NonExistingColumnDefError(key)
            not_null_key_set.update(primary_key)
        
        if foreign_key_dict:
            for foreign_key in foreign_key_dict:
                if foreign_key not in columns:
                    raise NonExistingColumnDefError(foreign_key)   

        # Error within the database
        self.meta_db.open_db()
        
        table_key = self.meta_db.create_key_from_value(table_name)
        if self.meta_db.exists(table_key):
            raise TableExistenceError()
        
        if foreign_key_dict:
            for foreign_key, (referenced_table_name, referenced_key) in foreign_key_dict.items():
                referenced_table_key = self.meta_db.create_key_from_value(referenced_table_name)
                referenced_table = self.meta_db.get(referenced_table_key)
                if not referenced_table:
                    raise ReferenceTableExistenceError()
                if referenced_key not in referenced_table:
                    raise ReferenceColumnExistenceError()
                if not referenced_table.check_reference_primary_key(referenced_key):
                    raise ReferenceNonPrimaryKeyError()
                foreign_key_type = columns[foreign_key]
                if not referenced_table.check_reference_type(foreign_key_type, referenced_key):
                    raise ReferenceTypeError()
                referenced_table.add_reference(table_name)
                # update referenced table info
                self.meta_db.put(referenced_table_key, referenced_table)
        
        table = Table(
            table_name=table_name,
            columns=columns,
            not_null_keys=not_null_key_set,
            primary_key=primary_key,
            foreign_keys=foreign_key_dict
        )
        # add table info to meta db
        self.meta_db.put(table_key, table)
        self.meta_db.close_db()
        
        # create table db
        table_db = DB(table_name)
        table_db.open_db()
        table_db.close_db()
        
        return CreateTableSuccess(table_name)

    
    
    def drop_table(self, table_name: str):
        # remove table info
        self.meta_db.open_db()
        table_key = self.meta_db.create_key_from_value(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise NoSuchTable()
        if table.has_reference():
            raise DropReferencedTableError(table_name)
        referencing_tables = table.get_referencing_tables()
        if referencing_tables:
            for referencing_table in referencing_tables:
                referencing_table_key = self.meta_db.create_key_from_value(referencing_table)
                referencing_table_db = self.meta_db.get(referencing_table_key)
                referencing_table_db.remove_reference(table_name)
                self.meta_db.put(referencing_table_key, referencing_table_db)
        self.meta_db.delete(table_key)
        
        # remove table records
        table_db_file = self.meta_db.get_db_file(table_name)
        table_db_file.unlink()
        self.meta_db.close_db()
        
        return DropSuccess(table_name)
    
    
    def explain_describe_desc(self, table_name: str):
        self.meta_db.open_db()
        table_key = self.meta_db.create_key_from_value(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise NoSuchTable()
        self.meta_db.close_db()
        return table
    
    
    def show_tables(self):
        self.meta_db.open_db()
        output = "\n------------------------\n"
        all_tables = self.meta_db.keys()
        for table_key in all_tables:
            output += table_key.decode() + "\n"
        output += "------------------------"
        self.meta_db.close_db()
        return output
    
    
    def insert(self, table_dict: dict, value_list: list):
        table_name = table_dict["table_name"]
        column_name_list = table_dict["column_name_list"]
        
        self.meta_db.open_db()
        table_key = self.meta_db.create_key_from_value(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise NoSuchTable()
        self.meta_db.close_db()
        
        if column_name_list:
            if len(column_name_list) != len(value_list):
                raise InsertTypeMismatchError()
            for column_name in column_name_list:
                if column_name not in table:
                    raise InsertColumnExistenceError(column_name)
            
        if len(table.columns.keys()) != len(value_list):
            raise InsertTypeMismatchError()
        
        for column_name, value in zip(table.columns.keys(), value_list):
            if value is None and column_name in table.not_null_keys:
                raise InsertColumnNonNullableError(column_name)
        
        if not all([self._validate_type(data_type, value) for data_type, value in zip(table.columns.values(), value_list)]):
            raise InsertTypeMismatchError()
        

        data = {}
        primary_value = []
        is_referencing = False
        for (column_name, data_type), value in zip(table.columns.items(), value_list):
            if data_type.startswith("char"):
                max_len = eval_char_max_len(data_type)
                value = value[:max_len]
            if table.primary_key and column_name in table.primary_key:  # may be composite primary key
                primary_value.append(value)
            if table.foreign_keys and column_name in table.foreign_keys:  # one foreign key per column
                referenced_table_name, _ = table.foreign_keys[column_name]
                # get referenced table schema
                self.meta_db.open_db()
                referenced_table_key = self.meta_db.create_key_from_value(referenced_table_name)
                referenced_table = self.meta_db.get(referenced_table_key)
                self.meta_db.close_db()
                # get referenced record
                referenced_table_db = DB(referenced_table_name)
                referenced_table_db.open_db()
                referenced_key = referenced_table_db.create_key_from_value((value,))
                referenced_record = None
                if len(referenced_table.primary_key) == 1:
                    referenced_record = referenced_table_db.get(referenced_key)
                else:  # composite primary key
                    all_primary_values = referenced_table_db.keys()
                    for primary_value in all_primary_values:
                        if referenced_key.decode() in primary_value.decode():
                            referenced_record = referenced_table_db.get(primary_value)
                            break
                if referenced_record is None:
                    raise InsertReferentialIntegrityError()
                is_referencing = True
                referenced_record.add_reference()  # update reference
                referenced_table_db.put(referenced_key, referenced_record)
                referenced_table_db.close_db()
            data[column_name] = value
        primary_value = tuple(primary_value) if primary_value else None
        record = Record(table_name, data, primary_value, is_referencing)
        
        table_db = DB(table_name)
        table_db.open_db()
        record_key = table_db.create_key_from_value(primary_value) if primary_value else table_db.create_random_key()
        if table_db.exists(record_key):
            raise InsertDuplicatePrimaryKeyError()
        table_db.put(record_key, record)
        table_db.close_db()
        
        return InsertResult()

    
    
    def delete(self, table_name: str, where_clause: str):
        self.meta_db.open_db()
        table_key = self.meta_db.create_key_from_value(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise NoSuchTable()
        self.meta_db.close_db()
        
        table_db = DB(table_name)
        table_db.open_db()
        cursor = table_db.create_cursor()
        
        success_cnt = 0
        fail_cnt = 0
        key_value_pair = cursor.first()
        while key_value_pair:
            key, value = key_value_pair
            record = Record.deserialize(value).data
            satisfies = self._evaluate_condition(where_clause, [table], record) if where_clause else True
            if satisfies == True:  # not UNKNOWN
                if record.is_referenced:
                    fail_cnt += 1
                else:
                    table_db.delete_by_cursor(cursor)
                    success_cnt += 1
            key_value_pair = cursor.next()
            
        table_db.discard_cursor(cursor)
        table_db.close_db()
        
        return DeleteResult(success_cnt), DeleteReferentialIntegrityPassed(fail_cnt) if fail_cnt else None
        
    
    def _evaluate_condition(self, condition, table_list: List[Table], record: dict):
        def get_record_value(operand):
            table_name, column_name = operand
            if table_name and not any([table_name == table.table_name for table in table_list]):
                raise WhereTableNotSpecified()
            found_tables = [table for table in table_list if column_name in table]
            if len(found_tables) < 1:
                raise WhereColumnNotExist()
            elif len(found_tables) > 1:
                if not table_name:  # column name is ambiguous
                    raise WhereAmbiguousReference()
                table = next(table for table in found_tables if table_name == table.table_name)
            else:
                table = found_tables[0]
            if table_name and table_name != table.table_name:
                raise WhereColumnNotExist()
            if table_name:
                prefixed_column_name = f"{table_name}.{column_name}"
                if prefixed_column_name in record:
                    return record[prefixed_column_name]
            return record[column_name]
            
        op = condition["op"]
        if op in comparison_op_map | null_op_map:
            op, left_operand, right_operand = map(condition.get, ["op", "left_operand", "right_operand"])
            if len(left_operand) == 1:  # comparable_value
                left_value = left_operand[0]
            else:  # table_name, column_name
                left_value = get_record_value(left_operand)
            if len(right_operand) == 1:
                right_value = right_operand[0]
            else:
                right_value = get_record_value(right_operand)
            
            if op in comparison_op_map and is_comparable(left_value, right_value) == False:
                raise WhereIncomparableError()
            
            if op in comparison_op_map:
                if left_value is None or right_value is None:
                    output = UNKNOWN
                else:
                    output = comparison_op_map[op](left_value, right_value)
            else:
                output = null_op_map[op](left_value, right_value)
            return output
            
        elif op == "not":
            boolean_test = condition["boolean_test"]
            return not_(self._evaluate_condition(boolean_test, table_list, record))
        
        elif op == "and":
            boolean_factors = condition["boolean_factors"]
            return and_(*[self._evaluate_condition(boolean_factor, table_list, record) for boolean_factor in boolean_factors])
        
        elif op == "or":
            boolean_terms = condition["boolean_terms"]
            return or_(*[self._evaluate_condition(boolean_term, table_list, record) for boolean_term in boolean_terms])
        
        else:  # None
            _, remaining_condition = condition.popitem()  # "boolean_terms", "boolean_factors", "boolean_test"
            if remaining_condition is not None:
                return self._evaluate_condition(remaining_condition, table_list, record)
    
    
    def select(self, tables: list, select_columns: list, where_clause: dict):
        table_list = []
        self.meta_db.open_db()
        for table_name in tables:
            table_key = self.meta_db.create_key_from_value(table_name)
            table = self.meta_db.get(table_key)
            if not table:
                raise SelectTableExistenceError(table_name)
            table_list.append(table)
        self.meta_db.close_db()
        
        if select_columns:
            for table_name, column_name in select_columns:
                found_tables = [table for table in table_list if column_name in table]
                if len(found_tables) != 1 and not table_name:  # column name is ambiguous
                    raise SelectColumnResolveError(column_name)
                found_table = found_tables[0]
                if table_name and table_name != found_table.table_name:
                    raise SelectColumnResolveError(column_name)
        
        all_columns = []
        for table_schema in table_list:
            all_columns.extend(list(table_schema.columns.keys()))
        counter = Counter(all_columns)
        common_columns = set([column for column, count in counter.items() if count > 1])
        
        final_columns = []
        for table_schema in table_list:
            for column in table_schema.columns:
                if column in common_columns:
                    final_columns.append(f"{table_schema.table_name}.{column}")
                else:
                    final_columns.append(column)
                    
        all_records_with_table = {}
        for table_name in tables:
            all_records_with_table[table_name] = []
            table_db = DB(table_name)
            table_db.open_db()
            cursor = table_db.create_cursor()
            key_value_pair = cursor.first()
            while key_value_pair:
                key, value = key_value_pair
                record = Record.deserialize(value)
                record_data = {}
                for column_name, value in record.data.items():
                    if column_name in common_columns:
                        prefixed_column_name = f"{table_name}.{column_name}"
                        record_data[prefixed_column_name] = value
                    else:
                        record_data[column_name] = value
                all_records_with_table[table_name].append(record_data)
                key_value_pair = cursor.next()
            table_db.discard_cursor(cursor)
            table_db.close_db()
        
        cartesian_product = itertools.product(*all_records_with_table.values())
        records_product = [{k: v for record in combination_tuple for k, v in record.items()} for combination_tuple in cartesian_product]
        
        if where_clause:
            filtered_records = []
            for record in records_product:
                satisfies = self._evaluate_condition(deepcopy(where_clause), table_list, record)  # otherwise the original where is modified
                if satisfies == True:
                    filtered_records.append(record)
        else:
            filtered_records = records_product  # list of dict[column_name, value]
            
        if select_columns:  # final output has headers by the specification of select_columns
            final_records = []
            for record in filtered_records:
                final_record = {}
                for table_name, column_name in select_columns:
                    value = None
                    if table_name:
                        prefixed_column_name = f"{table_name}.{column_name}"
                        value = record.get(prefixed_column_name, None)  # table name prefix may not be necessary
                    if value:
                        final_record[prefixed_column_name] = value
                    else:
                        final_record[column_name] = record[column_name]
                final_records.append(final_record)
        else:
            final_records = filtered_records
            
        headers = final_records[0].keys() if final_records else final_columns
        
        return self._format_select_output(final_records, headers)
        
    
    def _format_select_output(self, records: List[Dict], headers: List[str]):
        def create_separator(column_widths):
            return '+-' + '-+-'.join('-' * width for width in column_widths) + '-+'
        
        for record in records:
            for k, v in record.items():
                if v is None:
                    record[k] = "null"
        
        column_widths = [len(header) for header in headers]
        for record in records:
            for i, value in enumerate(record.values()):
                column_widths[i] = max(column_widths[i], len(str(value)))
        
        output = '\n'
        output += create_separator(column_widths) + '\n'
        output += '| ' + ' | '.join(header.upper().ljust(width) for header, width in zip(headers, column_widths)) + ' |\n'
        output += create_separator(column_widths) + '\n'
        
        for record in records:
            output += '| ' + ' | '.join(str(value).ljust(width) for value, width in zip(record.values(), column_widths)) + ' |\n'
        output += create_separator(column_widths)
        
        return output