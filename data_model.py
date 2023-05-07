import pickle  # handle complex data types and tuples as dict keys
from typing import Dict, Set, Tuple
from pathlib import Path
from uuid import uuid4

from berkeleydb import db

from messages import *


class DataObject:
    def serialize(self):
        return pickle.dumps(self.__dict__)
    
    @classmethod
    def deserialize(cls, pickled_data):
        data = pickle.loads(pickled_data)
        return cls(**data)


class Table(DataObject):
    def __init__(
        self, 
        table_name: str, 
        column_names: Dict[str, str], 
        not_null_keys: Set[str], 
        primary_key: Tuple[str], 
        foreign_keys: Dict[Tuple[str, str], Tuple[str, str]],
        referenced_by: Set[str]=set()
    ):
        self.table_name = table_name
        self.column_names = column_names  # key: column name, value: column referencing_type
        self.not_null_keys = not_null_keys  # set of column names
        self.primary_key = primary_key  # tuple of column names (order is important in this project)
        self.foreign_keys = foreign_keys  # key: referencing column name, value: tuple of (referenced table name, referenced column name)
        self.referenced_by = referenced_by  # set of table names that reference this table
        
    def __str__(self):
        info = "\n-----------------------------------------------------------------\n"
        info += f"table_name [{self.table_name}]\n"
        info += "{:<25}{:<15}{:<10}{:<10}\n".format("column_name", "type", "null", "key")
        for column, column_type in self.column_names.items():
            null_str = 'N' if column in self.not_null_keys else 'Y'
            key_str = ''
            if self.primary_key and column in self.primary_key:
                key_str = 'PRI'
            if self.foreign_keys and self.is_foreign_key(column):
                key_str = 'FOR'
                if self.primary_key and column in self.primary_key:
                    key_str = 'PRI/FOR'
            info += "{:<25}{:<15}{:<10}{:<10}\n".format(column, column_type, null_str, key_str)
        info += "-----------------------------------------------------------------"
        # info += f"\nreferenced by {self.referenced_by}"
        return info
    
    def __contains__(self, key: tuple):
        return key in self.column_names
    
    def is_foreign_key(self, key):
        return any([key in foreign_key for foreign_key in self.foreign_keys.keys()])
    
    def check_reference_primary_key(self, referenced_keys: Tuple[str]):
        if not self.primary_key:
            return False
        return referenced_keys == self.primary_key  # need to compare in case insensitive way
    
    def check_reference_type(self, referencing_types: Tuple[str]):
        if not self.primary_key:
            return False
        primary_key_types = tuple([self.column_names[key] for key in self.primary_key])
        return all([referencing_type == referenced_type for referencing_type, referenced_type in zip(referencing_types, primary_key_types)])
    
    def has_reference(self):
        return self.referenced_by is not None and len(self.referenced_by) > 0
    
    def get_referencing_tables(self):
        if self.foreign_keys is None or len(self.foreign_keys) == 0:
            return None
        return [table for table, column in self.foreign_keys.values()]
    
    def add_reference(self, table_name):
        self.referenced_by.add(table_name)
        
    def remove_reference(self, table_name):
        self.referenced_by.remove(table_name)
    
'''
table = TableSchema(
    table_name="employees",
    column_names={
        "id": "INTEGER",
        "name": "VARCHAR(255)",
        "age": "INTEGER",
        "department": "VARCHAR(255)"
    },
    not_null_keys={"id", "name", "age"},
    primary_key={"id"},
    foreign_keys={
        "department": ("departments", "department")
    }
)
'''
        

class Record(DataObject):
    def __init__(self, table_name, data):
        self.table_name = table_name
        self.data = data
        
        
'''
row = TableRow(
    table_name="employees",
    data={
        "id": 1,
        "name": "John Doe",
        "age": 30,
        "department": "HR"
    }
)
'''

class DB:
    """One database, One table"""
    def __init__(self, db_name: str):
        self.db_dir = Path("./DB")
        self.db_name = db_name
        self.db_file = self.db_dir / (self.db_name + ".db")
        
    def open_db(self):
        self.DB = db.DB()
        if self.db_file.exists():
            self.DB.open(str(self.db_file), dbname=self.db_name, dbtype=db.DB_HASH)
        else:
            self.DB.open(str(self.db_file), dbname=self.db_name, dbtype=db.DB_HASH, flags=db.DB_CREATE)
        
    def close_db(self):
        self.DB.close()
        
    def create_cursor(self):
        return self.DB.cursor()
        
    def discard_cursor(self, cursor):
        cursor.close()
        
    def get_dbname(self):
        return self.DB.get_dbname()
        
    def key_from_str(self, string):
        return string.encode()
    
    def exists(self, key):
        return self.DB.exists(key)
    
    def get(self, key):
        dataobj = self.DB.get(key, default=None)
        if not dataobj:
            return None
        return Record.deserialize(dataobj)

    def put(self, key, dataobj):
        self.DB.put(key, dataobj.serialize())
    
    def delete(self, key):
        self.DB.delete(key)
        
    def keys(self):
        return self.DB.keys()
    
    def values(self):
        return self.DB.values()
    
    def items(self):
        return self.DB.items()
    
    def define_meta(self, meta):
        self.meta = meta
        

class MetaDB(DB):
    """Metadata DB containing table schemas"""
    def __init__(self, db_name="table"):  # identifier
        super().__init__(db_name)
    
    def get(self, key):
        value = self.DB.get(key, default=None)
        if not value:
            return None
        return Table.deserialize(value)
    
    def get_db_file(self, db_name):
        return self.db_dir / (db_name + ".db")
    


class DBMS:
    def __init__(self):
        self.db_dir = Path("./DB")
        self.db_dir.mkdir(exist_ok=True)
        self.meta_db = MetaDB()
        
    def delete_all(self):
        remove_path(self.db_dir)
        
    def create_table(self, table_dict: dict):
        table_name = table_dict["table_name"]
        column_name_list = table_dict["column_name_list"]
        not_null_key_set = table_dict["not_null_key_set"]
        primary_key_list = table_dict["primary_key_list"]
        foreign_key_dict = table_dict["foreign_key_dict"]
        
        # Error within the table info
        if len(set([column_name for column_name, _ in column_name_list])) < len(column_name_list):
            raise DuplicateColumnDefError()
        column_names = {column_name: column_type for column_name, column_type in column_name_list}
        
        for data_type in column_names.values():
            if data_type.startswith("char"):
                if eval_char_len(data_type) < 1:  # hardcoding
                    raise CharLengthError()
        
        if len(primary_key_list) > 1:
            raise DuplicatePrimaryKeyDefError()
        elif len(primary_key_list) == 0:
            primary_key = None
        else:
            primary_key = primary_key_list[0]
            for key in primary_key:
                if key not in column_names:
                    raise NonExistingColumnDefError(key)
            not_null_key_set.update(primary_key)
        
        if foreign_key_dict:
            for foreign_key_tuple in foreign_key_dict.keys():
                for foreign_key in foreign_key_tuple:
                    if foreign_key not in column_names:
                        raise NonExistingColumnDefError(foreign_key)   

        # Error within the database
        self.meta_db.open_db()
        
        table_key = self.meta_db.key_from_str(table_name)
        if self.meta_db.exists(table_key):
            raise TableExistenceError()
        
        if foreign_key_dict:
            for foreign_key, (referenced_table_name, referenced_key) in foreign_key_dict.items():
                referenced_table_key = self.meta_db.key_from_str(referenced_table_name)
                referenced_table = self.meta_db.get(referenced_table_key)
                if not referenced_table:
                    raise ReferenceTableExistenceError()
                for key in referenced_key:
                    if key not in referenced_table:
                        raise ReferenceColumnExistenceError()
                if not referenced_table.check_reference_primary_key(referenced_key):
                    raise ReferenceNonPrimaryKeyError()
                foreign_key_types = tuple([column_names[foreign_key] for foreign_key in foreign_key])
                if not referenced_table.check_reference_type(foreign_key_types):
                    raise ReferenceTypeError()
                referenced_table.add_reference(table_name)
                # update referenced table info
                self.meta_db.put(referenced_table_key, referenced_table)
        
        table = Table(
            table_name=table_name,
            column_names=column_names,
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
        table_key = self.meta_db.key_from_str(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise NoSuchTable()
        if table.has_reference():
            raise DropReferencedTableError(table_name)
        referencing_tables = table.get_referencing_tables()
        if referencing_tables:
            for referencing_table in referencing_tables:
                referencing_table_key = self.meta_db.key_from_str(referencing_table)
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
        table_key = self.meta_db.key_from_str(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise NoSuchTable()
        self.meta_db.close_db()
        return table
    
    
    def show_tables(self):
        self.meta_db.open_db()
        output = "\n------------------------\n"
        for table_key in self.meta_db.keys():
            output += table_key.decode() + "\n"
        output += "------------------------"
        self.meta_db.close_db()
        return output
    
    
    def insert(self, table_name, record_list: list):
        self.meta_db.open_db()
        table_key = self.meta_db.key_from_str(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise NoSuchTable()
        self.meta_db.close_db()
        
        data = {}
        for (column_name, data_type), value in zip(table.column_names.items(), record_list):
            if "char" in data_type:
                max_len = eval_char_len(data_type)
                value = value[:max_len]
            data[column_name] = value
        record = Record(table_name, data)
        record_key = uuid4().bytes  # unique ID
        table_db = DB(table_name)
        table_db.open_db()
        table_db.put(record_key, record)
        table_db.close_db()
        
        return InsertResult()
    
    
    def select(self, table_name):
        def create_separator(column_widths):
            return '+-' + '-+-'.join('-' * width for width in column_widths) + '-+'
        
        self.meta_db.open_db()
        table_key = self.meta_db.key_from_str(table_name)
        table = self.meta_db.get(table_key)
        if not table:
            raise SelectTableExistenceError(table_name)
        self.meta_db.close_db()
        
        table_db = DB(table_name)
        table_db.open_db()
        records = [Record.deserialize(serialized_record).data for serialized_record in table_db.values()]
        table_db.close_db()
        
        headers = table.column_names.keys()
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
                    
        
        
def eval_char_len(data_type):
    """Return the length of char type."""
    return eval(data_type[5:-1])  # char($num) -> $num

def remove_path(path: Path):
    if path.is_file() or path.is_symlink():
        path.unlink()
        return
    for p in path.iterdir():
        remove_path(p)
    path.rmdir()