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
        columns: Dict[str, str], 
        not_null_keys: Set[str], 
        primary_key: Tuple[str], 
        foreign_keys: Dict[str, Tuple[str, str]],
        referenced_by: Set[str]=set()
    ):
        self.table_name = table_name
        self.columns = columns  # key: column name, value: column referencing_type
        self.not_null_keys = not_null_keys  # set of column names
        self.primary_key = primary_key  # tuple of column names (order is important in this project)
        self.foreign_keys = foreign_keys  # key: referencing column name, value: tuple of (referenced table name, referenced column name)
        self.referenced_by = referenced_by  # set of table names that reference this table
        
    def __str__(self):
        info = "\n-----------------------------------------------------------------\n"
        info += f"table_name [{self.table_name}]\n"
        info += "{:<25}{:<15}{:<10}{:<10}\n".format("column_name", "type", "null", "key")
        for column, column_type in self.columns.items():
            null_str = 'N' if column in self.not_null_keys else 'Y'
            key_str = ''
            if self.primary_key and column in self.primary_key:
                key_str = 'PRI'
            if self.foreign_keys and column in self.foreign_keys:
                key_str = 'FOR'
                if self.primary_key and column in self.primary_key:
                    key_str = 'PRI/FOR'
            info += "{:<25}{:<15}{:<10}{:<10}\n".format(column, column_type, null_str, key_str)
        info += "-----------------------------------------------------------------"
        return info
    
    def __contains__(self, key: tuple):
        return key in self.columns
    
    def check_reference_primary_key(self, referenced_key: str):
        return referenced_key in self.primary_key
    
    def check_reference_type(self, referencing_type: str, referenced_key: str):
        return self.columns[referenced_key] == referencing_type
    
    # def check_reference_primary_key(self, referenced_keys: Tuple[str]):
    #     if not self.primary_key:
    #         return False
    #     return referenced_keys == self.primary_key  # need to compare in case insensitive way
    
    # def check_reference_type(self, referencing_types: Tuple[str]):
    #     if not self.primary_key:
    #         return False
    #     primary_key_types = tuple([self.columns[key] for key in self.primary_key])
    #     return all([referencing_type == referenced_type for referencing_type, referenced_type in zip(referencing_types, primary_key_types)])
    
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
    primary_key=("id",),
    foreign_keys={
        "department": ("departments", "department")
    }
)
'''
        

class Record(DataObject):
    def __init__(
        self, 
        table_name: str, 
        data: Dict[str, str], 
        primary_value: Tuple[str],
        is_referencing: bool,
        is_referenced: bool=False
    ):
        self.table_name = table_name
        self.data = data
        self.primary_value = primary_value
        self.is_referencing = is_referencing
        self.is_referenced = is_referenced
    
    def add_reference(self):
        self.is_referenced = True
        
        
        
'''
row = TableRow(
    table_name="employees",
    data={
        "id": 1,
        "name": "John Doe",
        "age": 30,
        "department": "HR"
    },
    primary_value=(1,)
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
    
    def create_key_from_value(self, primary_tuple: tuple):  # if has primary key
        return str(primary_tuple).encode()
    
    def create_random_key(self):  # if no primary key
        return uuid4().bytes
    
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
    
    def delete_by_cursor(self, cursor):
        cursor.delete()
        
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

    def create_key_from_value(self, table_name):
        return table_name.encode()