# ---------------------------------------------------------------------------- #
#                       Success messages in DBMS                               #
# ---------------------------------------------------------------------------- #

class SuccessLog:
    """Class that contains the messages for a successful operation."""
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message
    
    
class CreateTableSuccess(SuccessLog):
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"'{self.table_name}' table is created")


class DropSuccess(SuccessLog):
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"'{self.table_name}' table is dropped")
      
        
class InsertResult(SuccessLog):
    def __init__(self):
        super().__init__("The row is inserted")



# ---------------------------------------------------------------------------- #
#                       Failure messages in DBMS                               #
# ---------------------------------------------------------------------------- #

class SyntaxError(Exception):
    """Raised when the syntax doesn't match the grammar defined in lark."""
    def __init__(self):
        super().__init__("Syntax error")
    
    

class DuplicateColumnDefError(Exception):
    """Raised when the column definition is duplicated."""
    def __init__(self):
        super().__init__("Create table has failed: column definition is duplicated")
        

class DuplicatePrimaryKeyDefError(Exception):
    """Raised when the primary key definition is duplicated."""
    def __init__(self):
        super().__init__("Create table has failed: primary key definition is duplicated")


class ReferenceTypeError(Exception):
    """Raised when the foreign key references wrong type."""
    def __init__(self):
        super().__init__("Create table has failed: foreign key references wrong type")


class ReferenceNonPrimaryKeyError(Exception):
    """Raised when the foreign key references non primary key column."""
    def __init__(self):
        super().__init__("Create table has failed: foreign key references non primary key column")


class ReferenceColumnExistenceError(Exception):
    """Raised when the foreign key references non existing column."""
    def __init__(self):
        super().__init__("Create table has failed: foreign key references non existing column")


class ReferenceTableExistenceError(Exception):
    """Raised when the foreign key references non existing table."""
    def __init__(self):
        super().__init__("Create table has failed: foreign key references non existing table")


class NonExistingColumnDefError(Exception):
    """Raised when the column definition does not exist in the table definition."""
    def __init__(self, column_name):
        self.column_name = column_name
        super().__init__(f"Create table has failed: '{self.column_name}' does not exist in column definition")


class TableExistenceError(Exception):
    """Raised when the table with the same name already exists."""
    def __init__(self):
        super().__init__("Create table has failed: table with the same name already exists")
        

class CharLengthError(Exception):
    """Raised when the char length is less than 1."""
    def __init__(self):
        super().__init__("Char length should be over 0")
        
        
class NoSuchTable(Exception):
    """Raised when the table does not exist."""
    def __init__(self):
        super().__init__("No such table")
        
        
class DropReferencedTableError(Exception):
    """Raised when the table is referenced by other table and cannot be dropped."""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Drop table has failed: '{self.table_name}' is referenced by other table")
        
        
class SelectTableExistenceError(Exception):
    """Raised when the table for selection does not exist."""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Selection has failed: '{self.table_name}' does not exist")
