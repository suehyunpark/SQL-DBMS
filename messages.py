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


class DeleteResult(SuccessLog):
    def __init__(self, num_deleted):
        self.num_deleted = num_deleted
        super().__init__(f"'{self.num_deleted}' row(s) are deleted")
        
        
class DeleteReferentialIntegrityPassed(SuccessLog):  # NOTE: optional
    def __init__(self, num_deleted):
        self.num_deleted = num_deleted
        super().__init__(f"'{self.num_deleted}' row(s) are not deleted due to referential integrity")
        

# ---------------------------------------------------------------------------- #
#                       Failure messages in DBMS                               #
# ---------------------------------------------------------------------------- #

class SyntaxError(Exception):
    """Raised when the syntax doesn't match the grammar defined in lark."""
    def __init__(self):
        super().__init__("Syntax error")
        
        
class NoSuchTable(Exception):
    """Raised when the table does not exist."""
    def __init__(self):
        super().__init__("No such table")
    
    
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
        
        
class DropReferencedTableError(Exception):
    """Raised when the table is referenced by other table and cannot be dropped."""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Drop table has failed: '{self.table_name}' is referenced by other table")


class InsertTypeMismatchError(Exception):
    """Raised when the type of the value does not match the type of the column."""
    def __init__(self):
        super().__init__("Insertion has failed: Types are not matched")
        

class InsertColumnExistenceError(Exception):
    """Raised when the column does not exist in the table."""
    def __init__(self, column_name):
        self.column_name = column_name
        super().__init__(f"Insertion has failed: '{self.column_name}' does not exist")
        
        
class InsertColumnNonNullableError(Exception):
    """Raised when the column is non nullable and the value is null."""
    def __init__(self, column_name):
        self.column_name = column_name
        super().__init__(f"Insertion has failed: '{self.column_name}' is not nullable")  
        
        
class InsertDuplicatePrimaryKeyError(Exception):  # NOTE: optional
    """Raised when the primary key value already exists in the table."""
    def __init__(self):
        super().__init__("Insertion has failed: Primary key duplication")
        
        
class InsertReferentialIntegrityError(Exception):  # NOTE: optional
    """Raised when the foreign key constraint is violated."""
    def __init__(self):
        super().__init__("Insertion has failed: Referential integrity violation")
        
        
class SelectTableExistenceError(Exception):
    """Raised when the table for selection does not exist."""
    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Selection has failed: '{self.table_name}' does not exist")
        
        
class SelectColumnResolveError(Exception):
    """Raised when the column does not exist in the table."""
    def __init__(self, column_name):
        self.column_name = column_name
        super().__init__(f"Selection has failed: fail to resolve '{self.column_name}'")
        
        
class WhereIncomparableError(Exception):
    """Raised when the operands in the where condition are incomparable."""
    def __init__(self):
        super().__init__("Where clause trying to compare incomparable values")
        
        
class WhereTableNotSpecified(Exception):
    def __init__(self):
        super().__init__("Where clause trying to reference tables which are not specified")
        
        
class WhereColumnNotExist(Exception):
    def __init__(self):
        super().__init__(f"Where clause trying to reference non existing column")
        
        
class WhereAmbiguousReference(Exception):
    def __init__(self):
        super().__init__(f"Where clause contains ambiguous reference")