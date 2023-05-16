import operator
import re


# ------------------------------- Unknown class ------------------------------ #

class Unknown:
    def __repr__(self):
        return "UNKNOWN"

UNKNOWN = Unknown()



# --------------------------------- operators -------------------------------- #

def or_(*args):
    for arg in args:
        if isinstance(arg, Unknown):
            return UNKNOWN if not any(arg is True for arg in args) else True
    return any(args)

def and_(*args):
    for arg in args:
        if isinstance(arg, Unknown):
            return UNKNOWN if any(arg is False for arg in args) else False
    return all(args)

def not_(x):
    if isinstance(x, Unknown):
        return UNKNOWN
    return not x


comparison_op_map = {
    '<': operator.lt,
    '<=': operator.le,
    '>': operator.gt,
    '>=': operator.ge,
    '=': operator.eq,
    '!=': operator.ne,
}

null_op_map = {
    'is': operator.is_,
    'is not': operator.is_not
}


# --------------------------------- data type -------------------------------- #

DATE_PATTERN = r"(\d{4})-(\d{2})-(\d{2})"

def eval_char_max_len(data_type):
    """Return the length of char type."""
    return eval(data_type[5:-1])  # char($num) -> $num
    
def is_valid_type(valid_type, value):
    if value == None:
        return True
    try:
        if valid_type == "int":
            return isinstance(value, int)
        elif valid_type.startswith("char"):
            return isinstance(value, str) and not value.isdigit()  # must check if value is string first to avoid AttributeError
        elif valid_type == "date":
            return re.match(DATE_PATTERN, value)
    except ValueError:
        return False

def is_comparable(a, b):
    def infer_type(value):
        if value == None:
            return None
        try:
            int(value)
            return "int"
        except ValueError:
            if re.match(DATE_PATTERN, value):
                return "date"
            else:
                return "char"
    a_type = infer_type(a)
    b_type = infer_type(b)
    if a_type == None or b_type == None:
        return True
    return a_type == b_type