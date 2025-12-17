# LeapSQL Utility Macros
# 
# This file contains reusable Starlark functions that can be called
# from SQL models using the {{ namespace.function() }} syntax.

def generate_surrogate_key(*columns):
    """
    Generate a surrogate key by hashing multiple columns.
    
    Usage in SQL:
        {{ utils.generate_surrogate_key('col1', 'col2', 'col3') }}
    
    Produces:
        MD5(CAST(col1 AS VARCHAR) || '-' || CAST(col2 AS VARCHAR) || '-' || CAST(col3 AS VARCHAR))
    """
    parts = []
    for col in columns:
        parts.append("CAST({} AS VARCHAR)".format(col))
    return "MD5({})".format(" || '-' || ".join(parts))


def safe_divide(numerator, denominator, default="0"):
    """
    Safely divide two values, returning a default if denominator is zero.
    
    Usage in SQL:
        {{ utils.safe_divide('revenue', 'quantity', '0') }}
    
    Produces:
        CASE WHEN quantity = 0 THEN 0 ELSE revenue / quantity END
    """
    return "CASE WHEN {} = 0 THEN {} ELSE {} / {} END".format(
        denominator, default, numerator, denominator
    )


def current_timestamp():
    """
    Return the current timestamp function for DuckDB.
    
    Usage in SQL:
        {{ utils.current_timestamp() }}
    
    Produces:
        CURRENT_TIMESTAMP
    """
    return "CURRENT_TIMESTAMP"
