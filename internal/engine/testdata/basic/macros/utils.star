# Utility macros for testing

def safe_divide(numerator, denominator, default="0"):
    """Safely divide two values, returning default if denominator is zero."""
    return "CASE WHEN {} = 0 THEN {} ELSE {} / {} END".format(denominator, default, numerator, denominator)

def cents_to_dollars(column):
    """Convert cents to dollars."""
    return "({} / 100.0)".format(column)
