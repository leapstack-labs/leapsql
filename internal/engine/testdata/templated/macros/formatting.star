# Macro for generating formatted output

def dollars(cents_column):
    """Convert cents to dollars with 2 decimal places."""
    return "ROUND({} / 100.0, 2)".format(cents_column)

def status_label(status_column):
    """Convert status codes to labels."""
    return """CASE {}
        WHEN 'P' THEN 'Pending'
        WHEN 'C' THEN 'Completed'
        WHEN 'X' THEN 'Cancelled'
        ELSE 'Unknown'
    END""".format(status_column)
