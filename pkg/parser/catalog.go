// Package parser provides SQL parsing and column-level lineage tracking.
// This file contains the DuckDB function catalog for LSP completions.
package parser

// FunctionCategory classifies SQL functions by their purpose.
type FunctionCategory string

// FunctionCategory constants for SQL function classification.
const (
	CategoryAggregate   FunctionCategory = "aggregate"
	CategoryWindow      FunctionCategory = "window"
	CategoryNumeric     FunctionCategory = "numeric"
	CategoryString      FunctionCategory = "string"
	CategoryDate        FunctionCategory = "date"
	CategoryTimestamp   FunctionCategory = "timestamp"
	CategoryConversion  FunctionCategory = "conversion"
	CategoryConditional FunctionCategory = "conditional"
	CategoryList        FunctionCategory = "list"
	CategoryStruct      FunctionCategory = "struct"
	CategoryMap         FunctionCategory = "map"
	CategoryUtility     FunctionCategory = "utility"
)

// FunctionInfo describes a SQL function for IDE features.
type FunctionInfo struct {
	Name        string           // Function name (e.g., "COUNT")
	Signature   string           // Full signature (e.g., "COUNT(expr) -> integer")
	Description string           // Brief description
	Category    FunctionCategory // Function category
	IsAggregate bool             // True if this is an aggregate function
	Snippet     string           // Optional snippet for insertion (e.g., "COUNT($1)")
}

// DuckDBCatalog contains all known DuckDB functions for LSP completions.
var DuckDBCatalog = []FunctionInfo{
	// ==================== AGGREGATE FUNCTIONS ====================
	{Name: "COUNT", Signature: "COUNT(expr) -> bigint", Description: "Count non-null values", Category: CategoryAggregate, IsAggregate: true, Snippet: "COUNT($1)"},
	{Name: "SUM", Signature: "SUM(expr) -> numeric", Description: "Sum of all values", Category: CategoryAggregate, IsAggregate: true, Snippet: "SUM($1)"},
	{Name: "AVG", Signature: "AVG(expr) -> double", Description: "Average of all values", Category: CategoryAggregate, IsAggregate: true, Snippet: "AVG($1)"},
	{Name: "MIN", Signature: "MIN(expr) -> same", Description: "Minimum value", Category: CategoryAggregate, IsAggregate: true, Snippet: "MIN($1)"},
	{Name: "MAX", Signature: "MAX(expr) -> same", Description: "Maximum value", Category: CategoryAggregate, IsAggregate: true, Snippet: "MAX($1)"},
	{Name: "FIRST", Signature: "FIRST(expr) -> same", Description: "First value (null or non-null)", Category: CategoryAggregate, IsAggregate: true, Snippet: "FIRST($1)"},
	{Name: "LAST", Signature: "LAST(expr) -> same", Description: "Last value", Category: CategoryAggregate, IsAggregate: true, Snippet: "LAST($1)"},
	{Name: "ANY_VALUE", Signature: "ANY_VALUE(expr) -> same", Description: "First non-null value", Category: CategoryAggregate, IsAggregate: true, Snippet: "ANY_VALUE($1)"},
	{Name: "LIST", Signature: "LIST(expr) -> list", Description: "Collect values into a list", Category: CategoryAggregate, IsAggregate: true, Snippet: "LIST($1)"},
	{Name: "ARRAY_AGG", Signature: "ARRAY_AGG(expr) -> list", Description: "Collect values into an array (alias for LIST)", Category: CategoryAggregate, IsAggregate: true, Snippet: "ARRAY_AGG($1)"},
	{Name: "STRING_AGG", Signature: "STRING_AGG(expr, sep) -> varchar", Description: "Concatenate strings with separator", Category: CategoryAggregate, IsAggregate: true, Snippet: "STRING_AGG($1, $2)"},
	{Name: "GROUP_CONCAT", Signature: "GROUP_CONCAT(expr, sep) -> varchar", Description: "Concatenate strings (alias for STRING_AGG)", Category: CategoryAggregate, IsAggregate: true, Snippet: "GROUP_CONCAT($1, $2)"},
	{Name: "LISTAGG", Signature: "LISTAGG(expr, sep) -> varchar", Description: "Concatenate strings (alias for STRING_AGG)", Category: CategoryAggregate, IsAggregate: true, Snippet: "LISTAGG($1, $2)"},
	{Name: "BIT_AND", Signature: "BIT_AND(expr) -> integer", Description: "Bitwise AND of all values", Category: CategoryAggregate, IsAggregate: true, Snippet: "BIT_AND($1)"},
	{Name: "BIT_OR", Signature: "BIT_OR(expr) -> integer", Description: "Bitwise OR of all values", Category: CategoryAggregate, IsAggregate: true, Snippet: "BIT_OR($1)"},
	{Name: "BIT_XOR", Signature: "BIT_XOR(expr) -> integer", Description: "Bitwise XOR of all values", Category: CategoryAggregate, IsAggregate: true, Snippet: "BIT_XOR($1)"},
	{Name: "BOOL_AND", Signature: "BOOL_AND(expr) -> boolean", Description: "True if all values are true", Category: CategoryAggregate, IsAggregate: true, Snippet: "BOOL_AND($1)"},
	{Name: "BOOL_OR", Signature: "BOOL_OR(expr) -> boolean", Description: "True if any value is true", Category: CategoryAggregate, IsAggregate: true, Snippet: "BOOL_OR($1)"},
	{Name: "PRODUCT", Signature: "PRODUCT(expr) -> numeric", Description: "Product of all values", Category: CategoryAggregate, IsAggregate: true, Snippet: "PRODUCT($1)"},
	{Name: "HISTOGRAM", Signature: "HISTOGRAM(expr) -> map", Description: "Returns bucket counts as a map", Category: CategoryAggregate, IsAggregate: true, Snippet: "HISTOGRAM($1)"},

	// Statistical aggregates
	{Name: "STDDEV", Signature: "STDDEV(expr) -> double", Description: "Sample standard deviation", Category: CategoryAggregate, IsAggregate: true, Snippet: "STDDEV($1)"},
	{Name: "STDDEV_POP", Signature: "STDDEV_POP(expr) -> double", Description: "Population standard deviation", Category: CategoryAggregate, IsAggregate: true, Snippet: "STDDEV_POP($1)"},
	{Name: "STDDEV_SAMP", Signature: "STDDEV_SAMP(expr) -> double", Description: "Sample standard deviation", Category: CategoryAggregate, IsAggregate: true, Snippet: "STDDEV_SAMP($1)"},
	{Name: "VARIANCE", Signature: "VARIANCE(expr) -> double", Description: "Sample variance", Category: CategoryAggregate, IsAggregate: true, Snippet: "VARIANCE($1)"},
	{Name: "VAR_POP", Signature: "VAR_POP(expr) -> double", Description: "Population variance", Category: CategoryAggregate, IsAggregate: true, Snippet: "VAR_POP($1)"},
	{Name: "VAR_SAMP", Signature: "VAR_SAMP(expr) -> double", Description: "Sample variance", Category: CategoryAggregate, IsAggregate: true, Snippet: "VAR_SAMP($1)"},
	{Name: "CORR", Signature: "CORR(y, x) -> double", Description: "Correlation coefficient", Category: CategoryAggregate, IsAggregate: true, Snippet: "CORR($1, $2)"},
	{Name: "COVAR_POP", Signature: "COVAR_POP(y, x) -> double", Description: "Population covariance", Category: CategoryAggregate, IsAggregate: true, Snippet: "COVAR_POP($1, $2)"},
	{Name: "COVAR_SAMP", Signature: "COVAR_SAMP(y, x) -> double", Description: "Sample covariance", Category: CategoryAggregate, IsAggregate: true, Snippet: "COVAR_SAMP($1, $2)"},
	{Name: "ENTROPY", Signature: "ENTROPY(expr) -> double", Description: "Log-2 entropy", Category: CategoryAggregate, IsAggregate: true, Snippet: "ENTROPY($1)"},
	{Name: "KURTOSIS", Signature: "KURTOSIS(expr) -> double", Description: "Excess kurtosis with bias correction", Category: CategoryAggregate, IsAggregate: true, Snippet: "KURTOSIS($1)"},
	{Name: "SKEWNESS", Signature: "SKEWNESS(expr) -> double", Description: "Skewness", Category: CategoryAggregate, IsAggregate: true, Snippet: "SKEWNESS($1)"},
	{Name: "MEDIAN", Signature: "MEDIAN(expr) -> same", Description: "Middle value of the set", Category: CategoryAggregate, IsAggregate: true, Snippet: "MEDIAN($1)"},
	{Name: "MODE", Signature: "MODE(expr) -> same", Description: "Most frequent value", Category: CategoryAggregate, IsAggregate: true, Snippet: "MODE($1)"},
	{Name: "QUANTILE_CONT", Signature: "QUANTILE_CONT(expr, pos) -> double", Description: "Interpolated quantile", Category: CategoryAggregate, IsAggregate: true, Snippet: "QUANTILE_CONT($1, $2)"},
	{Name: "QUANTILE_DISC", Signature: "QUANTILE_DISC(expr, pos) -> same", Description: "Discrete quantile", Category: CategoryAggregate, IsAggregate: true, Snippet: "QUANTILE_DISC($1, $2)"},
	{Name: "APPROX_COUNT_DISTINCT", Signature: "APPROX_COUNT_DISTINCT(expr) -> bigint", Description: "Approximate distinct count using HyperLogLog", Category: CategoryAggregate, IsAggregate: true, Snippet: "APPROX_COUNT_DISTINCT($1)"},
	{Name: "APPROX_QUANTILE", Signature: "APPROX_QUANTILE(expr, pos) -> same", Description: "Approximate quantile using T-Digest", Category: CategoryAggregate, IsAggregate: true, Snippet: "APPROX_QUANTILE($1, $2)"},
	{Name: "ARG_MAX", Signature: "ARG_MAX(arg, val) -> same", Description: "Value of arg at maximum val", Category: CategoryAggregate, IsAggregate: true, Snippet: "ARG_MAX($1, $2)"},
	{Name: "ARG_MIN", Signature: "ARG_MIN(arg, val) -> same", Description: "Value of arg at minimum val", Category: CategoryAggregate, IsAggregate: true, Snippet: "ARG_MIN($1, $2)"},
	{Name: "REGR_SLOPE", Signature: "REGR_SLOPE(y, x) -> double", Description: "Slope of linear regression line", Category: CategoryAggregate, IsAggregate: true, Snippet: "REGR_SLOPE($1, $2)"},
	{Name: "REGR_INTERCEPT", Signature: "REGR_INTERCEPT(y, x) -> double", Description: "Intercept of linear regression line", Category: CategoryAggregate, IsAggregate: true, Snippet: "REGR_INTERCEPT($1, $2)"},
	{Name: "REGR_R2", Signature: "REGR_R2(y, x) -> double", Description: "R-squared of linear regression", Category: CategoryAggregate, IsAggregate: true, Snippet: "REGR_R2($1, $2)"},

	// ==================== WINDOW FUNCTIONS ====================
	{Name: "ROW_NUMBER", Signature: "ROW_NUMBER() OVER(...) -> bigint", Description: "Sequential row number", Category: CategoryWindow, Snippet: "ROW_NUMBER() OVER($1)"},
	{Name: "RANK", Signature: "RANK() OVER(...) -> bigint", Description: "Rank with gaps", Category: CategoryWindow, Snippet: "RANK() OVER($1)"},
	{Name: "DENSE_RANK", Signature: "DENSE_RANK() OVER(...) -> bigint", Description: "Rank without gaps", Category: CategoryWindow, Snippet: "DENSE_RANK() OVER($1)"},
	{Name: "NTILE", Signature: "NTILE(n) OVER(...) -> bigint", Description: "Divide into n buckets", Category: CategoryWindow, Snippet: "NTILE($1) OVER($2)"},
	{Name: "PERCENT_RANK", Signature: "PERCENT_RANK() OVER(...) -> double", Description: "Relative rank (0-1)", Category: CategoryWindow, Snippet: "PERCENT_RANK() OVER($1)"},
	{Name: "CUME_DIST", Signature: "CUME_DIST() OVER(...) -> double", Description: "Cumulative distribution", Category: CategoryWindow, Snippet: "CUME_DIST() OVER($1)"},
	{Name: "LAG", Signature: "LAG(expr, offset, default) OVER(...) -> same", Description: "Value from previous row", Category: CategoryWindow, Snippet: "LAG($1, $2) OVER($3)"},
	{Name: "LEAD", Signature: "LEAD(expr, offset, default) OVER(...) -> same", Description: "Value from following row", Category: CategoryWindow, Snippet: "LEAD($1, $2) OVER($3)"},
	{Name: "FIRST_VALUE", Signature: "FIRST_VALUE(expr) OVER(...) -> same", Description: "First value in window", Category: CategoryWindow, Snippet: "FIRST_VALUE($1) OVER($2)"},
	{Name: "LAST_VALUE", Signature: "LAST_VALUE(expr) OVER(...) -> same", Description: "Last value in window", Category: CategoryWindow, Snippet: "LAST_VALUE($1) OVER($2)"},
	{Name: "NTH_VALUE", Signature: "NTH_VALUE(expr, n) OVER(...) -> same", Description: "Nth value in window", Category: CategoryWindow, Snippet: "NTH_VALUE($1, $2) OVER($3)"},

	// ==================== NUMERIC FUNCTIONS ====================
	{Name: "ABS", Signature: "ABS(x) -> same", Description: "Absolute value", Category: CategoryNumeric, Snippet: "ABS($1)"},
	{Name: "CEIL", Signature: "CEIL(x) -> same", Description: "Round up to nearest integer", Category: CategoryNumeric, Snippet: "CEIL($1)"},
	{Name: "CEILING", Signature: "CEILING(x) -> same", Description: "Round up to nearest integer", Category: CategoryNumeric, Snippet: "CEILING($1)"},
	{Name: "FLOOR", Signature: "FLOOR(x) -> same", Description: "Round down to nearest integer", Category: CategoryNumeric, Snippet: "FLOOR($1)"},
	{Name: "ROUND", Signature: "ROUND(x, s) -> numeric", Description: "Round to s decimal places", Category: CategoryNumeric, Snippet: "ROUND($1, $2)"},
	{Name: "TRUNC", Signature: "TRUNC(x) -> same", Description: "Truncate to integer", Category: CategoryNumeric, Snippet: "TRUNC($1)"},
	{Name: "SIGN", Signature: "SIGN(x) -> integer", Description: "Sign of x (-1, 0, or 1)", Category: CategoryNumeric, Snippet: "SIGN($1)"},
	{Name: "SQRT", Signature: "SQRT(x) -> double", Description: "Square root", Category: CategoryNumeric, Snippet: "SQRT($1)"},
	{Name: "CBRT", Signature: "CBRT(x) -> double", Description: "Cube root", Category: CategoryNumeric, Snippet: "CBRT($1)"},
	{Name: "POW", Signature: "POW(x, y) -> double", Description: "x raised to power y", Category: CategoryNumeric, Snippet: "POW($1, $2)"},
	{Name: "POWER", Signature: "POWER(x, y) -> double", Description: "x raised to power y", Category: CategoryNumeric, Snippet: "POWER($1, $2)"},
	{Name: "EXP", Signature: "EXP(x) -> double", Description: "e raised to power x", Category: CategoryNumeric, Snippet: "EXP($1)"},
	{Name: "LN", Signature: "LN(x) -> double", Description: "Natural logarithm", Category: CategoryNumeric, Snippet: "LN($1)"},
	{Name: "LOG", Signature: "LOG(x) -> double", Description: "Base-10 logarithm", Category: CategoryNumeric, Snippet: "LOG($1)"},
	{Name: "LOG10", Signature: "LOG10(x) -> double", Description: "Base-10 logarithm", Category: CategoryNumeric, Snippet: "LOG10($1)"},
	{Name: "LOG2", Signature: "LOG2(x) -> double", Description: "Base-2 logarithm", Category: CategoryNumeric, Snippet: "LOG2($1)"},
	{Name: "SIN", Signature: "SIN(x) -> double", Description: "Sine", Category: CategoryNumeric, Snippet: "SIN($1)"},
	{Name: "COS", Signature: "COS(x) -> double", Description: "Cosine", Category: CategoryNumeric, Snippet: "COS($1)"},
	{Name: "TAN", Signature: "TAN(x) -> double", Description: "Tangent", Category: CategoryNumeric, Snippet: "TAN($1)"},
	{Name: "ASIN", Signature: "ASIN(x) -> double", Description: "Inverse sine", Category: CategoryNumeric, Snippet: "ASIN($1)"},
	{Name: "ACOS", Signature: "ACOS(x) -> double", Description: "Inverse cosine", Category: CategoryNumeric, Snippet: "ACOS($1)"},
	{Name: "ATAN", Signature: "ATAN(x) -> double", Description: "Inverse tangent", Category: CategoryNumeric, Snippet: "ATAN($1)"},
	{Name: "ATAN2", Signature: "ATAN2(y, x) -> double", Description: "Two-argument inverse tangent", Category: CategoryNumeric, Snippet: "ATAN2($1, $2)"},
	{Name: "PI", Signature: "PI() -> double", Description: "Value of pi", Category: CategoryNumeric, Snippet: "PI()"},
	{Name: "RANDOM", Signature: "RANDOM() -> double", Description: "Random number between 0 and 1", Category: CategoryNumeric, Snippet: "RANDOM()"},
	{Name: "SETSEED", Signature: "SETSEED(x) -> void", Description: "Set random seed", Category: CategoryNumeric, Snippet: "SETSEED($1)"},
	{Name: "DEGREES", Signature: "DEGREES(x) -> double", Description: "Convert radians to degrees", Category: CategoryNumeric, Snippet: "DEGREES($1)"},
	{Name: "RADIANS", Signature: "RADIANS(x) -> double", Description: "Convert degrees to radians", Category: CategoryNumeric, Snippet: "RADIANS($1)"},
	{Name: "GCD", Signature: "GCD(x, y) -> integer", Description: "Greatest common divisor", Category: CategoryNumeric, Snippet: "GCD($1, $2)"},
	{Name: "LCM", Signature: "LCM(x, y) -> integer", Description: "Least common multiple", Category: CategoryNumeric, Snippet: "LCM($1, $2)"},
	{Name: "FACTORIAL", Signature: "FACTORIAL(x) -> bigint", Description: "Factorial", Category: CategoryNumeric, Snippet: "FACTORIAL($1)"},
	{Name: "GREATEST", Signature: "GREATEST(x1, x2, ...) -> same", Description: "Largest value", Category: CategoryNumeric, Snippet: "GREATEST($1, $2)"},
	{Name: "LEAST", Signature: "LEAST(x1, x2, ...) -> same", Description: "Smallest value", Category: CategoryNumeric, Snippet: "LEAST($1, $2)"},
	{Name: "ISNAN", Signature: "ISNAN(x) -> boolean", Description: "Check if NaN", Category: CategoryNumeric, Snippet: "ISNAN($1)"},
	{Name: "ISINF", Signature: "ISINF(x) -> boolean", Description: "Check if infinite", Category: CategoryNumeric, Snippet: "ISINF($1)"},
	{Name: "ISFINITE", Signature: "ISFINITE(x) -> boolean", Description: "Check if finite", Category: CategoryNumeric, Snippet: "ISFINITE($1)"},

	// ==================== STRING FUNCTIONS ====================
	{Name: "LENGTH", Signature: "LENGTH(str) -> integer", Description: "Number of characters", Category: CategoryString, Snippet: "LENGTH($1)"},
	{Name: "CHAR_LENGTH", Signature: "CHAR_LENGTH(str) -> integer", Description: "Number of characters", Category: CategoryString, Snippet: "CHAR_LENGTH($1)"},
	{Name: "STRLEN", Signature: "STRLEN(str) -> integer", Description: "Number of bytes", Category: CategoryString, Snippet: "STRLEN($1)"},
	{Name: "UPPER", Signature: "UPPER(str) -> varchar", Description: "Convert to uppercase", Category: CategoryString, Snippet: "UPPER($1)"},
	{Name: "LOWER", Signature: "LOWER(str) -> varchar", Description: "Convert to lowercase", Category: CategoryString, Snippet: "LOWER($1)"},
	{Name: "TRIM", Signature: "TRIM(str) -> varchar", Description: "Remove leading/trailing whitespace", Category: CategoryString, Snippet: "TRIM($1)"},
	{Name: "LTRIM", Signature: "LTRIM(str) -> varchar", Description: "Remove leading whitespace", Category: CategoryString, Snippet: "LTRIM($1)"},
	{Name: "RTRIM", Signature: "RTRIM(str) -> varchar", Description: "Remove trailing whitespace", Category: CategoryString, Snippet: "RTRIM($1)"},
	{Name: "LPAD", Signature: "LPAD(str, len, pad) -> varchar", Description: "Pad on left to length", Category: CategoryString, Snippet: "LPAD($1, $2, $3)"},
	{Name: "RPAD", Signature: "RPAD(str, len, pad) -> varchar", Description: "Pad on right to length", Category: CategoryString, Snippet: "RPAD($1, $2, $3)"},
	{Name: "LEFT", Signature: "LEFT(str, n) -> varchar", Description: "First n characters", Category: CategoryString, Snippet: "LEFT($1, $2)"},
	{Name: "RIGHT", Signature: "RIGHT(str, n) -> varchar", Description: "Last n characters", Category: CategoryString, Snippet: "RIGHT($1, $2)"},
	{Name: "SUBSTRING", Signature: "SUBSTRING(str, start, len) -> varchar", Description: "Extract substring", Category: CategoryString, Snippet: "SUBSTRING($1, $2, $3)"},
	{Name: "SUBSTR", Signature: "SUBSTR(str, start, len) -> varchar", Description: "Extract substring", Category: CategoryString, Snippet: "SUBSTR($1, $2, $3)"},
	{Name: "CONCAT", Signature: "CONCAT(str1, str2, ...) -> varchar", Description: "Concatenate strings", Category: CategoryString, Snippet: "CONCAT($1, $2)"},
	{Name: "CONCAT_WS", Signature: "CONCAT_WS(sep, str1, ...) -> varchar", Description: "Concatenate with separator", Category: CategoryString, Snippet: "CONCAT_WS($1, $2, $3)"},
	{Name: "REPLACE", Signature: "REPLACE(str, from, to) -> varchar", Description: "Replace occurrences", Category: CategoryString, Snippet: "REPLACE($1, $2, $3)"},
	{Name: "REVERSE", Signature: "REVERSE(str) -> varchar", Description: "Reverse string", Category: CategoryString, Snippet: "REVERSE($1)"},
	{Name: "REPEAT", Signature: "REPEAT(str, n) -> varchar", Description: "Repeat string n times", Category: CategoryString, Snippet: "REPEAT($1, $2)"},
	{Name: "SPLIT_PART", Signature: "SPLIT_PART(str, sep, idx) -> varchar", Description: "Split and get part", Category: CategoryString, Snippet: "SPLIT_PART($1, $2, $3)"},
	{Name: "STRING_SPLIT", Signature: "STRING_SPLIT(str, sep) -> list", Description: "Split into list", Category: CategoryString, Snippet: "STRING_SPLIT($1, $2)"},
	{Name: "POSITION", Signature: "POSITION(substr IN str) -> integer", Description: "Find position of substring", Category: CategoryString, Snippet: "POSITION($1 IN $2)"},
	{Name: "INSTR", Signature: "INSTR(str, substr) -> integer", Description: "Find position of substring", Category: CategoryString, Snippet: "INSTR($1, $2)"},
	{Name: "STRPOS", Signature: "STRPOS(str, substr) -> integer", Description: "Find position of substring", Category: CategoryString, Snippet: "STRPOS($1, $2)"},
	{Name: "CONTAINS", Signature: "CONTAINS(str, substr) -> boolean", Description: "Check if contains substring", Category: CategoryString, Snippet: "CONTAINS($1, $2)"},
	{Name: "STARTS_WITH", Signature: "STARTS_WITH(str, prefix) -> boolean", Description: "Check if starts with prefix", Category: CategoryString, Snippet: "STARTS_WITH($1, $2)"},
	{Name: "ENDS_WITH", Signature: "ENDS_WITH(str, suffix) -> boolean", Description: "Check if ends with suffix (alias: SUFFIX)", Category: CategoryString, Snippet: "ENDS_WITH($1, $2)"},
	{Name: "PREFIX", Signature: "PREFIX(str, prefix) -> boolean", Description: "Check if starts with prefix", Category: CategoryString, Snippet: "PREFIX($1, $2)"},
	{Name: "SUFFIX", Signature: "SUFFIX(str, suffix) -> boolean", Description: "Check if ends with suffix", Category: CategoryString, Snippet: "SUFFIX($1, $2)"},
	{Name: "ASCII", Signature: "ASCII(str) -> integer", Description: "ASCII code of first character", Category: CategoryString, Snippet: "ASCII($1)"},
	{Name: "CHR", Signature: "CHR(code) -> varchar", Description: "Character from ASCII code", Category: CategoryString, Snippet: "CHR($1)"},
	{Name: "UNICODE", Signature: "UNICODE(str) -> integer", Description: "Unicode code point of first char", Category: CategoryString, Snippet: "UNICODE($1)"},
	{Name: "TRANSLATE", Signature: "TRANSLATE(str, from, to) -> varchar", Description: "Replace characters", Category: CategoryString, Snippet: "TRANSLATE($1, $2, $3)"},
	{Name: "FORMAT", Signature: "FORMAT(fmt, ...) -> varchar", Description: "Format string (fmt syntax)", Category: CategoryString, Snippet: "FORMAT($1, $2)"},
	{Name: "PRINTF", Signature: "PRINTF(fmt, ...) -> varchar", Description: "Format string (printf syntax)", Category: CategoryString, Snippet: "PRINTF($1, $2)"},
	{Name: "MD5", Signature: "MD5(str) -> varchar", Description: "MD5 hash", Category: CategoryString, Snippet: "MD5($1)"},
	{Name: "SHA1", Signature: "SHA1(str) -> varchar", Description: "SHA-1 hash", Category: CategoryString, Snippet: "SHA1($1)"},
	{Name: "SHA256", Signature: "SHA256(str) -> varchar", Description: "SHA-256 hash", Category: CategoryString, Snippet: "SHA256($1)"},
	{Name: "HASH", Signature: "HASH(value) -> ubigint", Description: "Hash value (non-cryptographic)", Category: CategoryString, Snippet: "HASH($1)"},
	{Name: "REGEXP_MATCHES", Signature: "REGEXP_MATCHES(str, pattern) -> boolean", Description: "Check if matches regex", Category: CategoryString, Snippet: "REGEXP_MATCHES($1, $2)"},
	{Name: "REGEXP_REPLACE", Signature: "REGEXP_REPLACE(str, pattern, repl) -> varchar", Description: "Replace regex matches", Category: CategoryString, Snippet: "REGEXP_REPLACE($1, $2, $3)"},
	{Name: "REGEXP_EXTRACT", Signature: "REGEXP_EXTRACT(str, pattern, group) -> varchar", Description: "Extract regex match", Category: CategoryString, Snippet: "REGEXP_EXTRACT($1, $2, $3)"},
	{Name: "REGEXP_EXTRACT_ALL", Signature: "REGEXP_EXTRACT_ALL(str, pattern) -> list", Description: "Extract all regex matches", Category: CategoryString, Snippet: "REGEXP_EXTRACT_ALL($1, $2)"},
	{Name: "LEVENSHTEIN", Signature: "LEVENSHTEIN(s1, s2) -> integer", Description: "Edit distance between strings", Category: CategoryString, Snippet: "LEVENSHTEIN($1, $2)"},
	{Name: "JACCARD", Signature: "JACCARD(s1, s2) -> double", Description: "Jaccard similarity", Category: CategoryString, Snippet: "JACCARD($1, $2)"},
	{Name: "JARO_WINKLER_SIMILARITY", Signature: "JARO_WINKLER_SIMILARITY(s1, s2) -> double", Description: "Jaro-Winkler similarity", Category: CategoryString, Snippet: "JARO_WINKLER_SIMILARITY($1, $2)"},

	// ==================== DATE/TIME FUNCTIONS ====================
	{Name: "CURRENT_DATE", Signature: "CURRENT_DATE -> date", Description: "Current date", Category: CategoryDate, Snippet: "CURRENT_DATE"},
	{Name: "CURRENT_TIME", Signature: "CURRENT_TIME -> time", Description: "Current time", Category: CategoryDate, Snippet: "CURRENT_TIME"},
	{Name: "CURRENT_TIMESTAMP", Signature: "CURRENT_TIMESTAMP -> timestamp", Description: "Current timestamp", Category: CategoryTimestamp, Snippet: "CURRENT_TIMESTAMP"},
	{Name: "NOW", Signature: "NOW() -> timestamp", Description: "Current timestamp", Category: CategoryTimestamp, Snippet: "NOW()"},
	{Name: "TODAY", Signature: "TODAY() -> date", Description: "Current date", Category: CategoryDate, Snippet: "TODAY()"},
	{Name: "DATE_TRUNC", Signature: "DATE_TRUNC(part, date) -> date", Description: "Truncate to specified precision", Category: CategoryDate, Snippet: "DATE_TRUNC('$1', $2)"},
	{Name: "DATE_PART", Signature: "DATE_PART(part, date) -> integer", Description: "Extract date part", Category: CategoryDate, Snippet: "DATE_PART('$1', $2)"},
	{Name: "DATE_DIFF", Signature: "DATE_DIFF(part, start, end) -> integer", Description: "Difference between dates", Category: CategoryDate, Snippet: "DATE_DIFF('$1', $2, $3)"},
	{Name: "DATEDIFF", Signature: "DATEDIFF(part, start, end) -> integer", Description: "Difference between dates", Category: CategoryDate, Snippet: "DATEDIFF('$1', $2, $3)"},
	{Name: "DATE_ADD", Signature: "DATE_ADD(date, interval) -> timestamp", Description: "Add interval to date", Category: CategoryDate, Snippet: "DATE_ADD($1, $2)"},
	{Name: "DATE_SUB", Signature: "DATE_SUB(part, start, end) -> integer", Description: "Signed interval length", Category: CategoryDate, Snippet: "DATE_SUB('$1', $2, $3)"},
	{Name: "EXTRACT", Signature: "EXTRACT(part FROM date) -> integer", Description: "Extract date/time part", Category: CategoryDate, Snippet: "EXTRACT($1 FROM $2)"},
	{Name: "YEAR", Signature: "YEAR(date) -> integer", Description: "Extract year", Category: CategoryDate, Snippet: "YEAR($1)"},
	{Name: "MONTH", Signature: "MONTH(date) -> integer", Description: "Extract month", Category: CategoryDate, Snippet: "MONTH($1)"},
	{Name: "DAY", Signature: "DAY(date) -> integer", Description: "Extract day", Category: CategoryDate, Snippet: "DAY($1)"},
	{Name: "HOUR", Signature: "HOUR(time) -> integer", Description: "Extract hour", Category: CategoryDate, Snippet: "HOUR($1)"},
	{Name: "MINUTE", Signature: "MINUTE(time) -> integer", Description: "Extract minute", Category: CategoryDate, Snippet: "MINUTE($1)"},
	{Name: "SECOND", Signature: "SECOND(time) -> integer", Description: "Extract second", Category: CategoryDate, Snippet: "SECOND($1)"},
	{Name: "DAYOFWEEK", Signature: "DAYOFWEEK(date) -> integer", Description: "Day of week (0-6)", Category: CategoryDate, Snippet: "DAYOFWEEK($1)"},
	{Name: "DAYOFYEAR", Signature: "DAYOFYEAR(date) -> integer", Description: "Day of year (1-366)", Category: CategoryDate, Snippet: "DAYOFYEAR($1)"},
	{Name: "WEEK", Signature: "WEEK(date) -> integer", Description: "Week number", Category: CategoryDate, Snippet: "WEEK($1)"},
	{Name: "QUARTER", Signature: "QUARTER(date) -> integer", Description: "Quarter (1-4)", Category: CategoryDate, Snippet: "QUARTER($1)"},
	{Name: "DAYNAME", Signature: "DAYNAME(date) -> varchar", Description: "Name of weekday", Category: CategoryDate, Snippet: "DAYNAME($1)"},
	{Name: "MONTHNAME", Signature: "MONTHNAME(date) -> varchar", Description: "Name of month", Category: CategoryDate, Snippet: "MONTHNAME($1)"},
	{Name: "LAST_DAY", Signature: "LAST_DAY(date) -> date", Description: "Last day of month", Category: CategoryDate, Snippet: "LAST_DAY($1)"},
	{Name: "MAKE_DATE", Signature: "MAKE_DATE(year, month, day) -> date", Description: "Create date from parts", Category: CategoryDate, Snippet: "MAKE_DATE($1, $2, $3)"},
	{Name: "MAKE_TIME", Signature: "MAKE_TIME(hour, min, sec) -> time", Description: "Create time from parts", Category: CategoryDate, Snippet: "MAKE_TIME($1, $2, $3)"},
	{Name: "MAKE_TIMESTAMP", Signature: "MAKE_TIMESTAMP(y, m, d, h, min, s) -> timestamp", Description: "Create timestamp from parts", Category: CategoryTimestamp, Snippet: "MAKE_TIMESTAMP($1, $2, $3, $4, $5, $6)"},
	{Name: "STRFTIME", Signature: "STRFTIME(date, format) -> varchar", Description: "Format date as string", Category: CategoryDate, Snippet: "STRFTIME($1, '$2')"},
	{Name: "STRPTIME", Signature: "STRPTIME(str, format) -> timestamp", Description: "Parse string to timestamp", Category: CategoryTimestamp, Snippet: "STRPTIME($1, '$2')"},
	{Name: "TIME_BUCKET", Signature: "TIME_BUCKET(width, date) -> date", Description: "Truncate to time bucket", Category: CategoryDate, Snippet: "TIME_BUCKET($1, $2)"},
	{Name: "AGE", Signature: "AGE(end, start) -> interval", Description: "Interval between timestamps", Category: CategoryTimestamp, Snippet: "AGE($1, $2)"},
	{Name: "EPOCH", Signature: "EPOCH(timestamp) -> double", Description: "Seconds since epoch", Category: CategoryTimestamp, Snippet: "EPOCH($1)"},
	{Name: "EPOCH_MS", Signature: "EPOCH_MS(ms) -> timestamp", Description: "Timestamp from milliseconds", Category: CategoryTimestamp, Snippet: "EPOCH_MS($1)"},

	// ==================== CONVERSION FUNCTIONS ====================
	{Name: "CAST", Signature: "CAST(expr AS type) -> type", Description: "Convert to type", Category: CategoryConversion, Snippet: "CAST($1 AS $2)"},
	{Name: "TRY_CAST", Signature: "TRY_CAST(expr AS type) -> type", Description: "Convert to type, NULL on failure", Category: CategoryConversion, Snippet: "TRY_CAST($1 AS $2)"},
	{Name: "TYPEOF", Signature: "TYPEOF(expr) -> varchar", Description: "Get type name", Category: CategoryConversion, Snippet: "TYPEOF($1)"},

	// ==================== CONDITIONAL FUNCTIONS ====================
	{Name: "COALESCE", Signature: "COALESCE(expr1, expr2, ...) -> same", Description: "First non-null value", Category: CategoryConditional, Snippet: "COALESCE($1, $2)"},
	{Name: "NULLIF", Signature: "NULLIF(expr1, expr2) -> same", Description: "NULL if expr1 = expr2", Category: CategoryConditional, Snippet: "NULLIF($1, $2)"},
	{Name: "IFNULL", Signature: "IFNULL(expr, default) -> same", Description: "Default if expr is NULL", Category: CategoryConditional, Snippet: "IFNULL($1, $2)"},
	{Name: "NVL", Signature: "NVL(expr, default) -> same", Description: "Default if expr is NULL (alias for IFNULL)", Category: CategoryConditional, Snippet: "NVL($1, $2)"},
	{Name: "IF", Signature: "IF(cond, then, else) -> same", Description: "Conditional expression", Category: CategoryConditional, Snippet: "IF($1, $2, $3)"},
	{Name: "IIF", Signature: "IIF(cond, then, else) -> same", Description: "Conditional expression", Category: CategoryConditional, Snippet: "IIF($1, $2, $3)"},

	// ==================== LIST FUNCTIONS ====================
	{Name: "LIST_VALUE", Signature: "LIST_VALUE(v1, v2, ...) -> list", Description: "Create a list", Category: CategoryList, Snippet: "LIST_VALUE($1, $2)"},
	{Name: "LIST_ELEMENT", Signature: "LIST_ELEMENT(list, idx) -> element", Description: "Get element at index", Category: CategoryList, Snippet: "LIST_ELEMENT($1, $2)"},
	{Name: "LIST_EXTRACT", Signature: "LIST_EXTRACT(list, idx) -> element", Description: "Get element at index", Category: CategoryList, Snippet: "LIST_EXTRACT($1, $2)"},
	{Name: "LIST_CONCAT", Signature: "LIST_CONCAT(list1, list2) -> list", Description: "Concatenate lists", Category: CategoryList, Snippet: "LIST_CONCAT($1, $2)"},
	{Name: "LIST_CONTAINS", Signature: "LIST_CONTAINS(list, elem) -> boolean", Description: "Check if list contains element", Category: CategoryList, Snippet: "LIST_CONTAINS($1, $2)"},
	{Name: "LIST_POSITION", Signature: "LIST_POSITION(list, elem) -> integer", Description: "Position of element in list", Category: CategoryList, Snippet: "LIST_POSITION($1, $2)"},
	{Name: "LIST_SORT", Signature: "LIST_SORT(list) -> list", Description: "Sort list", Category: CategoryList, Snippet: "LIST_SORT($1)"},
	{Name: "LIST_REVERSE", Signature: "LIST_REVERSE(list) -> list", Description: "Reverse list", Category: CategoryList, Snippet: "LIST_REVERSE($1)"},
	{Name: "LIST_DISTINCT", Signature: "LIST_DISTINCT(list) -> list", Description: "Remove duplicates", Category: CategoryList, Snippet: "LIST_DISTINCT($1)"},
	{Name: "LIST_UNIQUE", Signature: "LIST_UNIQUE(list) -> integer", Description: "Count unique elements", Category: CategoryList, Snippet: "LIST_UNIQUE($1)"},
	{Name: "LIST_FILTER", Signature: "LIST_FILTER(list, lambda) -> list", Description: "Filter list by predicate", Category: CategoryList, Snippet: "LIST_FILTER($1, x -> $2)"},
	{Name: "LIST_TRANSFORM", Signature: "LIST_TRANSFORM(list, lambda) -> list", Description: "Transform list elements", Category: CategoryList, Snippet: "LIST_TRANSFORM($1, x -> $2)"},
	{Name: "LIST_REDUCE", Signature: "LIST_REDUCE(list, lambda) -> element", Description: "Reduce list to single value", Category: CategoryList, Snippet: "LIST_REDUCE($1, (a, b) -> $2)"},
	{Name: "UNNEST", Signature: "UNNEST(list) -> rows", Description: "Expand list to rows", Category: CategoryList, Snippet: "UNNEST($1)"},
	{Name: "LEN", Signature: "LEN(list) -> integer", Description: "List length", Category: CategoryList, Snippet: "LEN($1)"},
	{Name: "ARRAY_LENGTH", Signature: "ARRAY_LENGTH(list) -> integer", Description: "List length", Category: CategoryList, Snippet: "ARRAY_LENGTH($1)"},

	// ==================== STRUCT FUNCTIONS ====================
	{Name: "STRUCT_PACK", Signature: "STRUCT_PACK(k1 := v1, ...) -> struct", Description: "Create struct", Category: CategoryStruct, Snippet: "STRUCT_PACK($1 := $2)"},
	{Name: "STRUCT_EXTRACT", Signature: "STRUCT_EXTRACT(struct, key) -> value", Description: "Extract struct field", Category: CategoryStruct, Snippet: "STRUCT_EXTRACT($1, '$2')"},
	{Name: "ROW", Signature: "ROW(v1, v2, ...) -> struct", Description: "Create struct from values", Category: CategoryStruct, Snippet: "ROW($1, $2)"},

	// ==================== MAP FUNCTIONS ====================
	{Name: "MAP", Signature: "MAP([k1, k2], [v1, v2]) -> map", Description: "Create map from key/value lists", Category: CategoryMap, Snippet: "MAP($1, $2)"},
	{Name: "MAP_KEYS", Signature: "MAP_KEYS(map) -> list", Description: "Get map keys", Category: CategoryMap, Snippet: "MAP_KEYS($1)"},
	{Name: "MAP_VALUES", Signature: "MAP_VALUES(map) -> list", Description: "Get map values", Category: CategoryMap, Snippet: "MAP_VALUES($1)"},
	{Name: "MAP_EXTRACT", Signature: "MAP_EXTRACT(map, key) -> value", Description: "Get value for key", Category: CategoryMap, Snippet: "MAP_EXTRACT($1, $2)"},
	{Name: "ELEMENT_AT", Signature: "ELEMENT_AT(map, key) -> value", Description: "Get value for key", Category: CategoryMap, Snippet: "ELEMENT_AT($1, $2)"},
	{Name: "CARDINALITY", Signature: "CARDINALITY(map) -> integer", Description: "Number of entries", Category: CategoryMap, Snippet: "CARDINALITY($1)"},

	// ==================== UTILITY FUNCTIONS ====================
	{Name: "UUID", Signature: "UUID() -> uuid", Description: "Generate random UUID", Category: CategoryUtility, Snippet: "UUID()"},
	{Name: "GEN_RANDOM_UUID", Signature: "GEN_RANDOM_UUID() -> uuid", Description: "Generate random UUID", Category: CategoryUtility, Snippet: "GEN_RANDOM_UUID()"},
	{Name: "VERSION", Signature: "VERSION() -> varchar", Description: "DuckDB version", Category: CategoryUtility, Snippet: "VERSION()"},
	{Name: "CURRENT_SCHEMA", Signature: "CURRENT_SCHEMA() -> varchar", Description: "Current schema name", Category: CategoryUtility, Snippet: "CURRENT_SCHEMA()"},
	{Name: "CURRENT_DATABASE", Signature: "CURRENT_DATABASE() -> varchar", Description: "Current database name", Category: CategoryUtility, Snippet: "CURRENT_DATABASE()"},
	{Name: "GENERATE_SERIES", Signature: "GENERATE_SERIES(start, stop, step) -> rows", Description: "Generate series of values", Category: CategoryUtility, Snippet: "GENERATE_SERIES($1, $2, $3)"},
	{Name: "RANGE", Signature: "RANGE(start, stop, step) -> rows", Description: "Generate range of values", Category: CategoryUtility, Snippet: "RANGE($1, $2, $3)"},
}

// GetFunctionsByCategory returns all functions in a category.
func GetFunctionsByCategory(category FunctionCategory) []FunctionInfo {
	var result []FunctionInfo
	for _, fn := range DuckDBCatalog {
		if fn.Category == category {
			result = append(result, fn)
		}
	}
	return result
}

// GetAggregateFunctions returns all aggregate functions.
func GetAggregateFunctions() []FunctionInfo {
	var result []FunctionInfo
	for _, fn := range DuckDBCatalog {
		if fn.IsAggregate {
			result = append(result, fn)
		}
	}
	return result
}

// GetWindowFunctions returns all window functions.
func GetWindowFunctions() []FunctionInfo {
	return GetFunctionsByCategory(CategoryWindow)
}

// SearchFunctions returns functions matching a prefix (case-insensitive).
func SearchFunctions(prefix string) []FunctionInfo {
	if prefix == "" {
		return DuckDBCatalog
	}

	var result []FunctionInfo
	upperPrefix := toUpperASCII(prefix)
	for _, fn := range DuckDBCatalog {
		if len(fn.Name) >= len(prefix) && toUpperASCII(fn.Name[:len(prefix)]) == upperPrefix {
			result = append(result, fn)
		}
	}
	return result
}

// toUpperASCII converts ASCII letters to uppercase.
func toUpperASCII(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		result[i] = c
	}
	return string(result)
}
