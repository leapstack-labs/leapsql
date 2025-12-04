package lineage

import (
	"testing"
)

// Helper to check if a source column exists in a list
func hasSource(sources []SourceColumn, table, column string) bool {
	for _, s := range sources {
		if s.Table == table && s.Column == column {
			return true
		}
	}
	return false
}

// Helper to find a column lineage by name
func findColumn(cols []*ColumnLineage, name string) *ColumnLineage {
	for _, c := range cols {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// Helper to check if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// =============================================================================
// Test: Simple SELECT with direct columns
// =============================================================================

func TestExtractLineage_SimpleSelect(t *testing.T) {
	sql := `SELECT id, name, email FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should have one source table
	if len(lineage.Sources) != 1 || lineage.Sources[0] != "users" {
		t.Errorf("Expected sources [users], got %v", lineage.Sources)
	}

	// Should have 3 output columns
	if len(lineage.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(lineage.Columns))
	}

	// Check each column
	for _, name := range []string{"id", "name", "email"} {
		col := findColumn(lineage.Columns, name)
		if col == nil {
			t.Errorf("Missing column: %s", name)
			continue
		}
		if col.Transform != TransformDirect {
			t.Errorf("Column %s should be direct, got %v", name, col.Transform)
		}
		// Without schema, sources may not be fully resolved
		if len(col.Sources) == 0 {
			t.Logf("Column %s has no sources (expected without schema)", name)
		}
	}
}

func TestExtractLineage_QualifiedColumns(t *testing.T) {
	sql := `SELECT u.id, u.name FROM users u`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should have users as source
	if !contains(lineage.Sources, "users") {
		t.Errorf("Expected users in sources, got %v", lineage.Sources)
	}

	// Check columns have table qualifier tracked
	idCol := findColumn(lineage.Columns, "id")
	if idCol == nil {
		t.Fatal("Missing 'id' column")
	}
	if len(idCol.Sources) > 0 && idCol.Sources[0].Table != "users" {
		t.Errorf("Expected source table 'users', got '%s'", idCol.Sources[0].Table)
	}
}

// =============================================================================
// Test: SELECT with expressions (binary ops, functions)
// =============================================================================

func TestExtractLineage_BinaryExpression(t *testing.T) {
	sql := `SELECT price * quantity AS total FROM order_items`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	col := findColumn(lineage.Columns, "total")
	if col == nil {
		t.Fatal("Missing 'total' column")
	}

	// Binary expression should be marked as EXPR
	if col.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression, got %v", col.Transform)
	}

	// Should have both source columns
	if len(col.Sources) != 2 {
		t.Errorf("Expected 2 sources, got %d", len(col.Sources))
	}
}

func TestExtractLineage_ScalarFunction(t *testing.T) {
	sql := `SELECT UPPER(name) AS upper_name FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	col := findColumn(lineage.Columns, "upper_name")
	if col == nil {
		t.Fatal("Missing 'upper_name' column")
	}

	// Passthrough function with single source should be direct
	if col.Transform != TransformDirect {
		t.Errorf("Expected TransformDirect for UPPER, got %v", col.Transform)
	}
}

func TestExtractLineage_COALESCE(t *testing.T) {
	sql := `SELECT COALESCE(nickname, name) AS display_name FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	col := findColumn(lineage.Columns, "display_name")
	if col == nil {
		t.Fatal("Missing 'display_name' column")
	}

	// COALESCE with multiple columns is an expression
	if col.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression, got %v", col.Transform)
	}

	// Should have both source columns
	if len(col.Sources) != 2 {
		t.Errorf("Expected 2 sources, got %d", len(col.Sources))
	}
}

func TestExtractLineage_CAST(t *testing.T) {
	sql := `SELECT CAST(id AS VARCHAR) AS id_str FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	col := findColumn(lineage.Columns, "id_str")
	if col == nil {
		t.Fatal("Missing 'id_str' column")
	}

	// CAST is a transformation
	if col.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression for CAST, got %v", col.Transform)
	}
}

// =============================================================================
// Test: Aggregate functions
// =============================================================================

func TestExtractLineage_COUNT(t *testing.T) {
	sql := `SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Check customer_id is direct
	custCol := findColumn(lineage.Columns, "customer_id")
	if custCol == nil {
		t.Fatal("Missing 'customer_id' column")
	}
	if custCol.Transform != TransformDirect {
		t.Errorf("Expected TransformDirect for customer_id, got %v", custCol.Transform)
	}

	// Check COUNT is aggregate
	countCol := findColumn(lineage.Columns, "order_count")
	if countCol == nil {
		t.Fatal("Missing 'order_count' column")
	}
	if countCol.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression for COUNT, got %v", countCol.Transform)
	}
	if countCol.Function != "count" {
		t.Errorf("Expected function 'count', got '%s'", countCol.Function)
	}
}

func TestExtractLineage_SUM(t *testing.T) {
	sql := `SELECT customer_id, SUM(amount) AS total_amount FROM orders GROUP BY customer_id`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	sumCol := findColumn(lineage.Columns, "total_amount")
	if sumCol == nil {
		t.Fatal("Missing 'total_amount' column")
	}
	if sumCol.Function != "sum" {
		t.Errorf("Expected function 'sum', got '%s'", sumCol.Function)
	}
	if sumCol.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression, got %v", sumCol.Transform)
	}
}

func TestExtractLineage_AVG(t *testing.T) {
	sql := `SELECT product_id, AVG(price) AS avg_price FROM products GROUP BY product_id`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	avgCol := findColumn(lineage.Columns, "avg_price")
	if avgCol == nil {
		t.Fatal("Missing 'avg_price' column")
	}
	if avgCol.Function != "avg" {
		t.Errorf("Expected function 'avg', got '%s'", avgCol.Function)
	}
}

// =============================================================================
// Test: Window functions
// =============================================================================

func TestExtractLineage_WindowFunction(t *testing.T) {
	sql := `SELECT 
		id,
		amount,
		SUM(amount) OVER (PARTITION BY customer_id ORDER BY created_at) AS running_total
	FROM orders`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// id should be direct
	idCol := findColumn(lineage.Columns, "id")
	if idCol == nil {
		t.Fatal("Missing 'id' column")
	}
	if idCol.Transform != TransformDirect {
		t.Errorf("Expected TransformDirect for id, got %v", idCol.Transform)
	}

	// running_total is a window function
	runningCol := findColumn(lineage.Columns, "running_total")
	if runningCol == nil {
		t.Fatal("Missing 'running_total' column")
	}
	if runningCol.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression, got %v", runningCol.Transform)
	}
	// Window function should track sources from all parts
	// amount (main arg), customer_id (partition), created_at (order)
	if len(runningCol.Sources) < 1 {
		t.Errorf("Expected at least 1 source for window function, got %d", len(runningCol.Sources))
	}
}

func TestExtractLineage_ROW_NUMBER(t *testing.T) {
	sql := `SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) AS row_num FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	rowCol := findColumn(lineage.Columns, "row_num")
	if rowCol == nil {
		t.Fatal("Missing 'row_num' column")
	}
	if rowCol.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression, got %v", rowCol.Transform)
	}
}

// =============================================================================
// Test: CTEs (Common Table Expressions)
// =============================================================================

func TestExtractLineage_SimpleCTE(t *testing.T) {
	sql := `
	WITH active_users AS (
		SELECT id, name FROM users WHERE status = 'active'
	)
	SELECT id, name FROM active_users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Source should trace back to original table, not the CTE
	if !contains(lineage.Sources, "users") {
		t.Errorf("Expected 'users' in sources, got %v", lineage.Sources)
	}

	// Should have 2 columns
	if len(lineage.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(lineage.Columns))
	}
}

func TestExtractLineage_MultipleCTEs(t *testing.T) {
	sql := `
	WITH 
		customers AS (
			SELECT id, name FROM users WHERE type = 'customer'
		),
		orders_summary AS (
			SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
		)
	SELECT c.name, o.total
	FROM customers c
	JOIN orders_summary o ON c.id = o.customer_id`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should trace back to both original tables
	if !contains(lineage.Sources, "users") {
		t.Errorf("Expected 'users' in sources, got %v", lineage.Sources)
	}
	if !contains(lineage.Sources, "orders") {
		t.Errorf("Expected 'orders' in sources, got %v", lineage.Sources)
	}
}

func TestExtractLineage_CTEWithAggregation(t *testing.T) {
	sql := `
	WITH daily_totals AS (
		SELECT DATE(created_at) AS day, SUM(amount) AS total
		FROM transactions
		GROUP BY DATE(created_at)
	)
	SELECT day, total FROM daily_totals`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should trace to transactions
	if !contains(lineage.Sources, "transactions") {
		t.Errorf("Expected 'transactions' in sources, got %v", lineage.Sources)
	}
}

// =============================================================================
// Test: JOINs
// =============================================================================

func TestExtractLineage_InnerJoin(t *testing.T) {
	sql := `
	SELECT u.name, o.amount
	FROM users u
	INNER JOIN orders o ON u.id = o.user_id`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should have both tables as sources
	if !contains(lineage.Sources, "users") {
		t.Errorf("Expected 'users' in sources")
	}
	if !contains(lineage.Sources, "orders") {
		t.Errorf("Expected 'orders' in sources")
	}

	// name comes from users
	nameCol := findColumn(lineage.Columns, "name")
	if nameCol == nil {
		t.Fatal("Missing 'name' column")
	}
	if len(nameCol.Sources) > 0 && nameCol.Sources[0].Table != "users" {
		t.Errorf("Expected 'name' source table 'users', got '%s'", nameCol.Sources[0].Table)
	}

	// amount comes from orders
	amtCol := findColumn(lineage.Columns, "amount")
	if amtCol == nil {
		t.Fatal("Missing 'amount' column")
	}
	if len(amtCol.Sources) > 0 && amtCol.Sources[0].Table != "orders" {
		t.Errorf("Expected 'amount' source table 'orders', got '%s'", amtCol.Sources[0].Table)
	}
}

func TestExtractLineage_LeftJoin(t *testing.T) {
	sql := `
	SELECT c.name, COALESCE(SUM(o.amount), 0) AS total_orders
	FROM customers c
	LEFT JOIN orders o ON c.id = o.customer_id
	GROUP BY c.name`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Both tables should be sources
	if !contains(lineage.Sources, "customers") {
		t.Errorf("Expected 'customers' in sources")
	}
	if !contains(lineage.Sources, "orders") {
		t.Errorf("Expected 'orders' in sources")
	}
}

func TestExtractLineage_MultipleJoins(t *testing.T) {
	sql := `
	SELECT 
		c.name AS customer_name,
		p.name AS product_name,
		oi.quantity
	FROM customers c
	JOIN orders o ON c.id = o.customer_id
	JOIN order_items oi ON o.id = oi.order_id
	JOIN products p ON oi.product_id = p.id`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// All four tables should be sources
	expected := []string{"customers", "orders", "order_items", "products"}
	for _, table := range expected {
		if !contains(lineage.Sources, table) {
			t.Errorf("Expected '%s' in sources, got %v", table, lineage.Sources)
		}
	}
}

// =============================================================================
// Test: UNION / INTERSECT / EXCEPT
// =============================================================================

func TestExtractLineage_Union(t *testing.T) {
	sql := `
	SELECT id, name FROM customers
	UNION
	SELECT id, name FROM suppliers`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Both tables should be sources
	if !contains(lineage.Sources, "customers") {
		t.Errorf("Expected 'customers' in sources")
	}
	if !contains(lineage.Sources, "suppliers") {
		t.Errorf("Expected 'suppliers' in sources")
	}

	// Should have 2 columns
	if len(lineage.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(lineage.Columns))
	}

	// UNION columns should be marked as expressions (merged from multiple sources)
	idCol := findColumn(lineage.Columns, "id")
	if idCol != nil && idCol.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression for UNION column, got %v", idCol.Transform)
	}
}

func TestExtractLineage_UnionAll(t *testing.T) {
	sql := `
	SELECT id, email FROM users
	UNION ALL
	SELECT id, email FROM archived_users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	if !contains(lineage.Sources, "users") {
		t.Errorf("Expected 'users' in sources")
	}
	if !contains(lineage.Sources, "archived_users") {
		t.Errorf("Expected 'archived_users' in sources")
	}
}

func TestExtractLineage_Except(t *testing.T) {
	sql := `
	SELECT id FROM all_users
	EXCEPT
	SELECT id FROM blocked_users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	if !contains(lineage.Sources, "all_users") {
		t.Errorf("Expected 'all_users' in sources")
	}
	if !contains(lineage.Sources, "blocked_users") {
		t.Errorf("Expected 'blocked_users' in sources")
	}
}

// =============================================================================
// Test: SELECT * expansion
// =============================================================================

func TestExtractLineage_StarWithoutSchema(t *testing.T) {
	sql := `SELECT * FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Without schema, we should get a single "*" column
	if len(lineage.Columns) != 1 {
		t.Errorf("Expected 1 column (unexpanded *), got %d", len(lineage.Columns))
	}
	if lineage.Columns[0].Name != "*" {
		t.Errorf("Expected column name '*', got '%s'", lineage.Columns[0].Name)
	}
}

func TestExtractLineage_StarWithSchema(t *testing.T) {
	schema := Schema{
		"users": {"id", "name", "email", "created_at"},
	}
	sql := `SELECT * FROM users`

	lineage, err := ExtractLineage(sql, schema)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// With schema, * should expand to all columns
	if len(lineage.Columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(lineage.Columns))
	}

	expectedCols := []string{"id", "name", "email", "created_at"}
	for _, name := range expectedCols {
		col := findColumn(lineage.Columns, name)
		if col == nil {
			t.Errorf("Missing expanded column: %s", name)
		}
	}
}

func TestExtractLineage_TableStarWithSchema(t *testing.T) {
	schema := Schema{
		"users":  {"id", "name"},
		"orders": {"id", "user_id", "amount"},
	}
	sql := `SELECT u.*, o.amount FROM users u JOIN orders o ON u.id = o.user_id`

	lineage, err := ExtractLineage(sql, schema)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should have: id, name (from u.*) + amount
	if len(lineage.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(lineage.Columns))
	}
}

// =============================================================================
// Test: Derived tables (subqueries in FROM)
// =============================================================================

func TestExtractLineage_DerivedTable(t *testing.T) {
	sql := `
	SELECT sub.id, sub.total
	FROM (
		SELECT customer_id AS id, SUM(amount) AS total
		FROM orders
		GROUP BY customer_id
	) sub`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should trace back to orders
	if !contains(lineage.Sources, "orders") {
		t.Errorf("Expected 'orders' in sources, got %v", lineage.Sources)
	}
}

func TestExtractLineage_NestedDerivedTables(t *testing.T) {
	sql := `
	SELECT final.name, final.order_count
	FROM (
		SELECT u.name, counts.order_count
		FROM users u
		JOIN (
			SELECT user_id, COUNT(*) AS order_count
			FROM orders
			GROUP BY user_id
		) counts ON u.id = counts.user_id
	) final`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should trace to both original tables
	if !contains(lineage.Sources, "users") {
		t.Errorf("Expected 'users' in sources")
	}
	if !contains(lineage.Sources, "orders") {
		t.Errorf("Expected 'orders' in sources")
	}
}

// =============================================================================
// Test: CASE expressions
// =============================================================================

func TestExtractLineage_CaseExpression(t *testing.T) {
	sql := `
	SELECT 
		id,
		CASE 
			WHEN status = 'active' THEN 'Active'
			WHEN status = 'pending' THEN 'Pending'
			ELSE 'Unknown'
		END AS status_label
	FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	caseCol := findColumn(lineage.Columns, "status_label")
	if caseCol == nil {
		t.Fatal("Missing 'status_label' column")
	}
	if caseCol.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression for CASE, got %v", caseCol.Transform)
	}
}

// =============================================================================
// Test: Literal values
// =============================================================================

func TestExtractLineage_Literal(t *testing.T) {
	sql := `SELECT id, 'constant' AS label, 42 AS magic_number FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Literal columns should have no sources
	labelCol := findColumn(lineage.Columns, "label")
	if labelCol == nil {
		t.Fatal("Missing 'label' column")
	}
	if len(labelCol.Sources) != 0 {
		t.Errorf("Expected no sources for literal, got %d", len(labelCol.Sources))
	}
	if labelCol.Transform != TransformExpression {
		t.Errorf("Expected TransformExpression for literal, got %v", labelCol.Transform)
	}
}

// =============================================================================
// Test: Schema-qualified table names
// =============================================================================

func TestExtractLineage_SchemaQualifiedTable(t *testing.T) {
	sql := `SELECT id, name FROM public.users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Source should include schema qualification
	if !contains(lineage.Sources, "public.users") {
		t.Errorf("Expected 'public.users' in sources, got %v", lineage.Sources)
	}
}

func TestExtractLineage_CatalogSchemaTable(t *testing.T) {
	sql := `SELECT id FROM mydb.myschema.users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	if !contains(lineage.Sources, "mydb.myschema.users") {
		t.Errorf("Expected 'mydb.myschema.users' in sources, got %v", lineage.Sources)
	}
}

// =============================================================================
// Test: Generator functions (no source columns)
// =============================================================================

func TestExtractLineage_GeneratorFunction(t *testing.T) {
	sql := `SELECT id, NOW() AS current_time, RANDOM() AS rand_val FROM users`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// NOW() should have no source columns
	nowCol := findColumn(lineage.Columns, "current_time")
	if nowCol == nil {
		t.Fatal("Missing 'current_time' column")
	}
	if len(nowCol.Sources) != 0 {
		t.Errorf("Expected no sources for NOW(), got %d", len(nowCol.Sources))
	}
}

// =============================================================================
// Test: Complex real-world queries
// =============================================================================

func TestExtractLineage_ComplexQuery(t *testing.T) {
	sql := `
	WITH monthly_sales AS (
		SELECT 
			DATE_TRUNC('month', o.created_at) AS month,
			p.category_id,
			SUM(oi.quantity * oi.unit_price) AS revenue
		FROM orders o
		JOIN order_items oi ON o.id = oi.order_id
		JOIN products p ON oi.product_id = p.id
		WHERE o.status = 'completed'
		GROUP BY DATE_TRUNC('month', o.created_at), p.category_id
	),
	category_totals AS (
		SELECT 
			c.name AS category_name,
			ms.month,
			ms.revenue,
			SUM(ms.revenue) OVER (PARTITION BY c.id ORDER BY ms.month) AS cumulative_revenue
		FROM monthly_sales ms
		JOIN categories c ON ms.category_id = c.id
	)
	SELECT 
		category_name,
		month,
		revenue,
		cumulative_revenue,
		ROUND(revenue / NULLIF(LAG(revenue) OVER (PARTITION BY category_name ORDER BY month), 0) * 100 - 100, 2) AS growth_pct
	FROM category_totals
	ORDER BY category_name, month`

	lineage, err := ExtractLineage(sql, nil)
	if err != nil {
		t.Fatalf("ExtractLineage failed: %v", err)
	}

	// Should trace to all original tables
	expectedTables := []string{"orders", "order_items", "products", "categories"}
	for _, table := range expectedTables {
		if !contains(lineage.Sources, table) {
			t.Errorf("Expected '%s' in sources, got %v", table, lineage.Sources)
		}
	}

	// Should have 5 output columns
	if len(lineage.Columns) != 5 {
		t.Errorf("Expected 5 columns, got %d", len(lineage.Columns))
	}
}

// =============================================================================
// Test: Error handling
// =============================================================================

func TestExtractLineage_InvalidSQL(t *testing.T) {
	sql := `SELECT FROM WHERE`

	_, err := ExtractLineage(sql, nil)
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}

func TestExtractLineage_EmptySQL(t *testing.T) {
	sql := ``

	_, err := ExtractLineage(sql, nil)
	if err == nil {
		t.Error("Expected error for empty SQL")
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkExtractLineage_Simple(b *testing.B) {
	sql := `SELECT id, name, email FROM users`
	for i := 0; i < b.N; i++ {
		_, _ = ExtractLineage(sql, nil)
	}
}

func BenchmarkExtractLineage_Complex(b *testing.B) {
	sql := `
	WITH cte AS (
		SELECT id, SUM(amount) AS total FROM orders GROUP BY id
	)
	SELECT u.name, c.total
	FROM users u
	JOIN cte c ON u.id = c.id
	WHERE u.status = 'active'`

	for i := 0; i < b.N; i++ {
		_, _ = ExtractLineage(sql, nil)
	}
}
