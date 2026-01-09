package pages

// Helper functions for model page components

func materializationLabel(mat string) string {
	switch mat {
	case "table":
		return "Table"
	case "view":
		return "View"
	case "incremental":
		return "Incremental"
	case "":
		return "View"
	default:
		return mat
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	for n > 0 {
		result = string('0'+byte(n%10)) + result
		n /= 10
	}
	return result
}
