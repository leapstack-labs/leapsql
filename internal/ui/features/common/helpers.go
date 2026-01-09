// Package common provides shared types and utilities for UI features.
package common

// Itoa converts an integer to a string without using strconv.
// This is used in templ files where we want to avoid additional imports.
func Itoa(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	negative := n < 0
	if negative {
		n = -n
	}
	for n > 0 {
		result = string('0'+byte(n%10)) + result
		n /= 10
	}
	if negative {
		result = "-" + result
	}
	return result
}

// MaterializationLabel returns a human-readable label for a materialization type.
func MaterializationLabel(mat string) string {
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

// LenStr returns the length of a TreeNode slice as a formatted string like "(5)".
func LenStr(nodes []TreeNode) string {
	return "(" + Itoa(len(nodes)) + ")"
}
