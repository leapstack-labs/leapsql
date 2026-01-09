// Package database provides database browser handlers for the UI.
package database

func tableClass(tableType string) string {
	if tableType == "view" {
		return "db-table--view"
	}
	return "db-table--table"
}

func tableIcon(tableType string) string {
	if tableType == "view" {
		return "V"
	}
	return "T"
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
