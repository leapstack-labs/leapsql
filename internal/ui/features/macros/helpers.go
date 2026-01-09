package macros

import (
	"github.com/leapstack-labs/leapsql/internal/ui/features/common"
)

// isSelectedFunction checks if a function is currently selected.
func isSelectedFunction(selected *FunctionDetail, namespace, name string) bool {
	if selected == nil {
		return false
	}
	return selected.Namespace == namespace && selected.Name == name
}

// itoa converts int to string using common helper.
func itoa(n int) string {
	return common.Itoa(n)
}
