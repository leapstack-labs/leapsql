package lint

import (
	"fmt"
	"strings"
)

// DefaultDocsBaseURL is the hosted documentation site.
const DefaultDocsBaseURL = "https://leapsql.dev/docs/rules"

// DocsBaseURL can be overridden via config for local/offline mode.
var DocsBaseURL = DefaultDocsBaseURL

// BuildDocURL constructs a documentation URL for a rule.
func BuildDocURL(ruleID string) string {
	return fmt.Sprintf("%s/%s", DocsBaseURL, strings.ToLower(ruleID))
}

// SetDocsBaseURL overrides the default documentation base URL.
// Useful for offline mode or custom documentation sites.
func SetDocsBaseURL(url string) {
	DocsBaseURL = strings.TrimSuffix(url, "/")
}

// ResetDocsBaseURL resets to the default documentation URL.
func ResetDocsBaseURL() {
	DocsBaseURL = DefaultDocsBaseURL
}

// ImpactLevel represents predefined impact score ranges.
type ImpactLevel int

const (
	// ImpactLow for minor issues (0-30)
	ImpactLow ImpactLevel = 20
	// ImpactMedium for moderate issues (31-60)
	ImpactMedium ImpactLevel = 50
	// ImpactHigh for significant issues (61-80)
	ImpactHigh ImpactLevel = 70
	// ImpactCritical for critical issues (81-100)
	ImpactCritical ImpactLevel = 90
)

// Int returns the impact score as an integer.
func (l ImpactLevel) Int() int {
	return int(l)
}
