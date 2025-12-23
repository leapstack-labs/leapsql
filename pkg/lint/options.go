package lint

// GetOption extracts a typed option with a default value.
func GetOption[T any](opts map[string]any, key string, defaultVal T) T {
	if opts == nil {
		return defaultVal
	}
	v, ok := opts[key]
	if !ok {
		return defaultVal
	}
	// Handle type conversion
	if typed, ok := v.(T); ok {
		return typed
	}
	return defaultVal
}

// GetIntOption extracts an int option, handling float64 from JSON.
func GetIntOption(opts map[string]any, key string, defaultVal int) int {
	if opts == nil {
		return defaultVal
	}
	v, ok := opts[key]
	if !ok {
		return defaultVal
	}
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case int64:
		return int(n)
	default:
		return defaultVal
	}
}

// GetStringOption extracts a string option.
func GetStringOption(opts map[string]any, key string, defaultVal string) string {
	if opts == nil {
		return defaultVal
	}
	v, ok := opts[key]
	if !ok {
		return defaultVal
	}
	if s, ok := v.(string); ok {
		return s
	}
	return defaultVal
}

// GetBoolOption extracts a bool option.
func GetBoolOption(opts map[string]any, key string, defaultVal bool) bool {
	if opts == nil {
		return defaultVal
	}
	v, ok := opts[key]
	if !ok {
		return defaultVal
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return defaultVal
}

// GetStringSliceOption extracts a string slice option.
func GetStringSliceOption(opts map[string]any, key string, defaultVal []string) []string {
	if opts == nil {
		return defaultVal
	}
	v, ok := opts[key]
	if !ok {
		return defaultVal
	}
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		result := make([]string, 0, len(s))
		for _, item := range s {
			if str, ok := item.(string); ok {
				result = append(result, str)
			}
		}
		return result
	default:
		return defaultVal
	}
}
