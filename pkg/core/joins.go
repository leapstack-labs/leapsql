package core

import "github.com/leapstack-labs/leapsql/pkg/token"

// Standard ANSI SQL join type values.
const (
	JoinInner = "INNER"
	JoinLeft  = "LEFT"
	JoinRight = "RIGHT"
	JoinFull  = "FULL"
	JoinCross = "CROSS"
)

// JoinTypeDef defines a dialect-specific join type.
type JoinTypeDef struct {
	Token         token.TokenType // The trigger token for this join type
	Type          string          // JoinType value (e.g., "LEFT", "SEMI")
	OptionalToken token.TokenType // Optional modifier token (OUTER) - 0 means none
	RequiresOn    bool            // true if ON clause is required
	AllowsUsing   bool            // true if USING clause is allowed
}
