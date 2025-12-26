// Package dialect provides SQL dialect configuration and function classification.
//
// This file contains pre-built ClauseDef definitions - the "menu items" that
// dialects can compose from. Each ClauseDef bundles a token, handler, slot,
// and metadata together.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// --- Standard Clause Definitions ---
// These are pre-configured ClauseDefs that dialects can compose.
// Each bundles the token, handler, slot, and formatting metadata.

var (
	// StandardWhere is the standard WHERE clause definition.
	StandardWhere = ClauseDef{
		Token:   token.WHERE,
		Handler: ParseWhere,
		Slot:    spi.SlotWhere,
	}

	// StandardGroupBy is the standard GROUP BY clause definition.
	StandardGroupBy = ClauseDef{
		Token:    token.GROUP,
		Handler:  ParseGroupBy,
		Slot:     spi.SlotGroupBy,
		Keywords: []string{"GROUP", "BY"},
	}

	// StandardHaving is the standard HAVING clause definition.
	StandardHaving = ClauseDef{
		Token:   token.HAVING,
		Handler: ParseHaving,
		Slot:    spi.SlotHaving,
	}

	// StandardWindow is the standard WINDOW clause definition.
	StandardWindow = ClauseDef{
		Token:   token.WINDOW,
		Handler: ParseWindow,
		Slot:    spi.SlotWindow,
	}

	// StandardOrderBy is the standard ORDER BY clause definition.
	StandardOrderBy = ClauseDef{
		Token:    token.ORDER,
		Handler:  ParseOrderBy,
		Slot:     spi.SlotOrderBy,
		Keywords: []string{"ORDER", "BY"},
	}

	// StandardLimit is the standard LIMIT clause definition.
	StandardLimit = ClauseDef{
		Token:   token.LIMIT,
		Handler: ParseLimit,
		Slot:    spi.SlotLimit,
		Inline:  true,
	}

	// StandardOffset is the standard OFFSET clause definition.
	StandardOffset = ClauseDef{
		Token:   token.OFFSET,
		Handler: ParseOffset,
		Slot:    spi.SlotOffset,
		Inline:  true,
	}

	// StandardFetch is the standard FETCH clause definition (SQL:2008).
	StandardFetch = ClauseDef{
		Token:   token.FETCH,
		Handler: ParseFetch,
		Slot:    spi.SlotFetch,
	}
)

// StandardSelectClauses is the typical ANSI SELECT clause sequence.
// Dialects can use this directly or compose their own from the individual defs.
var StandardSelectClauses = []ClauseDef{
	StandardWhere,
	StandardGroupBy,
	StandardHaving,
	StandardWindow,
	StandardOrderBy,
	StandardLimit,
	StandardOffset,
	StandardFetch,
}
