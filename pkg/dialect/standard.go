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
// Handlers are explicitly typed to spi.ClauseHandler to enable type assertions at call site.

var (
	// StandardWhere is the standard WHERE clause definition.
	StandardWhere = ClauseDef{
		Token:   token.WHERE,
		Handler: spi.ClauseHandler(ParseWhere),
		Slot:    spi.SlotWhere,
	}

	// StandardGroupBy is the standard GROUP BY clause definition.
	StandardGroupBy = ClauseDef{
		Token:    token.GROUP,
		Handler:  spi.ClauseHandler(ParseGroupBy),
		Slot:     spi.SlotGroupBy,
		Keywords: []string{"GROUP", "BY"},
	}

	// StandardHaving is the standard HAVING clause definition.
	StandardHaving = ClauseDef{
		Token:   token.HAVING,
		Handler: spi.ClauseHandler(ParseHaving),
		Slot:    spi.SlotHaving,
	}

	// StandardWindow is the standard WINDOW clause definition.
	StandardWindow = ClauseDef{
		Token:   token.WINDOW,
		Handler: spi.ClauseHandler(ParseWindow),
		Slot:    spi.SlotWindow,
	}

	// StandardOrderBy is the standard ORDER BY clause definition.
	StandardOrderBy = ClauseDef{
		Token:    token.ORDER,
		Handler:  spi.ClauseHandler(ParseOrderBy),
		Slot:     spi.SlotOrderBy,
		Keywords: []string{"ORDER", "BY"},
	}

	// StandardLimit is the standard LIMIT clause definition.
	StandardLimit = ClauseDef{
		Token:   token.LIMIT,
		Handler: spi.ClauseHandler(ParseLimit),
		Slot:    spi.SlotLimit,
		Inline:  true,
	}

	// StandardOffset is the standard OFFSET clause definition.
	StandardOffset = ClauseDef{
		Token:   token.OFFSET,
		Handler: spi.ClauseHandler(ParseOffset),
		Slot:    spi.SlotOffset,
		Inline:  true,
	}

	// StandardFetch is the standard FETCH clause definition (SQL:2008).
	StandardFetch = ClauseDef{
		Token:   token.FETCH,
		Handler: spi.ClauseHandler(ParseFetch),
		Slot:    spi.SlotFetch,
	}

	// StandardQualify is the standard QUALIFY clause definition (DuckDB, Databricks, etc.).
	StandardQualify = ClauseDef{
		Token:   token.QUALIFY,
		Handler: spi.ClauseHandler(ParseQualify),
		Slot:    spi.SlotQualify,
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

// --- Configurable Clause Factory Functions ---
// These functions return ClauseDefs with behavior customized by options.

// GroupByOpts configures GROUP BY clause behavior.
type GroupByOpts struct {
	AllowAll bool // Support GROUP BY ALL
}

// GroupBy returns a ClauseDef for GROUP BY with options.
func GroupBy(opts GroupByOpts) ClauseDef {
	if opts.AllowAll {
		return ClauseDef{
			Token:    token.GROUP,
			Handler:  spi.ClauseHandler(ParseGroupByWithAll),
			Slot:     spi.SlotGroupBy,
			Keywords: []string{"GROUP", "BY"},
		}
	}
	return StandardGroupBy
}

// OrderByOpts configures ORDER BY clause behavior.
type OrderByOpts struct {
	AllowAll bool // Support ORDER BY ALL
}

// OrderBy returns a ClauseDef for ORDER BY with options.
func OrderBy(opts OrderByOpts) ClauseDef {
	if opts.AllowAll {
		return ClauseDef{
			Token:    token.ORDER,
			Handler:  spi.ClauseHandler(ParseOrderByWithAll),
			Slot:     spi.SlotOrderBy,
			Keywords: []string{"ORDER", "BY"},
		}
	}
	return StandardOrderBy
}
