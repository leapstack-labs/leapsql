package dialect

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// DuckDB-specific star modifier tokens (registered dynamically)
var (
	// TokenExclude is the EXCLUDE star modifier keyword
	TokenExclude = token.Register("EXCLUDE")
	// TokenReplace is the REPLACE star modifier keyword
	TokenReplace = token.Register("REPLACE")
	// TokenRename is the RENAME star modifier keyword
	TokenRename = token.Register("RENAME")
)

// parseExclude handles * EXCLUDE (col1, col2, ...).
// The EXCLUDE keyword has already been consumed.
func parseExclude(p spi.ParserOps) (spi.StarModifier, error) {
	if err := p.Expect(token.LPAREN); err != nil {
		return nil, fmt.Errorf("EXCLUDE: %w", err)
	}

	var cols []string
	for {
		name, err := p.ParseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("EXCLUDE: %w", err)
		}
		cols = append(cols, name)

		if !p.Match(token.COMMA) {
			break
		}
	}

	if err := p.Expect(token.RPAREN); err != nil {
		return nil, fmt.Errorf("EXCLUDE: %w", err)
	}

	return &parser.ExcludeModifier{Columns: cols}, nil
}

// parseReplace handles * REPLACE (expr AS col, ...).
// The REPLACE keyword has already been consumed.
func parseReplace(p spi.ParserOps) (spi.StarModifier, error) {
	if err := p.Expect(token.LPAREN); err != nil {
		return nil, fmt.Errorf("REPLACE: %w", err)
	}

	var items []parser.ReplaceItem
	for {
		expr, err := p.ParseExpression()
		if err != nil {
			return nil, fmt.Errorf("REPLACE: %w", err)
		}

		if err := p.Expect(token.AS); err != nil {
			return nil, fmt.Errorf("REPLACE: expected AS after expression: %w", err)
		}

		name, err := p.ParseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("REPLACE: %w", err)
		}

		items = append(items, parser.ReplaceItem{
			Expr:  expr.(parser.Expr),
			Alias: name,
		})

		if !p.Match(token.COMMA) {
			break
		}
	}

	if err := p.Expect(token.RPAREN); err != nil {
		return nil, fmt.Errorf("REPLACE: %w", err)
	}

	return &parser.ReplaceModifier{Items: items}, nil
}

// parseRename handles * RENAME (old AS new, ...).
// The RENAME keyword has already been consumed.
func parseRename(p spi.ParserOps) (spi.StarModifier, error) {
	if err := p.Expect(token.LPAREN); err != nil {
		return nil, fmt.Errorf("RENAME: %w", err)
	}

	var items []parser.RenameItem
	for {
		oldName, err := p.ParseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("RENAME: %w", err)
		}

		if err := p.Expect(token.AS); err != nil {
			return nil, fmt.Errorf("RENAME: expected AS after old column name: %w", err)
		}

		newName, err := p.ParseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("RENAME: %w", err)
		}

		items = append(items, parser.RenameItem{
			OldName: oldName,
			NewName: newName,
		})

		if !p.Match(token.COMMA) {
			break
		}
	}

	if err := p.Expect(token.RPAREN); err != nil {
		return nil, fmt.Errorf("RENAME: %w", err)
	}

	return &parser.RenameModifier{Items: items}, nil
}
