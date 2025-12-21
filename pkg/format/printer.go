// Package format provides SQL statement formatting.
package format

import (
	"bytes"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

const indentSize = 2

// Printer handles SQL formatting with proper indentation and style.
type Printer struct {
	dialect     *dialect.Dialect
	output      *bytes.Buffer
	depth       int
	atLineStart bool
}

func newPrinter(d *dialect.Dialect) *Printer {
	return &Printer{
		dialect:     d,
		output:      &bytes.Buffer{},
		atLineStart: true,
	}
}

// String returns the formatted output.
func (p *Printer) String() string {
	return strings.TrimRight(p.output.String(), "\n") + "\n"
}

func (p *Printer) write(s string) {
	if p.atLineStart && len(s) > 0 && s[0] != '\n' {
		p.writeIndent()
	}
	p.output.WriteString(s)
	p.atLineStart = false
}

func (p *Printer) writeln() {
	p.output.WriteByte('\n')
	p.atLineStart = true
}

func (p *Printer) writeIndent() {
	for i := 0; i < p.depth*indentSize; i++ {
		p.output.WriteByte(' ')
	}
	p.atLineStart = false
}

func (p *Printer) keyword(s string) {
	p.write(strings.ToUpper(s))
}

func (p *Printer) indent() {
	p.depth++
}

func (p *Printer) dedent() {
	if p.depth > 0 {
		p.depth--
	}
}

func (p *Printer) space() {
	p.output.WriteByte(' ')
}

// kw prints a keyword based on the token type.
// It automatically handles capitalization or dialect specifics if we add them later.
func (p *Printer) kw(tokens ...token.TokenType) {
	for i, t := range tokens {
		if i > 0 {
			p.space()
		}
		p.write(t.String())
	}
}

func (p *Printer) formatComments(comments []*token.Comment) {
	for _, c := range comments {
		p.write(c.Text)
		p.writeln()
	}
}

func (p *Printer) formatTrailingComments(comments []*token.Comment) {
	for _, c := range comments {
		p.space()
		p.write(c.Text)
	}
}

// formatList prints a list of items with separators.
// count is the number of items, format is called for each index,
// sep is the separator string, multiline adds newlines after separators.
func (p *Printer) formatList(count int, format func(i int), sep string, multiline bool) {
	for i := 0; i < count; i++ {
		format(i)
		if i < count-1 {
			p.write(sep)
			if multiline {
				p.writeln()
			}
		}
	}
}
