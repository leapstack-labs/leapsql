package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentStore_OpenGetClose(t *testing.T) {
	store := NewDocumentStore()

	uri := "file:///test/model.sql"
	content := "SELECT * FROM users"

	// Open document
	store.Open(uri, content, 1)

	// Get document
	doc := store.Get(uri)
	require.NotNil(t, doc, "expected document to exist")
	assert.Equal(t, uri, doc.URI, "expected URI to match")
	assert.Equal(t, content, doc.Content, "expected content to match")
	assert.Equal(t, 1, doc.Version, "expected version to match")

	// Close document
	store.Close(uri)
	doc = store.Get(uri)
	assert.Nil(t, doc, "expected document to be nil after close")
}

func TestDocumentStore_Update(t *testing.T) {
	store := NewDocumentStore()

	uri := "file:///test/model.sql"
	store.Open(uri, "SELECT 1", 1)

	// Update
	store.Update(uri, "SELECT 2", 2)

	doc := store.Get(uri)
	assert.Equal(t, "SELECT 2", doc.Content, "expected content to be updated")
	assert.Equal(t, 2, doc.Version, "expected version to be updated")
}

func TestDocumentStore_List(t *testing.T) {
	store := NewDocumentStore()

	store.Open("file:///a.sql", "SELECT a", 1)
	store.Open("file:///b.sql", "SELECT b", 1)
	store.Open("file:///c.sql", "SELECT c", 1)

	uris := store.List()
	assert.Len(t, uris, 3, "expected 3 URIs")
}

func TestComputeLineOffsets(t *testing.T) {
	tests := []struct {
		content  string
		expected []int
	}{
		{"", []int{0}},
		{"abc", []int{0}},
		{"a\nb", []int{0, 2}},
		{"a\nb\nc", []int{0, 2, 4}},
		{"\n\n\n", []int{0, 1, 2, 3}},
		{"line1\nline2\nline3", []int{0, 6, 12}},
	}

	for _, tt := range tests {
		offsets := computeLineOffsets(tt.content)
		require.Len(t, offsets, len(tt.expected), "content %q: wrong number of offsets", tt.content)
		for i, exp := range tt.expected {
			assert.Equal(t, exp, offsets[i], "content %q: offset[%d]", tt.content, i)
		}
	}
}

func TestDocument_PositionToOffset(t *testing.T) {
	content := "line0\nline1\nline2"
	doc := &Document{
		Content: content,
		Lines:   computeLineOffsets(content),
	}

	tests := []struct {
		pos      Position
		expected int
	}{
		{Position{Line: 0, Character: 0}, 0},
		{Position{Line: 0, Character: 3}, 3},
		{Position{Line: 0, Character: 5}, 5},
		{Position{Line: 1, Character: 0}, 6},
		{Position{Line: 1, Character: 4}, 10},
		{Position{Line: 2, Character: 0}, 12},
		{Position{Line: 2, Character: 5}, 17},
		// Edge cases
		{Position{Line: 100, Character: 0}, len(content)}, // Line beyond document
		{Position{Line: 0, Character: 100}, len(content)}, // Character beyond line
	}

	for _, tt := range tests {
		offset := doc.PositionToOffset(tt.pos)
		assert.Equal(t, tt.expected, offset, "PositionToOffset(%v)", tt.pos)
	}
}

func TestDocument_OffsetToPosition(t *testing.T) {
	content := "line0\nline1\nline2"
	doc := &Document{
		Content: content,
		Lines:   computeLineOffsets(content),
	}

	tests := []struct {
		offset   int
		expected Position
	}{
		{0, Position{Line: 0, Character: 0}},
		{3, Position{Line: 0, Character: 3}},
		{5, Position{Line: 0, Character: 5}},
		{6, Position{Line: 1, Character: 0}},
		{10, Position{Line: 1, Character: 4}},
		{12, Position{Line: 2, Character: 0}},
		{17, Position{Line: 2, Character: 5}},
		// Edge cases
		{-1, Position{Line: 0, Character: 0}},  // Negative offset
		{100, Position{Line: 2, Character: 5}}, // Beyond end
	}

	for _, tt := range tests {
		pos := doc.OffsetToPosition(tt.offset)
		assert.Equal(t, tt.expected.Line, pos.Line, "OffsetToPosition(%d) line", tt.offset)
		assert.Equal(t, tt.expected.Character, pos.Character, "OffsetToPosition(%d) character", tt.offset)
	}
}

func TestDocument_GetLine(t *testing.T) {
	content := "line0\nline1\nline2"
	doc := &Document{
		Content: content,
		Lines:   computeLineOffsets(content),
	}

	tests := []struct {
		line     int
		expected string
	}{
		{0, "line0"},
		{1, "line1"},
		{2, "line2"},
		{-1, ""},
		{100, ""},
	}

	for _, tt := range tests {
		line := doc.GetLine(tt.line)
		assert.Equal(t, tt.expected, line, "GetLine(%d)", tt.line)
	}
}

func TestDocument_GetWordAtPosition(t *testing.T) {
	content := "SELECT id, name FROM users WHERE active"
	doc := &Document{
		Content: content,
		Lines:   computeLineOffsets(content),
	}

	tests := []struct {
		pos          Position
		expectedWord string
	}{
		{Position{Line: 0, Character: 0}, "SELECT"},
		{Position{Line: 0, Character: 3}, "SELECT"},
		{Position{Line: 0, Character: 7}, "id"},
		{Position{Line: 0, Character: 11}, "name"},
		{Position{Line: 0, Character: 21}, "users"},
		{Position{Line: 0, Character: 33}, "active"},
	}

	for _, tt := range tests {
		word, _ := doc.GetWordAtPosition(tt.pos)
		assert.Equal(t, tt.expectedWord, word, "GetWordAtPosition(%v)", tt.pos)
	}
}

func TestDocument_GetTextBefore(t *testing.T) {
	content := "SELECT * FROM users"
	doc := &Document{
		Content: content,
		Lines:   computeLineOffsets(content),
	}

	tests := []struct {
		pos      Position
		expected string
	}{
		{Position{Line: 0, Character: 0}, ""},
		{Position{Line: 0, Character: 6}, "SELECT"},
		{Position{Line: 0, Character: 9}, "SELECT * "},
	}

	for _, tt := range tests {
		text := doc.GetTextBefore(tt.pos)
		assert.Equal(t, tt.expected, text, "GetTextBefore(%v)", tt.pos)
	}
}

func TestDocument_GetTextAfter(t *testing.T) {
	content := "SELECT * FROM users"
	doc := &Document{
		Content: content,
		Lines:   computeLineOffsets(content),
	}

	tests := []struct {
		pos      Position
		expected string
	}{
		{Position{Line: 0, Character: 0}, "SELECT * FROM users"},
		{Position{Line: 0, Character: 7}, "* FROM users"},
		{Position{Line: 0, Character: 19}, ""},
	}

	for _, tt := range tests {
		text := doc.GetTextAfter(tt.pos)
		assert.Equal(t, tt.expected, text, "GetTextAfter(%v)", tt.pos)
	}
}

func TestDocument_GetTextInRange(t *testing.T) {
	content := "SELECT * FROM users"
	doc := &Document{
		Content: content,
		Lines:   computeLineOffsets(content),
	}

	tests := []struct {
		r        Range
		expected string
	}{
		{
			Range{Start: Position{Line: 0, Character: 0}, End: Position{Line: 0, Character: 6}},
			"SELECT",
		},
		{
			Range{Start: Position{Line: 0, Character: 7}, End: Position{Line: 0, Character: 8}},
			"*",
		},
		{
			Range{Start: Position{Line: 0, Character: 14}, End: Position{Line: 0, Character: 19}},
			"users",
		},
	}

	for _, tt := range tests {
		text := doc.GetTextInRange(tt.r)
		assert.Equal(t, tt.expected, text, "GetTextInRange(%v)", tt.r)
	}
}

func TestURIToPath(t *testing.T) {
	tests := []struct {
		uri      string
		expected string
	}{
		{"file:///Users/test/model.sql", "/Users/test/model.sql"},
		{"file:///home/user/file.sql", "/home/user/file.sql"},
		{"/already/a/path.sql", "/already/a/path.sql"},
	}

	for _, tt := range tests {
		path := URIToPath(tt.uri)
		assert.Equal(t, tt.expected, path, "URIToPath(%q)", tt.uri)
	}
}

func TestPathToURI(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"/Users/test/model.sql", "file:///Users/test/model.sql"},
		{"/home/user/file.sql", "file:///home/user/file.sql"},
		{"file:///already/uri.sql", "file:///already/uri.sql"},
	}

	for _, tt := range tests {
		uri := PathToURI(tt.path)
		assert.Equal(t, tt.expected, uri, "PathToURI(%q)", tt.path)
	}
}

func TestIsWordChar(t *testing.T) {
	wordChars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
	nonWordChars := " \t\n!@#$%^&*()-+=[]{}|;':\",./<>?"

	for _, c := range wordChars {
		assert.True(t, isWordChar(byte(c)), "isWordChar(%q): expected true", c)
	}

	for _, c := range nonWordChars {
		assert.False(t, isWordChar(byte(c)), "isWordChar(%q): expected false", c)
	}
}
