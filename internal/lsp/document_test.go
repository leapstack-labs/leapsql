package lsp

import (
	"testing"
)

func TestDocumentStore_OpenGetClose(t *testing.T) {
	store := NewDocumentStore()

	uri := "file:///test/model.sql"
	content := "SELECT * FROM users"

	// Open document
	store.Open(uri, content, 1)

	// Get document
	doc := store.Get(uri)
	if doc == nil {
		t.Fatal("expected document to exist")
	}
	if doc.URI != uri {
		t.Errorf("expected URI %s, got %s", uri, doc.URI)
	}
	if doc.Content != content {
		t.Errorf("expected content %q, got %q", content, doc.Content)
	}
	if doc.Version != 1 {
		t.Errorf("expected version 1, got %d", doc.Version)
	}

	// Close document
	store.Close(uri)
	doc = store.Get(uri)
	if doc != nil {
		t.Error("expected document to be nil after close")
	}
}

func TestDocumentStore_Update(t *testing.T) {
	store := NewDocumentStore()

	uri := "file:///test/model.sql"
	store.Open(uri, "SELECT 1", 1)

	// Update
	store.Update(uri, "SELECT 2", 2)

	doc := store.Get(uri)
	if doc.Content != "SELECT 2" {
		t.Errorf("expected content 'SELECT 2', got %q", doc.Content)
	}
	if doc.Version != 2 {
		t.Errorf("expected version 2, got %d", doc.Version)
	}
}

func TestDocumentStore_List(t *testing.T) {
	store := NewDocumentStore()

	store.Open("file:///a.sql", "SELECT a", 1)
	store.Open("file:///b.sql", "SELECT b", 1)
	store.Open("file:///c.sql", "SELECT c", 1)

	uris := store.List()
	if len(uris) != 3 {
		t.Errorf("expected 3 URIs, got %d", len(uris))
	}
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
		if len(offsets) != len(tt.expected) {
			t.Errorf("content %q: expected %d offsets, got %d", tt.content, len(tt.expected), len(offsets))
			continue
		}
		for i, exp := range tt.expected {
			if offsets[i] != exp {
				t.Errorf("content %q: offset[%d] expected %d, got %d", tt.content, i, exp, offsets[i])
			}
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
		if offset != tt.expected {
			t.Errorf("PositionToOffset(%v): expected %d, got %d", tt.pos, tt.expected, offset)
		}
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
		if pos.Line != tt.expected.Line || pos.Character != tt.expected.Character {
			t.Errorf("OffsetToPosition(%d): expected %v, got %v", tt.offset, tt.expected, pos)
		}
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
		if line != tt.expected {
			t.Errorf("GetLine(%d): expected %q, got %q", tt.line, tt.expected, line)
		}
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
		if word != tt.expectedWord {
			t.Errorf("GetWordAtPosition(%v): expected %q, got %q", tt.pos, tt.expectedWord, word)
		}
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
		if text != tt.expected {
			t.Errorf("GetTextBefore(%v): expected %q, got %q", tt.pos, tt.expected, text)
		}
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
		if text != tt.expected {
			t.Errorf("GetTextAfter(%v): expected %q, got %q", tt.pos, tt.expected, text)
		}
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
		if text != tt.expected {
			t.Errorf("GetTextInRange(%v): expected %q, got %q", tt.r, tt.expected, text)
		}
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
		if path != tt.expected {
			t.Errorf("URIToPath(%q): expected %q, got %q", tt.uri, tt.expected, path)
		}
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
		if uri != tt.expected {
			t.Errorf("PathToURI(%q): expected %q, got %q", tt.path, tt.expected, uri)
		}
	}
}

func TestIsWordChar(t *testing.T) {
	wordChars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
	nonWordChars := " \t\n!@#$%^&*()-+=[]{}|;':\",./<>?"

	for _, c := range wordChars {
		if !isWordChar(byte(c)) {
			t.Errorf("isWordChar(%q): expected true", c)
		}
	}

	for _, c := range nonWordChars {
		if isWordChar(byte(c)) {
			t.Errorf("isWordChar(%q): expected false", c)
		}
	}
}
