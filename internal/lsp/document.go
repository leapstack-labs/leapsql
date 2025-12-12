package lsp

import (
	"strings"
	"sync"
)

// Document represents an open text document in the editor.
type Document struct {
	URI     string // Document URI (file:///path/to/file.sql)
	Content string // Full document content
	Version int    // Version number, incremented on each change
	Lines   []int  // Byte offsets of line starts for fast position lookups
}

// DocumentStore manages open documents in memory.
type DocumentStore struct {
	mu        sync.RWMutex
	documents map[string]*Document
}

// NewDocumentStore creates a new document store.
func NewDocumentStore() *DocumentStore {
	return &DocumentStore{
		documents: make(map[string]*Document),
	}
}

// Open adds or updates a document in the store.
func (s *DocumentStore) Open(uri string, content string, version int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.documents[uri] = &Document{
		URI:     uri,
		Content: content,
		Version: version,
		Lines:   computeLineOffsets(content),
	}
}

// Close removes a document from the store.
func (s *DocumentStore) Close(uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.documents, uri)
}

// Get retrieves a document by URI.
func (s *DocumentStore) Get(uri string) *Document {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.documents[uri]
}

// Update modifies an existing document's content.
func (s *DocumentStore) Update(uri string, content string, version int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if doc, ok := s.documents[uri]; ok {
		doc.Content = content
		doc.Version = version
		doc.Lines = computeLineOffsets(content)
	}
}

// List returns all open document URIs.
func (s *DocumentStore) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	uris := make([]string, 0, len(s.documents))
	for uri := range s.documents {
		uris = append(uris, uri)
	}
	return uris
}

// computeLineOffsets calculates byte offsets for each line start.
func computeLineOffsets(content string) []int {
	offsets := []int{0} // First line starts at offset 0

	for i := 0; i < len(content); i++ {
		if content[i] == '\n' {
			offsets = append(offsets, i+1)
		}
	}

	return offsets
}

// PositionToOffset converts a Position to a byte offset in the document.
func (d *Document) PositionToOffset(pos Position) int {
	if d == nil || len(d.Lines) == 0 {
		return 0
	}

	line := int(pos.Line)
	if line >= len(d.Lines) {
		return len(d.Content)
	}

	offset := d.Lines[line] + int(pos.Character)
	if offset > len(d.Content) {
		return len(d.Content)
	}

	return offset
}

// OffsetToPosition converts a byte offset to a Position.
func (d *Document) OffsetToPosition(offset int) Position {
	if d == nil || len(d.Lines) == 0 {
		return Position{}
	}

	if offset < 0 {
		offset = 0
	}
	if offset > len(d.Content) {
		offset = len(d.Content)
	}

	// Binary search for the line
	line := 0
	for i, lineOffset := range d.Lines {
		if lineOffset > offset {
			break
		}
		line = i
	}

	character := offset - d.Lines[line]
	return Position{
		Line:      uint32(line),
		Character: uint32(character),
	}
}

// GetTextBefore returns the text before the given position.
func (d *Document) GetTextBefore(pos Position) string {
	offset := d.PositionToOffset(pos)
	if offset <= 0 {
		return ""
	}
	return d.Content[:offset]
}

// GetTextAfter returns the text after the given position.
func (d *Document) GetTextAfter(pos Position) string {
	offset := d.PositionToOffset(pos)
	if offset >= len(d.Content) {
		return ""
	}
	return d.Content[offset:]
}

// GetLine returns the content of a specific line.
func (d *Document) GetLine(line int) string {
	if d == nil || line < 0 || line >= len(d.Lines) {
		return ""
	}

	start := d.Lines[line]
	end := len(d.Content)

	if line+1 < len(d.Lines) {
		end = d.Lines[line+1] - 1 // Exclude newline
		if end < start {
			end = start
		}
	}

	return d.Content[start:end]
}

// GetWordAtPosition returns the word at the given position and its range.
func (d *Document) GetWordAtPosition(pos Position) (string, Range) {
	offset := d.PositionToOffset(pos)
	if offset >= len(d.Content) {
		return "", Range{Start: pos, End: pos}
	}

	// Find word boundaries
	start := offset
	for start > 0 && isWordChar(d.Content[start-1]) {
		start--
	}

	end := offset
	for end < len(d.Content) && isWordChar(d.Content[end]) {
		end++
	}

	if start == end {
		return "", Range{Start: pos, End: pos}
	}

	return d.Content[start:end], Range{
		Start: d.OffsetToPosition(start),
		End:   d.OffsetToPosition(end),
	}
}

// isWordChar returns true if the character is part of a word.
func isWordChar(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_'
}

// GetTextInRange returns the text within a range.
func (d *Document) GetTextInRange(r Range) string {
	start := d.PositionToOffset(r.Start)
	end := d.PositionToOffset(r.End)
	if start >= end || start >= len(d.Content) {
		return ""
	}
	return d.Content[start:end]
}

// URIToPath converts a file:// URI to a file system path.
func URIToPath(uri string) string {
	const prefix = "file://"
	if strings.HasPrefix(uri, prefix) {
		return uri[len(prefix):]
	}
	return uri
}

// PathToURI converts a file system path to a file:// URI.
func PathToURI(path string) string {
	if strings.HasPrefix(path, "file://") {
		return path
	}
	return "file://" + path
}
