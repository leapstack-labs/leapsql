package lsp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/leapstack-labs/leapsql/pkg/lineage"
)

// Server implements the Language Server Protocol for LeapSQL.
type Server struct {
	// Document management
	documents *DocumentStore

	// Project context
	projectRoot string
	initialized bool

	// State store (may be nil if discover not run)
	store state.StateStore

	// Memory caches for fast lookups
	macroNamespaceCache map[string]bool
	modelNameCache      map[string]bool
	cacheMu             sync.RWMutex

	// SQL dialect for validation
	dialect *lineage.Dialect

	// I/O
	reader  *bufio.Reader
	writer  io.Writer
	writeMu sync.Mutex

	// Logging
	logger *log.Logger

	// Shutdown state
	shutdown   bool
	shutdownMu sync.RWMutex
}

// NewServer creates a new LSP server instance.
func NewServer(reader io.Reader, writer io.Writer) *Server {
	return &Server{
		documents:           NewDocumentStore(),
		reader:              bufio.NewReader(reader),
		writer:              writer,
		logger:              log.New(os.Stderr, "[lsp] ", log.LstdFlags),
		macroNamespaceCache: make(map[string]bool),
		modelNameCache:      make(map[string]bool),
	}
}

// Run starts the server's main loop, processing JSON-RPC messages.
func (s *Server) Run() error {
	s.logger.Println("LeapSQL LSP server starting...")

	for {
		s.shutdownMu.RLock()
		if s.shutdown {
			s.shutdownMu.RUnlock()
			return nil
		}
		s.shutdownMu.RUnlock()

		// Read message
		msg, err := s.readMessage()
		if err != nil {
			if err == io.EOF {
				s.logger.Println("Client disconnected")
				return nil
			}
			s.logger.Printf("Error reading message: %v", err)
			continue
		}

		// Handle message
		if err := s.handleMessage(msg); err != nil {
			s.logger.Printf("Error handling message: %v", err)
		}
	}
}

// JSONRPCMessage represents a JSON-RPC 2.0 message.
type JSONRPCMessage struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method,omitempty"`
	Params  json.RawMessage  `json:"params,omitempty"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *JSONRPCError    `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC error.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// readMessage reads a JSON-RPC message from the input stream.
func (s *Server) readMessage() (*JSONRPCMessage, error) {
	// Read headers
	var contentLength int
	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			break // End of headers
		}

		if strings.HasPrefix(line, "Content-Length: ") {
			lengthStr := strings.TrimPrefix(line, "Content-Length: ")
			contentLength, err = strconv.Atoi(lengthStr)
			if err != nil {
				return nil, fmt.Errorf("invalid Content-Length: %w", err)
			}
		}
	}

	if contentLength == 0 {
		return nil, fmt.Errorf("missing Content-Length header")
	}

	// Read body
	body := make([]byte, contentLength)
	_, err := io.ReadFull(s.reader, body)
	if err != nil {
		return nil, fmt.Errorf("error reading body: %w", err)
	}

	// Parse message
	var msg JSONRPCMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, fmt.Errorf("error parsing message: %w", err)
	}

	return &msg, nil
}

// sendResponse sends a JSON-RPC response.
func (s *Server) sendResponse(id *json.RawMessage, result any, err *JSONRPCError) {
	msg := JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      id,
	}

	if err != nil {
		msg.Error = err
	} else {
		resultBytes, _ := json.Marshal(result)
		msg.Result = resultBytes
	}

	s.writeMessage(&msg)
}

// sendNotification sends a JSON-RPC notification (no ID).
func (s *Server) sendNotification(method string, params any) {
	msg := JSONRPCMessage{
		JSONRPC: "2.0",
		Method:  method,
	}

	if params != nil {
		paramsBytes, _ := json.Marshal(params)
		msg.Params = paramsBytes
	}

	s.writeMessage(&msg)
}

// writeMessage writes a JSON-RPC message to the output stream.
func (s *Server) writeMessage(msg *JSONRPCMessage) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	body, err := json.Marshal(msg)
	if err != nil {
		s.logger.Printf("Error marshaling message: %v", err)
		return
	}

	header := fmt.Sprintf("Content-Length: %d\r\n\r\n", len(body))
	s.writer.Write([]byte(header))
	s.writer.Write(body)
}

// handleMessage dispatches a message to the appropriate handler.
func (s *Server) handleMessage(msg *JSONRPCMessage) error {
	s.logger.Printf("Received: %s", msg.Method)

	switch msg.Method {
	case "initialize":
		return s.handleInitialize(msg)
	case "initialized":
		return s.handleInitialized(msg)
	case "shutdown":
		return s.handleShutdown(msg)
	case "exit":
		return s.handleExit(msg)
	case "textDocument/didOpen":
		return s.handleDidOpen(msg)
	case "textDocument/didClose":
		return s.handleDidClose(msg)
	case "textDocument/didChange":
		return s.handleDidChange(msg)
	case "textDocument/didSave":
		return s.handleDidSave(msg)
	case "textDocument/completion":
		return s.handleCompletion(msg)
	case "textDocument/hover":
		return s.handleHover(msg)
	case "textDocument/definition":
		return s.handleDefinition(msg)
	default:
		if msg.ID != nil {
			// Unknown method with ID - respond with method not found
			s.sendResponse(msg.ID, nil, &JSONRPCError{
				Code:    -32601,
				Message: "Method not found: " + msg.Method,
			})
		}
		return nil
	}
}

// --- Lifecycle handlers ---

func (s *Server) handleInitialize(msg *JSONRPCMessage) error {
	var params InitializeParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		s.sendResponse(msg.ID, nil, &JSONRPCError{Code: -32602, Message: err.Error()})
		return err
	}

	s.projectRoot = URIToPath(params.RootURI)
	s.logger.Printf("Project root: %s", s.projectRoot)

	// Try to open SQLite database
	dbPath := filepath.Join(s.projectRoot, ".leapsql", "state.db")
	store := state.NewSQLiteStore()
	if err := store.Open(dbPath); err != nil {
		s.logger.Printf("SQLite database not found at %s: %v", dbPath, err)
		s.store = nil
	} else {
		s.store = store
		s.loadCaches()
	}

	// Get dialect (default to duckdb)
	s.dialect, _ = lineage.GetDialect("duckdb")

	result := InitializeResult{
		Capabilities: ServerCapabilities{
			TextDocumentSync: &TextDocumentSyncOptions{
				OpenClose: true,
				Change:    TextDocumentSyncKindFull,
				Save: &SaveOptions{
					IncludeText: true,
				},
			},
			CompletionProvider: &CompletionOptions{
				TriggerCharacters: []string{".", " ", "(", "{", "'", "\""},
			},
			HoverProvider:      true,
			DefinitionProvider: true,
		},
	}

	s.sendResponse(msg.ID, result, nil)
	return nil
}

func (s *Server) handleInitialized(msg *JSONRPCMessage) error {
	s.initialized = true
	s.logger.Println("Server initialized")

	// Show warning if store not available
	if s.store == nil {
		s.sendNotification("window/showMessage", &ShowMessageParams{
			Type:    MessageTypeWarning,
			Message: "SQLite database not found. Run 'leapsql discover' to enable full IDE features.",
		})
	}

	return nil
}

func (s *Server) handleShutdown(msg *JSONRPCMessage) error {
	s.shutdownMu.Lock()
	s.shutdown = true
	s.shutdownMu.Unlock()

	if s.store != nil {
		s.store.Close()
	}

	s.sendResponse(msg.ID, nil, nil)
	s.logger.Println("Server shutdown")
	return nil
}

func (s *Server) handleExit(msg *JSONRPCMessage) error {
	s.logger.Println("Server exit")
	os.Exit(0)
	return nil
}

// --- Document handlers ---

func (s *Server) handleDidOpen(msg *JSONRPCMessage) error {
	var params DidOpenTextDocumentParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return err
	}

	s.documents.Open(params.TextDocument.URI, params.TextDocument.Text, params.TextDocument.Version)
	s.logger.Printf("Opened: %s", params.TextDocument.URI)

	// Run diagnostics
	s.publishDiagnostics(params.TextDocument.URI)

	return nil
}

func (s *Server) handleDidClose(msg *JSONRPCMessage) error {
	var params DidCloseTextDocumentParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return err
	}

	s.documents.Close(params.TextDocument.URI)
	s.logger.Printf("Closed: %s", params.TextDocument.URI)

	// Clear diagnostics
	s.sendNotification("textDocument/publishDiagnostics", &PublishDiagnosticsParams{
		URI:         params.TextDocument.URI,
		Diagnostics: []Diagnostic{},
	})

	return nil
}

func (s *Server) handleDidChange(msg *JSONRPCMessage) error {
	var params DidChangeTextDocumentParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return err
	}

	// We use full sync, so take the last change
	if len(params.ContentChanges) > 0 {
		lastChange := params.ContentChanges[len(params.ContentChanges)-1]
		s.documents.Update(params.TextDocument.URI, lastChange.Text, params.TextDocument.Version)
	}

	// Run diagnostics
	s.publishDiagnostics(params.TextDocument.URI)

	return nil
}

func (s *Server) handleDidSave(msg *JSONRPCMessage) error {
	var params DidSaveTextDocumentParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return err
	}

	path := URIToPath(params.TextDocument.URI)
	s.logger.Printf("Saved: %s", path)

	// If it's a .star file, re-index it
	if strings.HasSuffix(path, ".star") && s.store != nil {
		s.reindexMacroFile(path)
	}

	return nil
}

// --- Feature handlers (stubs for now) ---

func (s *Server) handleCompletion(msg *JSONRPCMessage) error {
	var params CompletionParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		s.sendResponse(msg.ID, nil, &JSONRPCError{Code: -32602, Message: err.Error()})
		return err
	}

	items := s.getCompletions(params)
	s.sendResponse(msg.ID, &CompletionList{Items: items}, nil)
	return nil
}

func (s *Server) handleHover(msg *JSONRPCMessage) error {
	var params HoverParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		s.sendResponse(msg.ID, nil, &JSONRPCError{Code: -32602, Message: err.Error()})
		return err
	}

	hover := s.getHover(params)
	s.sendResponse(msg.ID, hover, nil)
	return nil
}

func (s *Server) handleDefinition(msg *JSONRPCMessage) error {
	var params DefinitionParams
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		s.sendResponse(msg.ID, nil, &JSONRPCError{Code: -32602, Message: err.Error()})
		return err
	}

	location := s.getDefinition(params)
	s.sendResponse(msg.ID, location, nil)
	return nil
}

// --- Helper methods ---

// loadCaches loads macro and model names into memory for fast lookups.
func (s *Server) loadCaches() {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	// Load macro namespaces
	namespaces, _ := s.store.GetMacroNamespaces()
	s.macroNamespaceCache = make(map[string]bool)
	for _, ns := range namespaces {
		s.macroNamespaceCache[ns.Name] = true
	}

	// Load model names
	models, _ := s.store.ListModels()
	s.modelNameCache = make(map[string]bool)
	for _, m := range models {
		s.modelNameCache[m.Name] = true
		s.modelNameCache[m.Path] = true
	}

	s.logger.Printf("Loaded %d macro namespaces, %d model references", len(s.macroNamespaceCache), len(s.modelNameCache))
}

// reindexMacroFile re-parses and stores a macro file.
func (s *Server) reindexMacroFile(path string) {
	// This would call macro.ParseStarlarkFile and store in SQLite
	// For now, just log
	s.logger.Printf("TODO: Re-index macro file: %s", path)
}
