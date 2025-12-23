package lsp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/leapstack-labs/leapsql/internal/config"
	"github.com/leapstack-labs/leapsql/internal/provider"
	"github.com/leapstack-labs/leapsql/internal/state"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules" // Register project rules

	// Import dialect implementations so they register themselves
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
	_ "github.com/leapstack-labs/leapsql/pkg/dialects/ansi"
)

// Server implements the Language Server Protocol for LeapSQL.
type Server struct {
	// Document management
	documents *DocumentStore

	// Shared provider for all parsing and context (eliminates redundant parsing)
	provider *provider.Provider

	// Project context
	projectRoot string
	initialized bool

	// State store (may be nil if discover not run)
	store state.Store

	// Memory caches for fast lookups
	macroNamespaceCache map[string]bool
	modelNameCache      map[string]bool
	cacheMu             sync.RWMutex

	// SQL dialect for validation (never nil after initialization)
	dialect           *dialect.Dialect
	dialectFromConfig bool // true if dialect was loaded from config, false if ANSI default

	// Project health analyzer for DAG/architecture linting
	projectAnalyzer *project.Analyzer
	projectConfig   lint.ProjectHealthConfig

	// I/O
	reader  *bufio.Reader
	writer  io.Writer
	writeMu sync.Mutex

	// Logging
	logger *slog.Logger

	// Shutdown state
	shutdown   bool
	shutdownMu sync.RWMutex
}

// NewServer creates a new LSP server instance.
func NewServer(reader io.Reader, writer io.Writer) *Server {
	return NewServerWithLogger(reader, writer, nil)
}

// NewServerWithLogger creates a new LSP server instance with a custom logger.
func NewServerWithLogger(reader io.Reader, writer io.Writer, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	return &Server{
		documents:           NewDocumentStore(),
		reader:              bufio.NewReader(reader),
		writer:              writer,
		logger:              logger,
		macroNamespaceCache: make(map[string]bool),
		modelNameCache:      make(map[string]bool),
		projectAnalyzer:     project.NewAnalyzer(nil),
		projectConfig:       lint.DefaultProjectHealthConfig(),
	}
}

// Run starts the server's main loop, processing JSON-RPC messages.
func (s *Server) Run() error {
	s.logger.Info("LeapSQL LSP server starting...")

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
			if errors.Is(err, io.EOF) {
				s.logger.Info("Client disconnected")
				return nil
			}
			s.logger.Error("Error reading message", "error", err)
			continue
		}

		// Handle message
		if err := s.handleMessage(msg); err != nil {
			s.logger.Error("Error handling message", "error", err)
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
		s.logger.Error("Error marshaling message", "error", err)
		return
	}

	header := fmt.Sprintf("Content-Length: %d\r\n\r\n", len(body))
	_, _ = s.writer.Write([]byte(header))
	_, _ = s.writer.Write(body)
}

// handleMessage dispatches a message to the appropriate handler.
func (s *Server) handleMessage(msg *JSONRPCMessage) error {
	s.logger.Info("Received", "method", msg.Method)

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
	case "textDocument/codeAction":
		return s.handleCodeAction(msg)
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
	s.logger.Info("Project root", "path", s.projectRoot)

	// Try to open SQLite database
	dbPath := filepath.Join(s.projectRoot, ".leapsql", "state.db")
	store := state.NewSQLiteStore(s.logger)
	if err := store.Open(dbPath); err != nil {
		s.logger.Info("SQLite database not found", "path", dbPath, "error", err)
		s.store = nil
	} else {
		s.store = store
		s.loadCaches()
	}

	// Load dialect from project config
	s.loadDialectFromConfig()

	// Initialize the shared provider for parsing and context
	s.provider = provider.New(s.store, s.dialect, s.projectConfig, s.logger)

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
			CodeActionProvider: &CodeActionOptions{
				CodeActionKinds: []CodeActionKind{CodeActionKindQuickFix},
			},
		},
	}

	s.sendResponse(msg.ID, result, nil)
	return nil
}

func (s *Server) handleInitialized(_ *JSONRPCMessage) error {
	s.initialized = true
	s.logger.Info("Server initialized")

	// Show warning if store not available
	if s.store == nil {
		s.sendNotification("window/showMessage", &ShowMessageParams{
			Type:    MessageTypeWarning,
			Message: "SQLite database not found. Run 'leapsql discover' to enable full IDE features.",
		})
	} else {
		// Run project health diagnostics on startup if store is available
		s.publishProjectHealthDiagnostics()
	}

	// Show info if using default ANSI dialect
	if !s.dialectFromConfig {
		s.sendNotification("window/showMessage", &ShowMessageParams{
			Type:    MessageTypeInfo,
			Message: "Using ANSI SQL dialect. Configure 'target' in leapsql.yaml for dialect-specific features.",
		})
	}

	return nil
}

func (s *Server) handleShutdown(msg *JSONRPCMessage) error {
	s.shutdownMu.Lock()
	s.shutdown = true
	s.shutdownMu.Unlock()

	if s.store != nil {
		_ = s.store.Close()
	}

	s.sendResponse(msg.ID, nil, nil)
	s.logger.Info("Server shutdown")
	return nil
}

func (s *Server) handleExit(_ *JSONRPCMessage) error {
	s.logger.Info("Server exit")
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
	s.logger.Info("Opened", "uri", params.TextDocument.URI)

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
	s.logger.Info("Closed", "uri", params.TextDocument.URI)

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
	s.logger.Info("Saved", "path", path)

	// If it's a .star file, re-index it
	if strings.HasSuffix(path, ".star") && s.store != nil {
		s.reindexMacroFile(path)
	}

	// If it's a .sql file, re-run project health diagnostics
	// Project health rules may be affected by model changes
	if strings.HasSuffix(path, ".sql") && s.store != nil {
		s.publishProjectHealthDiagnostics()
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

	s.logger.Info("Loaded caches", "macro_namespaces", len(s.macroNamespaceCache), "model_refs", len(s.modelNameCache))
}

// reindexMacroFile re-parses and stores a macro file.
func (s *Server) reindexMacroFile(path string) {
	// This would call macro.ParseStarlarkFile and store in SQLite
	// For now, just log
	s.logger.Info("TODO: Re-index macro file", "path", path)
}

// loadDialectFromConfig loads the dialect from the project's leapsql.yaml config.
// Defaults to ANSI if no config or target is specified.
func (s *Server) loadDialectFromConfig() {
	// Try to load from config
	if s.projectRoot != "" {
		cfg, err := config.LoadFromDir(s.projectRoot)
		if err == nil && cfg != nil && cfg.Target != nil && cfg.Target.Type != "" {
			if d, ok := dialect.Get(cfg.Target.Type); ok {
				s.dialect = d
				s.dialectFromConfig = true
				s.logger.Info("Loaded dialect from project config", "dialect", cfg.Target.Type)
				return
			}
			s.logger.Warn("Unknown dialect type in project config", "type", cfg.Target.Type)
		}
	}

	// Default to ANSI
	s.dialect, _ = dialect.Get("ansi")
	s.dialectFromConfig = false
	s.logger.Info("No target configured, defaulting to ANSI dialect")
}

// buildProjectContext delegates to the provider for project context.
// Falls back to building directly from store if provider is not available.
func (s *Server) buildProjectContext() *project.Context {
	if s.provider != nil {
		return s.provider.GetProjectContext()
	}
	// Fallback: should not happen after initialization
	return nil
}
