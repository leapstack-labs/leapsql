package commands

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	"github.com/leapstack-labs/leapsql/internal/cli/config"
	"github.com/leapstack-labs/leapsql/internal/engine"
	"github.com/leapstack-labs/leapsql/internal/ui"
	"github.com/spf13/cobra"
)

// UIOptions holds options for the ui command.
type UIOptions struct {
	Port      int
	NoBrowser bool
	Watch     bool
}

// NewUICommand creates the ui command.
func NewUICommand() *cobra.Command {
	opts := &UIOptions{}

	cmd := &cobra.Command{
		Use:   "ui",
		Short: "Start the LeapSQL development UI",
		Long: `Start a local web server providing an interactive development UI.

The UI provides:
- Model explorer with SQL preview
- Data preview from target database  
- DAG visualization
- Column-level lineage
- Run history`,
		Example: `  # Start UI on default port
  leapsql ui

  # Start on custom port
  leapsql ui --port 3000

  # Start without auto-opening browser
  leapsql ui --no-browser`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runUI(cmd, opts)
		},
	}

	cmd.Flags().IntVar(&opts.Port, "port", 0, "Port to serve on (default: 8765)")
	cmd.Flags().BoolVar(&opts.NoBrowser, "no-browser", false, "Don't auto-open browser")
	cmd.Flags().BoolVar(&opts.Watch, "watch", true, "Watch for file changes")

	return cmd
}

func runUI(cmd *cobra.Command, opts *UIOptions) error {
	cfg := getConfig()
	logger := config.GetLogger(cmd.Context())

	// Get UI config with defaults
	uiCfg := cfg.GetUIConfig()

	// CLI flags override config file
	port := uiCfg.Port
	if opts.Port != 0 {
		port = opts.Port
	}

	autoOpen := uiCfg.AutoOpen
	if opts.NoBrowser {
		autoOpen = false
	}

	watch := uiCfg.Watch
	if cmd.Flags().Changed("watch") {
		watch = opts.Watch
	}

	// Validate models directory exists
	if _, err := os.Stat(cfg.ModelsDir); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s", cfg.ModelsDir)
	}

	// Create engine for state access and discover
	eng, err := createEngine(cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create engine: %w", err)
	}
	defer func() { _ = eng.Close() }()

	// Auto-run discover
	fmt.Println("Discovering models...")
	if _, err := eng.Discover(engine.DiscoveryOptions{}); err != nil {
		return fmt.Errorf("discover failed: %w", err)
	}

	// Get store from engine
	store := eng.GetStateStore()

	// Create and start UI server
	serverCfg := ui.Config{
		Engine:        eng,
		Store:         store,
		Port:          port,
		Watch:         watch,
		SessionSecret: generateSessionSecret(),
		Logger:        logger,
		ModelsDir:     cfg.ModelsDir,
	}

	server := ui.NewServer(serverCfg)

	// Open browser if configured
	if autoOpen {
		url := fmt.Sprintf("http://localhost:%d", port)
		go openBrowser(url)
	}

	fmt.Printf("Starting UI server on http://localhost:%d\n", port)
	fmt.Println("Press Ctrl+C to stop")

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	return server.Serve(ctx)
}

// generateSessionSecret generates a simple session secret.
// In production, this should be loaded from config or environment.
func generateSessionSecret() string {
	// For now, use a fixed secret. In production, load from env/config.
	secret := os.Getenv("LEAPSQL_SESSION_SECRET")
	if secret == "" {
		// Default secret for development (nolint:gosec)
		secret = "leapsql-dev-secret-change-in-production" //nolint:gosec
	}
	return secret
}

// openBrowser opens the default browser to the specified URL.
func openBrowser(url string) {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url) //nolint:noctx
	case "linux":
		cmd = exec.Command("xdg-open", url) //nolint:noctx
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url) //nolint:noctx
	default:
		return
	}

	_ = cmd.Start()
}
