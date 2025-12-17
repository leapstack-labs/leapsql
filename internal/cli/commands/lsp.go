package commands

import (
	"os"

	"github.com/leapstack-labs/leapsql/internal/cli/config"
	"github.com/leapstack-labs/leapsql/internal/lsp"
	"github.com/spf13/cobra"
)

// NewLSPCommand creates the lsp command.
func NewLSPCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lsp",
		Short: "Start the Language Server Protocol server",
		Long: `Start the LSP server for IDE integration.

The server communicates over stdin/stdout using JSON-RPC.
The project root and state database are determined by the
client's initialization request (rootUri parameter).`,
		Example: `  # Start LSP server (usually called by an IDE)
  leapsql lsp`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runLSP(cmd)
		},
	}

	return cmd
}

func runLSP(cmd *cobra.Command) error {
	logger := config.GetLogger(cmd.Context())
	server := lsp.NewServerWithLogger(os.Stdin, os.Stdout, logger)
	return server.Run()
}
