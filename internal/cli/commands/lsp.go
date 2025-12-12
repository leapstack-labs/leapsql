package commands

import (
	"os"

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
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLSP()
		},
	}

	return cmd
}

func runLSP() error {
	server := lsp.NewServer(os.Stdin, os.Stdout)
	return server.Run()
}
