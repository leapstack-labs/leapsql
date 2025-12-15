package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewVersionCommand creates the version command.
func NewVersionCommand(version string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Long:  `Display LeapSQL version and build information.`,
		Run: func(cmd *cobra.Command, _ []string) {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "LeapSQL v%s\n", version)
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Data Transformation Engine built with Go and DuckDB")
		},
	}
}
