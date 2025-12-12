// Package main provides the CLI entry point for LeapSQL.
package main

import (
	"os"

	"github.com/leapstack-labs/leapsql/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
