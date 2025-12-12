package commands

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

// NewSeedCommand creates the seed command.
func NewSeedCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "seed",
		Short: "Load seed data from CSV files",
		Long: `Load seed data from CSV files in the seeds directory into the database.

Seeds are typically used for reference data like country codes, status enums,
or small lookup tables that don't change frequently.`,
		Example: `  # Load all seeds
  leapsql seed

  # Load seeds from a specific directory
  leapsql seed --seeds-dir ./data/seeds`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSeed()
		},
	}

	return cmd
}

func runSeed() error {
	cfg := getConfig()

	eng, err := createEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	ctx := context.Background()

	fmt.Printf("Loading seeds from %s...\n", cfg.SeedsDir)
	if err := eng.LoadSeeds(ctx); err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	fmt.Println("Seeds loaded successfully")
	return nil
}
