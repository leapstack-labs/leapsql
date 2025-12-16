package config

import (
	"os"
	"path/filepath"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// ConfigFileName is the name of the config file.
const ConfigFileName = "leapsql.yaml"

// ConfigFileNameAlt is the alternate name of the config file.
const ConfigFileNameAlt = "leapsql.yml"

// LoadFromDir loads a ProjectConfig from the given directory.
// It looks for leapsql.yaml or leapsql.yml in the directory.
// Returns nil, nil if no config file is found (not an error condition).
func LoadFromDir(dir string) (*ProjectConfig, error) {
	// Find config file
	configPath := findConfigFile(dir)
	if configPath == "" {
		return nil, nil
	}

	// Load with koanf
	k := koanf.New(".")
	if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
		return nil, err
	}

	// Unmarshal into ProjectConfig
	var cfg ProjectConfig
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, err
	}

	// Apply defaults
	cfg.ApplyDefaults()
	if cfg.Target != nil {
		cfg.Target.ApplyDefaults()
	}

	return &cfg, nil
}

// findConfigFile finds the config file in the given directory.
// Returns empty string if not found.
func findConfigFile(dir string) string {
	yamlPath := filepath.Join(dir, ConfigFileName)
	if _, err := os.Stat(yamlPath); err == nil {
		return yamlPath
	}

	ymlPath := filepath.Join(dir, ConfigFileNameAlt)
	if _, err := os.Stat(ymlPath); err == nil {
		return ymlPath
	}

	return ""
}

// FindProjectRoot walks up from the given directory to find a directory
// containing leapsql.yaml or leapsql.yml.
// Returns empty string if not found.
func FindProjectRoot(startDir string) string {
	dir := startDir
	for {
		if findConfigFile(dir) != "" {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached filesystem root
			return ""
		}
		dir = parent
	}
}
