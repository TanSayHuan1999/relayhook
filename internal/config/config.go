// Package config loads relayhook configuration from environment variables.
package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for the service.
type Config struct {
	DatabaseURL    string
	Port           int
	SigningSecret  string
	MaxAttempts    int
	BackoffBase    time.Duration
	PollInterval   time.Duration
	DeliverTimeout time.Duration
}

// Load reads configuration from the environment, applying defaults.
// DATABASE_URL is required; everything else has a sane default.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		Port:           envInt("PORT", 8080),
		SigningSecret:  os.Getenv("RELAYHOOK_SIGNING_SECRET"),
		MaxAttempts:    envInt("RELAYHOOK_MAX_ATTEMPTS", 5),
		PollInterval:   750 * time.Millisecond,
		DeliverTimeout: 5 * time.Second,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.SigningSecret == "" {
		cfg.SigningSecret = "dev-secret-change-me"
		log.Printf("WARNING: RELAYHOOK_SIGNING_SECRET not set, using insecure dev default")
	}
	baseSecs := envInt("RELAYHOOK_BACKOFF_BASE_SECONDS", 2)
	if baseSecs < 0 {
		baseSecs = 0
	}
	cfg.BackoffBase = time.Duration(baseSecs) * time.Second
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("invalid PORT %d", cfg.Port)
	}
	return cfg, nil
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("WARNING: invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}
