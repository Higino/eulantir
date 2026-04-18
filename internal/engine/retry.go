package engine

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/higino/eulantir/internal/config"
)

// newBackoff builds an ExponentialBackOff from the pipeline retry config.
func newBackoff(cfg config.RetryConfig) backoff.BackOff {
	b := backoff.NewExponentialBackOff()

	if cfg.InitialInterval != "" {
		if d, err := time.ParseDuration(cfg.InitialInterval); err == nil {
			b.InitialInterval = d
		}
	}
	if cfg.MaxInterval != "" {
		if d, err := time.ParseDuration(cfg.MaxInterval); err == nil {
			b.MaxInterval = d
		}
	}
	if cfg.Multiplier > 0 {
		b.Multiplier = cfg.Multiplier
	}
	if !cfg.Jitter {
		b.RandomizationFactor = 0
	}

	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	return backoff.WithMaxRetries(b, uint64(maxAttempts-1))
}
