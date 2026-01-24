package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/timescale/tigerfs/internal/tigerfs/cmd"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	"go.uber.org/zap"
)

func main() {
	if err := run(); err != nil {
		if exitErr, ok := err.(interface{ ExitCode() int }); ok {
			os.Exit(exitErr.ExitCode())
		}
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := notifyContext(context.Background())
	defer cancel()

	err := cmd.Execute(ctx)
	return err
}

func notifyContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-sigCh:
			logging.Info("Received signal, shutting down gracefully", zap.String("signal", sig.String()))
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(sigCh)
		cancel()
	}
}
