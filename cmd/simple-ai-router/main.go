// Command simple-ai-router is an OpenAI-compatible proxy that starts inference
// servers on demand and swaps between them.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/giapnguyen74/simple-ai-router/internal/config"
	"github.com/giapnguyen74/simple-ai-router/internal/router"
	"github.com/giapnguyen74/simple-ai-router/internal/server"
)

const shutdownTimeout = 10 * time.Second

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to config file, .yaml or .json (default: config.yaml, config.yml or config.json)")
	listen := flag.String("listen", "", "listen address (overrides config)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	if err := run(*configPath, *listen); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(configPath, listen string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if listen != "" {
		cfg.Listen = listen
	}

	rt := router.New(cfg, os.Stderr)
	srv, err := server.New(rt)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go rt.RunTTL(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.Listen, "version", version, "models", len(cfg.Models))
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		rt.Shutdown()
		return err
	case <-ctx.Done():
	}

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		slog.Warn("http shutdown", "err", err)
	}
	httpSrv.Close()
	rt.Shutdown()
	return nil
}
