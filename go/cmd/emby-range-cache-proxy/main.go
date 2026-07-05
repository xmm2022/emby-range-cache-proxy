package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/app"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("emby-range-cache-proxy", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "Path to JSON config file")
	checkConfig := flags.Bool("check-config", false, "Load and validate config, then exit")
	printEffectiveConfig := flags.Bool("print-effective-config", false, "Print effective config as JSON, then exit")
	showSecrets := flags.Bool("show-secrets", false, "Show secrets in --print-effective-config output")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "--config is required")
		return 2
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load config failed: %s\n", err)
		return 1
	}
	if *checkConfig {
		fmt.Fprintln(stdout, "config ok")
		return 0
	}
	if *printEffectiveConfig {
		data, err := config.MarshalEffectiveJSON(cfg, *showSecrets)
		if err != nil {
			fmt.Fprintf(stderr, "print effective config failed: %s\n", err)
			return 1
		}
		_, _ = stdout.Write(data)
		fmt.Fprintln(stdout)
		return 0
	}
	server, err := app.New(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "create server failed: %s\n", err)
		return 1
	}
	defer server.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	server.StartBackground(ctx)

	httpServer := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.ListenHost, cfg.ListenPort),
		Handler:           server,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	fmt.Fprintf(stderr, "emby range cache proxy listening on %s\n", httpServer.Addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(stderr, "listen failed: %s\n", err)
		return 1
	}
	return 0
}
