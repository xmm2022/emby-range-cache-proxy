package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/app"
	"github.com/xmm2022/emby-range-cache-proxy/go/internal/config"
)

func main() {
	configPath := flag.String("config", "", "Path to JSON config file")
	flag.Parse()
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "--config is required")
		os.Exit(2)
	}
	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("load config failed: %s", err)
	}
	server, err := app.New(cfg)
	if err != nil {
		log.Fatalf("create server failed: %s", err)
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
	log.Printf("emby range cache proxy listening on %s", httpServer.Addr)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen failed: %s", err)
	}
}
