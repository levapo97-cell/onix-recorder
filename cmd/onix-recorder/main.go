// Command onix-recorder — persistencia de OnixGuard (FASE 2).
//
// Consume onix.clean.* de NATS/JetStream y escribe en PostgreSQL (events, tool_calls,
// credentials_detected — solo hash). Expone /healthz y /readyz.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/levapo97-cell/onix-recorder/internal/consumer"
	"github.com/levapo97-cell/onix-recorder/internal/store"
)

var version = "dev"
var ready atomic.Bool

func main() {
	hc := flag.Bool("healthcheck", false, "hace ping a /healthz y termina")
	flag.Parse()
	if *hc {
		os.Exit(runHealthcheck(env("PORT", "8082")))
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	port := env("PORT", "8082")
	natsURL := env("NATS_URL", "nats://nats:4222")
	databaseURL := os.Getenv("DATABASE_URL")
	slog.Info("onix-recorder arrancando", "version", version, "port", port, "nats_url", natsURL, "has_db", databaseURL != "")
	if databaseURL == "" {
		slog.Error("DATABASE_URL es obligatorio para el recorder")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "service": "onix-recorder", "version": version})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ready", "pipeline": ready.Load()})
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	st, err := store.New(ctx, databaseURL)
	if err != nil {
		slog.Error("no se pudo abrir Postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	if err := st.WaitReady(ctx, 30*time.Second); err != nil {
		slog.Error("Postgres no listo", "err", err)
		os.Exit(1)
	}
	cons, err := consumer.Start(ctx, natsURL, st)
	if err != nil {
		slog.Error("no se pudo arrancar el consumidor NATS", "err", err)
		os.Exit(1)
	}
	defer cons.Close()
	ready.Store(true)
	slog.Info("pipeline activo: onix.clean.* → Postgres")

	select {
	case err := <-errCh:
		slog.Error("servidor falló", "err", err)
		os.Exit(1)
	case <-ctx.Done():
		slog.Info("apagando…")
		ready.Store(false)
		sc, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runHealthcheck(port string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
