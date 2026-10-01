// Package store persiste CleanEvent en PostgreSQL. FASE 2: consume el evento YA redactado
// (sin valores de credenciales) y guarda events + tool_calls + credentials_detected (solo hash).
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	c "github.com/levapo97-cell/onix-contracts/go/onixcontracts"
)

type Store struct{ pool *pgxpool.Pool }

func New(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 5
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("conectar postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close()                        { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// RecordClean auto-registra project/agent/session e inserta el evento limpio, su tool_call
// y sus credenciales detectadas (SOLO el hash; el valor real ya no existe en el evento).
func (s *Store) RecordClean(ctx context.Context, e c.CleanEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	projectID, err := ensureProject(ctx, tx, e.Project)
	if err != nil {
		return fmt.Errorf("ensureProject: %w", err)
	}
	agentID, err := ensureAgent(ctx, tx, projectID, string(e.AgentRole))
	if err != nil {
		return fmt.Errorf("ensureAgent: %w", err)
	}
	sessionID, err := ensureSession(ctx, tx, projectID, agentID, e.Session)
	if err != nil {
		return fmt.Errorf("ensureSession: %w", err)
	}

	if string(e.Hook) == "SessionStart" {
		return tx.Commit(ctx) // solo registrar la sesión
	}

	evType := classify(e)
	var durationMs, exitCode *int64
	if e.Result != nil {
		durationMs = e.Result.DurationMS
		exitCode = e.Result.ExitCode
	}

	var eventID string
	err = tx.QueryRow(ctx, `
		INSERT INTO events (session_id, project_id, agent_id, ts, type, tool, summary, duration_ms, tokens, cost_usd, is_error, is_repetition)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id`,
		sessionID, projectID, agentID, e.Ts, evType, nullStr(e.Tool), summary(e), durationMs, e.Tokens, e.CostUsd, e.IsError, e.IsRepetition,
	).Scan(&eventID)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	// tool_call con los params YA normalizados/redactados y su hash.
	if e.Tool != nil && *e.Tool != "" {
		paramsJSON, _ := json.Marshal(e.ParamsNormalized)
		if _, err := tx.Exec(ctx, `
			INSERT INTO tool_calls (event_id, tool, params_normalized, params_hash, exit_code, duration_ms)
			VALUES ($1,$2,$3::jsonb,$4,$5,$6)`,
			eventID, *e.Tool, string(paramsJSON), nullStr(&e.ParamsHash), exitCode, durationMs,
		); err != nil {
			return fmt.Errorf("insert tool_call: %w", err)
		}
	}

	// Credenciales detectadas: SOLO el hash. El valor real nunca llega aquí (lo removió onix-guard).
	for _, cred := range e.Credentials {
		if _, err := tx.Exec(ctx, `
			INSERT INTO credentials_detected (event_id, kind, sha256, label)
			VALUES ($1,$2,$3,$4)`,
			eventID, string(cred.Kind), cred.Sha256, cred.Label,
		); err != nil {
			return fmt.Errorf("insert credential: %w", err)
		}
	}

	return tx.Commit(ctx)
}

func classify(e c.CleanEvent) string {
	if e.IsError {
		return "error"
	}
	if e.IsRepetition {
		return "repeticion"
	}
	if len(e.Credentials) > 0 {
		return "credencial"
	}
	if e.Tool != nil && *e.Tool != "" {
		return "tool"
	}
	return "nota"
}

func summary(e c.CleanEvent) string {
	if e.Tool != nil && *e.Tool != "" {
		return *e.Tool
	}
	return string(e.Hook)
}

func ensureProject(ctx context.Context, tx pgx.Tx, name string) (string, error) {
	if name == "" {
		name = "desconocido"
	}
	var id string
	if err := tx.QueryRow(ctx, `SELECT id FROM projects WHERE name=$1 LIMIT 1`, name).Scan(&id); err == nil {
		return id, nil
	}
	err := tx.QueryRow(ctx, `INSERT INTO projects (name) VALUES ($1) RETURNING id`, name).Scan(&id)
	return id, err
}

func ensureAgent(ctx context.Context, tx pgx.Tx, projectID, role string) (string, error) {
	if role == "" {
		role = "fullstack"
	}
	var id string
	err := tx.QueryRow(ctx,
		`INSERT INTO agents (project_id, role) VALUES ($1,$2)
		 ON CONFLICT (project_id, role) DO UPDATE SET role=EXCLUDED.role RETURNING id`,
		projectID, role).Scan(&id)
	return id, err
}

func ensureSession(ctx context.Context, tx pgx.Tx, projectID, agentID, claudeSessionID string) (string, error) {
	var id string
	err := tx.QueryRow(ctx,
		`INSERT INTO sessions (project_id, agent_id, claude_session_id) VALUES ($1,$2,$3)
		 ON CONFLICT (claude_session_id) DO UPDATE SET status='activa' RETURNING id`,
		projectID, agentID, claudeSessionID).Scan(&id)
	return id, err
}

func nullStr(p *string) any {
	if p == nil || *p == "" {
		return nil
	}
	return *p
}

func (s *Store) WaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := s.Ping(ctx); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres no respondió en %s", timeout)
		}
		time.Sleep(time.Second)
	}
}
