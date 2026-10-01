// Package consumer: consume onix.clean.* de NATS/JetStream y entrega cada CleanEvent al store.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	c "github.com/levapo97-cell/onix-contracts/go/onixcontracts"
)

// Persister es lo que el consumer necesita del store.
type Persister interface {
	RecordClean(ctx context.Context, e c.CleanEvent) error
}

const (
	streamClean = "ONIX_CLEAN"
	subjClean   = "onix.clean.>"
	durable     = "onix-recorder"
)

type Consumer struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

func Start(ctx context.Context, natsURL string, p Persister) (*Consumer, error) {
	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.Name("onix-recorder"))
	if err != nil {
		return nil, fmt.Errorf("conectar NATS: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: streamClean, Subjects: []string{subjClean}, Storage: nats.FileStorage,
	}); err != nil && err != nats.ErrStreamNameAlreadyInUse {
		slog.Warn("AddStream", "err", err)
	}

	handler := func(m *nats.Msg) {
		var e c.CleanEvent
		if err := json.Unmarshal(m.Data, &e); err != nil {
			slog.Error("clean inválido, descartado", "err", err)
			_ = m.Ack()
			return
		}
		if err := p.RecordClean(ctx, e); err != nil {
			slog.Error("no se pudo persistir", "err", err, "session", e.Session)
			_ = m.Nak()
			return
		}
		_ = m.Ack()
		slog.Info("persistido", "session", e.Session, "role", e.AgentRole, "is_error", e.IsError, "is_repetition", e.IsRepetition, "creds", len(e.Credentials))
	}

	sub, err := js.Subscribe(subjClean, handler,
		nats.Durable(durable), nats.ManualAck(), nats.DeliverAll(), nats.AckExplicit())
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("suscribir clean: %w", err)
	}
	slog.Info("recorder activo", "consume", subjClean)
	return &Consumer{nc: nc, sub: sub}, nil
}

func (c *Consumer) Close() {
	if c.sub != nil {
		_ = c.sub.Drain()
	}
	if c.nc != nil {
		c.nc.Drain()
	}
}
