package service

import "time"

const (
	DefaultTimeout = 15 * time.Second
)

type Service interface {
	Start() error
	Close() error
}

type Config struct {
	NodeID                int
	FetchUserInterval     time.Duration
	ReportTrafficInterval time.Duration
	HeartbeatInterval     time.Duration
	RegisterID            string
}
