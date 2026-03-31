package service

import "time"

const (
	DefaultTimeout   = 15 * time.Second
	FetchUserTimeout = 60 * time.Second
	MaxRecvMsgSize   = 16 * 1024 * 1024 // 16MB
)

type Service interface {
	Start() error
	Close() error
}

type Config struct {
	NodeID                int
	FetchUserInterval     time.Duration
	ReportTrafficInterval time.Duration
	HeartBeatInterval     time.Duration
}

type RawResult struct {
	Data []byte
	Err  error
}
