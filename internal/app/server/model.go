package server

import "crypto/tls"

const (
	LogLevelDebug = "debug"
	LogLevelError = "error"
	LogLevelInfo  = "info"
)

type CertConfig struct {
	CertFile string
	KeyFile  string
}

func (c *CertConfig) Load() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}, nil
}
