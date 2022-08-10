package http_api

import (
	"net"
	"net/http"
	"strings"

	log "github.com/ericluj/elog"
)

func Serve(listener net.Listener, handler http.Handler) error {
	log.Infof("HTTP: listening on %s", listener.Addr())

	server := &http.Server{
		Handler: handler,
	}

	err := server.Serve(listener)
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Infof("network error: %v", err)
		return err
	}

	log.Infof("HTTP: closing %s", listener.Addr())

	return nil
}
