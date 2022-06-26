package emqd

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	log "github.com/ericluj/elog"
)

func HTTPServer(listener net.Listener) error {
	log.Infof("HTTP: listening on %s", listener.Addr())

	mux := http.NewServeMux()
	mux.Handle("/ping", http.HandlerFunc(pingHandler))
	err := http.Serve(listener, mux)

	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return fmt.Errorf("http.Serve() error - %s", err)
	}

	log.Infof("HTTP: closing %s", listener.Addr())

	return nil
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ping")
}
