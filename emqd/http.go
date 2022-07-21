package emqd

import (
	"fmt"
	"net/http"
)

type HTTPServer struct {
	mux http.Handler
}

func newHTTPServer() *HTTPServer {
	mux := http.NewServeMux()
	mux.Handle("/ping", http.HandlerFunc(pingHandler))
	s := &HTTPServer{
		mux: mux,
	}
	return s
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.mux.ServeHTTP(w, req)
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ping")
}
