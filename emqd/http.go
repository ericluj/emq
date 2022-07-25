package emqd

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HTTPServer struct {
	router http.Handler
}

func newHTTPServer() *HTTPServer {
	s := &HTTPServer{}

	router := gin.Default()
	router.GET("/ping", s.ping)

	s.router = router
	return s
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *HTTPServer) ping(c *gin.Context) {
	c.String(http.StatusOK, "ok")
}
