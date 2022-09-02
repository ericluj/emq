package emqd

import (
	"net/http"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
)

type HTTPServer struct {
	router http.Handler
}

func newHTTPServer() *HTTPServer {
	s := &HTTPServer{}

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	pprof.Register(router) // 性能
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
