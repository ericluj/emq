package emqlookupd

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HTTPServer struct {
	router     http.Handler
	emqlookupd *EMQLookupd
}

func newHTTPServer() *HTTPServer {
	s := &HTTPServer{}

	router := gin.Default()
	router.GET("/ping", s.ping)
	router.GET("/lookup", s.lookup)

	s.router = router
	return s
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *HTTPServer) ping(c *gin.Context) {
	c.String(http.StatusOK, "ok")
}

type ErrResp struct {
	Message string `json:"mesage"`
}

func NewErrResp(message string) *ErrResp {
	return &ErrResp{
		Message: message,
	}
}

func (s *HTTPServer) lookup(c *gin.Context) {
	topicName := c.Param("topic")
	if topicName == "" {
		c.JSON(http.StatusBadRequest, NewErrResp("INVALID_REQUEST"))
		return
	}

	registration := s.emqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {
		c.JSON(http.StatusNotFound, NewErrResp("TOPIC_NOT_FOUND"))
		return
	}

	// TODO:
}
