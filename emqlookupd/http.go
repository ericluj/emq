package emqlookupd

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HTTPServer struct {
	router     http.Handler
	emqlookupd *EMQLookupd
}

func newHTTPServer(l *EMQLookupd) *HTTPServer {
	s := &HTTPServer{
		emqlookupd: l,
	}

	router := gin.Default()
	router.GET("/ping", s.ping)
	router.GET("/lookup", s.lookup)
	router.GET("/topics", s.topics)
	router.GET("/channels", s.channels)

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

// 查询topic下所有数据
func (s *HTTPServer) lookup(c *gin.Context) {
	topicName := c.Param("topic")
	if topicName == "" {
		c.JSON(http.StatusBadRequest, NewErrResp("INVALID_REQUEST"))
		return
	}

	registrations := s.emqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registrations) == 0 {
		c.JSON(http.StatusNotFound, NewErrResp("TOPIC_NOT_FOUND"))
		return
	}

	channels := s.emqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.emqlookupd.DB.FindProducers("topic", topicName, "")

	c.JSON(http.StatusOK, map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	})
}

func (s *HTTPServer) topics(c *gin.Context) {
	topics := s.emqlookupd.DB.FindProducers("topic", "*", "")

	c.JSON(http.StatusOK, map[string]interface{}{
		"topics": topics,
	})
}

func (s *HTTPServer) channels(c *gin.Context) {
	topicName := c.Param("topic")
	if topicName == "" {
		c.JSON(http.StatusBadRequest, NewErrResp("INVALID_REQUEST"))
		return
	}

	channels := s.emqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()

	c.JSON(http.StatusOK, map[string]interface{}{
		"channels": channels,
	})
}
