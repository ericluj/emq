package emqd

import (
	"net/http"
	"sync/atomic"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
)

type HTTPServer struct {
	emqd   *EMQD
	router http.Handler
}

func newHTTPServer(emqd *EMQD) *HTTPServer {
	s := &HTTPServer{
		emqd: emqd,
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	pprof.Register(router) // 性能
	router.GET("/ping", s.ping)
	router.GET("/stats", s.stats)

	s.router = router
	return s
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *HTTPServer) ping(c *gin.Context) {
	c.String(http.StatusOK, "ok")
}

type Resp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func ErrResp(message string) *Resp {
	return &Resp{
		Code:    -1,
		Message: message,
	}
}

func (s *HTTPServer) stats(c *gin.Context) {
	topicName := c.Query("topic")
	if topicName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_TOPIC"))
		return
	}

	topic := s.emqd.GetTopic(topicName)
	topic.mtx.RLock()
	defer topic.mtx.RUnlock()

	countMap := make(map[string]int64)
	for _, c := range topic.channelMap {
		countMap[c.name] = atomic.LoadInt64(&c.messageCount)
	}

	c.JSON(http.StatusOK, map[string]interface{}{
		"channels": countMap,
	})
}
