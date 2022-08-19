package emqlookupd

import (
	"net/http"

	log "github.com/ericluj/elog"
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

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/ping", s.ping)
	router.GET("/lookup", s.lookup)
	router.GET("/topics", s.topics)
	router.GET("/channels", s.channels)
	router.GET("/nodes", s.nodes)

	router.POST("/topic/create", s.createTopic)
	router.POST("/topic/delete", s.deleteTopic)
	router.POST("/channel/create", s.createChannel)
	router.POST("/channel/delete", s.deleteChannel)

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

func SucResp() *Resp {
	return &Resp{
		Code: 0,
	}
}

func ErrResp(message string) *Resp {
	return &Resp{
		Code:    -1,
		Message: message,
	}
}

// 查询topic下所有数据
func (s *HTTPServer) lookup(c *gin.Context) {
	topicName := c.Query("topic")
	if topicName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_TOPIC"))
		return
	}

	registrations := s.emqlookupd.DB.FindRegistrations(CatagoryTopic, topicName, "")
	if len(registrations) == 0 {
		c.JSON(http.StatusNotFound, ErrResp("TOPIC_NOT_FOUND"))
		return
	}

	channels := s.emqlookupd.DB.FindRegistrations(CatagoryChannel, topicName, "*").SubKeys()
	producers := s.emqlookupd.DB.FindProducers(CatagoryTopic, topicName, "")

	c.JSON(http.StatusOK, map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	})
}

func (s *HTTPServer) topics(c *gin.Context) {
	topics := s.emqlookupd.DB.FindRegistrations(CatagoryTopic, "*", "").Keys()

	c.JSON(http.StatusOK, map[string]interface{}{
		"topics": topics,
	})
}

func (s *HTTPServer) channels(c *gin.Context) {
	topicName := c.Query("topic")
	if topicName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_TOPIC"))
		return
	}

	channels := s.emqlookupd.DB.FindRegistrations(CatagoryChannel, topicName, "*").SubKeys()

	c.JSON(http.StatusOK, map[string]interface{}{
		"channels": channels,
	})
}

type node struct {
	TCPAddress  string   `json:"tcp_address"`
	HTTPAddress string   `json:"http_address"`
	Topics      []string `json:"topics"`
}

func (s *HTTPServer) nodes(c *gin.Context) {
	producers := s.emqlookupd.DB.FindProducers(CatagoryClient, "", "")
	nodes := make([]*node, len(producers))
	for i, p := range producers {
		topics := s.emqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter(CatagoryTopic, "*", "").Keys()
		nodes[i] = &node{
			TCPAddress:  p.peerInfo.TCPAddress,
			HTTPAddress: p.peerInfo.HTTPAddress,
			Topics:      topics,
		}
	}

	c.JSON(http.StatusOK, map[string]interface{}{
		"nodes": nodes,
	})
}

type createTopicReq struct {
	Topic string `json:"topic"`
}

func (s *HTTPServer) createTopic(c *gin.Context) {
	req := createTopicReq{}
	c.BindJSON(&req)

	topicName := req.Topic
	if topicName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_TOPIC"))
		return
	}

	log.Infof("DB: adding topic %s", topicName)
	reg := Registration{
		Category: CatagoryTopic,
		Key:      topicName,
		SubKey:   "",
	}
	s.emqlookupd.DB.AddRegistration(reg)

	c.JSON(http.StatusOK, SucResp())
}

type deleteTopicReq struct {
	Topic string `json:"topic"`
}

func (s *HTTPServer) deleteTopic(c *gin.Context) {
	req := deleteTopicReq{}
	c.BindJSON(&req)

	topicName := req.Topic
	if topicName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_TOPIC"))
		return
	}

	regs := s.emqlookupd.DB.FindRegistrations(CatagoryChannel, topicName, "*")
	for _, r := range regs {
		log.Infof("DB: removing channel: %s from topic: %s", r.SubKey, topicName)
		s.emqlookupd.DB.RemoveRegistration(r)
	}

	regs = s.emqlookupd.DB.FindRegistrations(CatagoryTopic, topicName, "")
	for _, r := range regs {
		log.Infof("DB: removing topic: %s", topicName)
		s.emqlookupd.DB.RemoveRegistration(r)
	}

	c.JSON(http.StatusOK, SucResp())
}

type createChannelReq struct {
	Topic   string `json:"topic"`
	Channel string `json:"channel"`
}

func (s *HTTPServer) createChannel(c *gin.Context) {
	req := createChannelReq{}
	c.BindJSON(&req)

	topicName := req.Topic
	if topicName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_TOPIC"))
		return
	}

	channelName := req.Channel
	if channelName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_CHANNEL"))
		return
	}

	log.Infof("DB: adding topic %s", topicName)
	reg := Registration{
		Category: CatagoryTopic,
		Key:      topicName,
		SubKey:   "",
	}
	s.emqlookupd.DB.AddRegistration(reg)

	log.Infof("DB: adding channel %s", channelName)
	reg = Registration{
		Category: CatagoryChannel,
		Key:      topicName,
		SubKey:   channelName,
	}
	s.emqlookupd.DB.AddRegistration(reg)

	c.JSON(http.StatusOK, SucResp())
}

type deleteChannelReq struct {
	Topic   string `json:"topic"`
	Channel string `json:"channel"`
}

func (s *HTTPServer) deleteChannel(c *gin.Context) {
	req := deleteChannelReq{}
	c.BindJSON(&req)

	topicName := req.Topic
	if topicName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_TOPIC"))
		return
	}

	channelName := req.Channel
	if channelName == "" {
		c.JSON(http.StatusBadRequest, ErrResp("MISSING_ARG_CHANNEL"))
		return
	}

	regs := s.emqlookupd.DB.FindRegistrations(CatagoryChannel, topicName, channelName)
	if len(regs) == 0 {
		c.JSON(http.StatusBadRequest, ErrResp("CHANNEL_NOT_FOUND"))
		return
	}
	for _, r := range regs {
		log.Infof("DB: removing channel: %s from topic: %s", r.SubKey, topicName)
		s.emqlookupd.DB.RemoveRegistration(r)
	}

	c.JSON(http.StatusOK, SucResp())
}
