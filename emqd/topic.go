package emqd

type Topic struct {
	name string
	emqd *EMQD

	channelMap map[string]*Channel
}
