package emqd

type Client struct {
	ID int64

	emqd *EMQD
}

func (c *Client) IOLoop() error {
	for {

	}
}
