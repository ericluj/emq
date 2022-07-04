package emqcli

type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
}

func Subscribe(topic string, channel string) *Command {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &Command{[]byte("SUB"), params, nil}
}
