package emqcli

type Delegate interface {
	OnClose(*Conn)

	OnRequeue(*Message)
}
