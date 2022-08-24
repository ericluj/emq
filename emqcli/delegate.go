package emqcli

type ConnDelegate interface {
	OnClose(*Conn)
}
