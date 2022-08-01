package emqlookupd

type Options struct {
	TCPAddress  string
	HTTPAddress string
}

func NewOptions() *Options {
	o := &Options{
		TCPAddress:  "0.0.0.0:7001",
		HTTPAddress: "0.0.0.0:7002",
	}
	return o
}
