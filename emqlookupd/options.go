package emqlookupd

type Options struct {
	TCPAddress  string
	HTTPAddress string
}

func NewOptions() *Options {
	o := &Options{
		TCPAddress:  "0.0.0.0:6003",
		HTTPAddress: "0.0.0.0:6004",
	}
	return o
}
