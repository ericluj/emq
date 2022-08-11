package protocol

const (
	ErrTypeInvalid = "E_INVALID"
)

type FatalClientErr struct {
	Err  error
	Code string
	Desc string
}

func NewFatalClientErr(err error, code, desc string) *FatalClientErr {
	return &FatalClientErr{
		Err:  err,
		Code: code,
		Desc: desc,
	}
}

func (f *FatalClientErr) Error() string {
	return f.Code + "," + f.Desc
}
