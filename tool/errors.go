package tool

type RuntimeError struct {
	Stage string
	Err   error
}

func (re *RuntimeError) Error() string {
	return re.Stage + ": " + re.Err.Error()
}
