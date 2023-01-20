package tool

import "errors"

type RuntimeError struct {
	Stage string
	Err   error
}

func (re *RuntimeError) Error() string {
	return re.Stage + ": " + re.Err.Error()
}

var ErrEvenServers = errors.New("even number of servers")

var ErrInvalidPort = errors.New("server port is invalid")

var ErrNilStorage = errors.New("storage is nil")
