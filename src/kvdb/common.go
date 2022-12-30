package kvdb

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrTimeout        = "ErrTimeout"
	ErrInvalidSession = "ErrInvalidSession"
)

const (
	OpenSession = "Open_Session"
	GET         = "Get"
	PUT         = "Put"
	APPEND      = "Append"
	DELETE      = "Delete"
)

type Err string

type OpenSessionArgs struct {
}

type OpenSessionReply struct {
	ClientId int64 // client id
	Err      string
}

type UpdateArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append" or "Delete"
	ClientId  int64  // client id
	RequestId int64  // client request id (increase monotonically)
}

type UpdateReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64 // client id
	RequestId int64 // client request id (increase monotonically)
}

type GetReply struct {
	Err   Err
	Value string
}
