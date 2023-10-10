package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "TimeOut"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandId int
	ClientId  int64
}

type GetReply struct {
	Err   Err
	Value string
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        string
	CommandId int
	ClientId  int64
}

type CommandReply struct {
	Err   Err
	Value string
}
