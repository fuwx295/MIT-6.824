package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             Err = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"

	ShardNotArrived     = "ShardNotArrived"
	ConfigNotArrived    = "ConfigNotArrived"
	ErrInconsistentData = "ErrInconsistentData"
	ErrOverTime         = "ErrOverTime"
)

const (
	PutType    Operation = "Put"
	AppendType           = "Append"
	GetType              = "Get"

	UpConfigType    = "UpConfig"
	AddShardType    = "AddShard"
	RemoveShardType = "RemoveShard"
)

type Operation string

type Err string

// PutType or AppendType
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        Operation // "Put" or "Append"
	ClientId  int64
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int
}

type GetReply struct {
	Err   Err
	Value string
}

// Put Append Get
type CommandArgs struct {
	Key       string
	Value     string
	Op        Operation
	ClientId  int64
	CommandId int
}

type CommandReply struct {
	Value string
	Err   Err
}

type SendShardArg struct {
	LastAppliedCommandId map[int64]int // for receiver to update its state
	ShardId              int
	Shard                KVShard // Shard to be sent
	ClientId             int64
	CommandId            int
}

type AddShardReply struct {
	Err Err
}
