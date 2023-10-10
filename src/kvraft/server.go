package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string
	ClientId  int64
	CommandId int
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// appliedindex
	LastApplied  int
	StateMachine KVStateMachine

	// clientId to commandId
	ClientId2ComId map[int64]int

	// commandId to command
	ComNotify map[int]chan Op
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

// kv datebase implement kvstatemachine
type MemoryKV struct {
	KV map[string]string
}

func (kv *MemoryKV) Get(key string) (string, Err) {
	value, ok := kv.KV[key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *MemoryKV) Put(key, value string) Err {
	kv.KV[key] = value
	return OK
}

func (kv *MemoryKV) Append(key, value string) Err {
	kv.KV[key] += value
	return OK
}

func (kv *KVServer) applyStateMachine(op *Op) {
	switch op.Command {
	case "Put":
		kv.StateMachine.Put(op.Key, op.Value)
	case "Append":
		kv.StateMachine.Append(op.Key, op.Value)
	}
}

func (kv *KVServer) GetChan(index int) chan Op {
	ch, ok := kv.ComNotify[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.ComNotify[index] = ch
	}
	return ch
}

// persist
func (kv *KVServer) PersisterSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.StateMachine)
	e.Encode(kv.ClientId2ComId)
	return w.Bytes()
}

func (kv *KVServer) DecodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var StateMachine MemoryKV
	var ClientId2ComId map[int64]int
	if d.Decode(&StateMachine) != nil || d.Decode(&ClientId2ComId) != nil {

	} else {
		kv.StateMachine = &StateMachine
		kv.ClientId2ComId = ClientId2ComId
	}
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		select {

		// server get applych from raftnote
		case ch := <-kv.applyCh:
			// an apply
			if ch.CommandValid {
				kv.mu.Lock()
				if ch.CommandIndex <= kv.LastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.LastApplied = ch.CommandIndex

				opchan := kv.GetChan(ch.CommandIndex)
				op := ch.Command.(Op)

				// apply to stateMachine(kvdatebase)
				if kv.ClientId2ComId[op.ClientId] < op.CommandId {
					kv.applyStateMachine(&op)
					kv.ClientId2ComId[op.ClientId] = op.CommandId
				}

				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					kv.rf.Snapshot(ch.CommandIndex, kv.PersisterSnapshot())
				}

				kv.mu.Unlock()
				opchan <- op
			}

			// a snap
			if ch.SnapshotValid {
				kv.mu.Lock()
				if ch.SnapshotIndex > kv.LastApplied {
					kv.DecodeSnapshot(ch.Snapshot)
					kv.LastApplied = ch.SnapshotIndex
				}
				kv.mu.Unlock()
			}

		}
	}
}

// rpc
func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	// old commandId for put or append method
	if args.Op != "Get" && kv.ClientId2ComId[args.ClientId] >= args.CommandId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Command:   args.Op,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader

		return
	}
	kv.mu.Lock()
	ch := kv.GetChan(index)
	kv.mu.Unlock()

	select {
	case apply := <-ch:
		if apply.ClientId == op.ClientId && apply.CommandId == op.CommandId {
			if args.Op == "Get" {
				kv.mu.Lock()
				reply.Value, reply.Err = kv.StateMachine.Get(apply.Key)
				kv.mu.Unlock()
			}
			reply.Err = OK
		} else {
			reply.Err = TimeOut
		}
	case <-time.After(time.Millisecond * 33):
		reply.Err = TimeOut
	}

	go func() {
		kv.mu.Lock()
		delete(kv.ComNotify, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ClientId2ComId = make(map[int64]int)
	kv.ComNotify = make(map[int]chan Op)
	kv.StateMachine = &MemoryKV{make(map[string]string)}

	// read snapshot
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}

	go kv.apply()
	// You may need initialization code here.

	return kv
}
