package shardkv

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"

	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

//----------------------------------------------------结构体定义部分------------------------------------------------------

const (
	UpConfigLoopInterval = 100 * time.Millisecond // poll configuration period

	GetTimeout          = 500 * time.Millisecond
	AppOrPutTimeout     = 500 * time.Millisecond
	UpConfigTimeout     = 500 * time.Millisecond
	AddShardsTimeout    = 500 * time.Millisecond
	RemoveShardsTimeout = 500 * time.Millisecond

	TimeOut = 500 * time.Millisecond
)

type Op struct {

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int
	OpType    Operation // "get" "put" "append"
	Key       string
	Value     string
	UpConfig  shardctrler.Config
	ShardId   int

	Shard          KVShard
	ClientId2ComId map[int64]int
}

// OpReply is used to wake waiting RPC caller after Op arrived from applyCh
type OpReply struct {
	ClientId  int64
	CommandId int
	Err       Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxRaftState int // snapshot if log grows this big

	// Your definitions here.

	dead int32 // set by Kill()

	Config     shardctrler.Config // 需要更新的最新的配置
	LastConfig shardctrler.Config // 更新之前的配置，用于比对是否全部更新完了

	DB KVStateMachine // Storage system

	ComNotify      map[int]chan OpReply
	ClientId2ComId map[int64]int

	sck *shardctrler.Clerk // sck is a client used to contact shard master
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
	GetShard(shardId int) *KVShard
}

type KVMemory struct {
	KV []KVShard
}

type KVShard struct {
	KVS       map[string]string
	ConfigNum int // what version this Shard is in
}

func (kv *KVMemory) Get(key string) (string, Err) {
	shardId := key2shard(key)
	value, ok := kv.KV[shardId].KVS[key]
	if ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (kv *KVMemory) Put(key, value string) Err {
	shardId := key2shard(key)
	kv.KV[shardId].KVS[key] = value
	return OK
}

func (kv *KVMemory) Append(key, value string) Err {
	shardId := key2shard(key)
	kv.KV[shardId].KVS[key] += value
	return OK
}

func (kv *KVMemory) GetShard(shardId int) *KVShard {
	return &kv.KV[shardId]
}

func (kv *ShardKV) applyStateMachine(op *Op) {
	switch op.OpType {
	case PutType:
		kv.DB.Put(op.Key, op.Value)
	case AppendType:
		kv.DB.Append(op.Key, op.Value)
	}
}

//-------------------------------------------------初始化(Start)部分------------------------------------------------------

// StartServer me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxRaftState bytes, in order to allow Raft to garbage-collect its
// log. if maxRaftState is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass masters[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// UpConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific Shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	kv.DB = &KVMemory{make([]KVShard, shardctrler.NShards)}
	kv.ClientId2ComId = make(map[int64]int)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.masters)
	kv.sck = shardctrler.MakeClerk(kv.masters)
	kv.ComNotify = make(map[int]chan OpReply)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()
	go kv.ConfigDetectedLoop()

	return kv
}

//------------------------------------------------------Loop部分--------------------------------------------------------

// applyMsgHandlerLoop 处理applyCh发送过来的ApplyMsg
func (kv *ShardKV) apply() {
	for {
		if kv.killed() {
			return
		}
		select {

		case ch := <-kv.applyCh:

			if ch.CommandValid == true {
				kv.mu.Lock()
				op := ch.Command.(Op)
				reply := OpReply{
					ClientId:  op.ClientId,
					CommandId: op.CommandId,
					Err:       OK,
				}

				if op.OpType == PutType || op.OpType == GetType || op.OpType == AppendType {

					shardId := key2shard(op.Key)
					if kv.Config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.DB.GetShard(shardId).KVS == nil {
						// 如果应该存在的切片没有数据那么这个切片就还没到达
						reply.Err = ShardNotArrived
					} else {
						if !kv.ifDuplicate(op.ClientId, op.CommandId) {
							kv.ClientId2ComId[op.ClientId] = op.CommandId
							kv.applyStateMachine(&op)
						}
					}
				} else {
					// request from server of other group
					switch op.OpType {
					case UpConfigType:
						kv.upConfigHandler(op)
					case AddShardType:
						// 如果配置号比op的SeqId还低说明不是最新的配置
						if kv.Config.Num < op.CommandId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.addShardHandler(op)
					case RemoveShardType:
						// remove operation is from previous UpConfig
						kv.removeShardHandler(op)
					default:
						log.Fatalf("invalid command type: %v.", op.OpType)
					}
				}

				// 如果需要snapshot，且超出其stateSize
				if kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() > kv.maxRaftState {
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(ch.CommandIndex, snapshot)
				}

				ch := kv.getWaitCh(ch.CommandIndex)
				ch <- reply

				kv.mu.Unlock()

			}

			if ch.SnapshotValid == true {
				if len(ch.Snapshot) > 0 {
					// 读取快照的数据
					kv.mu.Lock()
					kv.DecodeSnapShot(ch.Snapshot)
					kv.mu.Unlock()
				}
				continue
			}

		}
	}

}

func (kv *ShardKV) sendAddShrad(servers []*labrpc.ClientEnd, args *SendShardArg) {
	index := 0
	start := time.Now()
	for {
		var reply AddShardReply

		ok := servers[index].Call("ShardKV.AddShard", args, &reply)
		// send shard success or timeout
		if ok && reply.Err == OK || time.Now().Sub(start) >= 2*time.Second {
			kv.mu.Lock()
			command := Op{
				OpType:    RemoveShardType,
				ClientId:  int64(kv.gid),
				ShardId:   args.ShardId,
				CommandId: kv.Config.Num,
			}
			kv.mu.Unlock()
			// remove shard
			kv.startCommand(command, TimeOut)
			break
		}
		index = (index + 1) % len(servers)
		if index == 0 {
			time.Sleep(UpConfigLoopInterval)
		}
	}
}

// ConfigDetectedLoop 配置检测
func (kv *ShardKV) ConfigDetectedLoop() {
	kv.mu.Lock()

	curConfig := kv.Config
	rf := kv.rf
	kv.mu.Unlock()

	for !kv.killed() {
		// only leader needs to deal with configuration tasks
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()

		// send finished ?
		if !kv.allSent() {
			SeqMap := make(map[int64]int)
			for k, v := range kv.ClientId2ComId {
				SeqMap[k] = v
			}
			for shardId, gid := range kv.LastConfig.Shards {

				// send shard to other
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid && kv.DB.GetShard(shardId).ConfigNum < kv.Config.Num {

					sendDate := kv.cloneShard(kv.Config.Num, kv.DB.GetShard(shardId).KVS)

					args := SendShardArg{
						LastAppliedCommandId: SeqMap,
						ShardId:              shardId,
						Shard:                sendDate,
						ClientId:             int64(gid),
						CommandId:            kv.Config.Num,
					}

					// shardId -> gid -> server names
					serversList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.makeEnd(name)
					}
					go kv.sendAddShrad(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// received finished ?
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// current configuration is configured, poll for the next configuration
		curConfig = kv.Config
		sck := kv.sck
		kv.mu.Unlock()
		// pull new configuration
		newConfig := sck.Query(curConfig.Num + 1)
		if newConfig.Num != curConfig.Num+1 {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		command := Op{
			OpType:    UpConfigType,
			ClientId:  int64(kv.gid),
			CommandId: newConfig.Num,
			UpConfig:  newConfig,
		}
		kv.startCommand(command, UpConfigTimeout)
	}

}

//------------------------------------------------------RPC部分----------------------------------------------------------

func (kv *ShardKV) Get(args *CommandArgs, reply *CommandReply) {
	kv.Command(args, reply)
}

func (kv *ShardKV) PutAppend(args *CommandArgs, reply *CommandReply) {
	kv.Command(args, reply)
}

// AddShard move shards from caller to this server
func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	command := Op{
		OpType:         AddShardType,
		ClientId:       args.ClientId,
		CommandId:      args.CommandId,
		ShardId:        args.ShardId,
		Shard:          args.Shard,
		ClientId2ComId: args.LastAppliedCommandId,
	}
	reply.Err = kv.startCommand(command, AddShardsTimeout)
}

// Put Append Get
func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.DB.GetShard(shardId).KVS == nil {
		reply.Err = ShardNotArrived
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		ShardId:   shardId,
	}
	reply.Err = kv.startCommand(command, TimeOut)

	// Get function should check twice
	if reply.Err == OK && command.OpType == GetType {
		kv.mu.Lock()
		if kv.Config.Shards[shardId] != kv.gid {
			reply.Err = ErrWrongGroup
		} else if kv.DB.GetShard(shardId).KVS == nil {
			reply.Err = ShardNotArrived
		} else {

			reply.Value, reply.Err = kv.DB.Get(args.Key)
		}
		kv.mu.Unlock()
	}

}

func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	ch := kv.getWaitCh(index)
	kv.mu.Unlock()

	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop()

	select {
	case re := <-ch:
		kv.mu.Lock()
		delete(kv.ComNotify, index)
		if re.CommandId != command.CommandId || re.ClientId != command.ClientId {
			// One way to do this is for the server to detect that it has lost leadership,
			// by noticing that a different request has appeared at the index returned by Start()
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return re.Err

	case <-timer.C:
		return ErrOverTime
	}
}

//------------------------------------------------------handler部分------------------------------------------------------

// update Configuration
func (kv *ShardKV) upConfigHandler(op Op) {
	curConfig := kv.Config
	newConfig := op.UpConfig
	if curConfig.Num >= newConfig.Num {
		return
	}
	for shard, gid := range newConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
			kv.DB.GetShard(shard).KVS = make(map[string]string)
			kv.DB.GetShard(shard).ConfigNum = newConfig.Num
		}
	}
	kv.LastConfig = curConfig
	kv.Config = newConfig

}

func (kv *ShardKV) addShardHandler(op Op) {
	// this shard is added or it is an outdated command
	if kv.DB.GetShard(op.ShardId).KVS != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}
	clone := kv.cloneShard(op.Shard.ConfigNum, op.Shard.KVS)

	kv.DB.GetShard(op.ShardId).KVS = clone.KVS
	kv.DB.GetShard(op.ShardId).ConfigNum = clone.ConfigNum

	for clientId, comId := range op.ClientId2ComId {
		if r, ok := kv.ClientId2ComId[clientId]; !ok || r < comId {
			kv.ClientId2ComId[clientId] = comId
		}
	}
}

func (kv *ShardKV) removeShardHandler(op Op) {
	if op.CommandId < kv.Config.Num {
		return
	}
	kv.DB.GetShard(op.ShardId).KVS = nil
	kv.DB.GetShard(op.ShardId).ConfigNum = op.CommandId
}

//------------------------------------------------------持久化快照部分-----------------------------------------------------

// PersistSnapShot Snapshot get snapshot data of kvserver
func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.DB)
	err = e.Encode(kv.ClientId2ComId)
	err = e.Encode(kv.maxRaftState)
	err = e.Encode(kv.Config)
	err = e.Encode(kv.LastConfig)
	if err != nil {
		log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}
	return w.Bytes()
}

// DecodeSnapShot install a given snapshot
func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shardsPersist KVMemory
	var SeqMap map[int64]int
	var MaxRaftState int
	var Config, LastConfig shardctrler.Config

	if d.Decode(&shardsPersist) != nil || d.Decode(&SeqMap) != nil ||
		d.Decode(&MaxRaftState) != nil || d.Decode(&Config) != nil || d.Decode(&LastConfig) != nil {
		log.Fatalf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	} else {
		kv.DB = &shardsPersist
		kv.ClientId2ComId = SeqMap
		kv.maxRaftState = MaxRaftState
		kv.Config = Config
		kv.LastConfig = LastConfig

	}
}

//------------------------------------------------------utils封装部分----------------------------------------------------

// Kill the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) ifDuplicate(clientId int64, commandId int) bool {

	lastCommandId, ok := kv.ClientId2ComId[clientId]
	if !ok {
		return false
	}
	return commandId <= lastCommandId
}

func (kv *ShardKV) getWaitCh(index int) chan OpReply {

	ch, ok := kv.ComNotify[index]
	if !ok {
		kv.ComNotify[index] = make(chan OpReply, 1)
		ch = kv.ComNotify[index]
	}
	return ch
}

func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid && kv.DB.GetShard(shard).ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {

		// 判断切片是否都收到了
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid && kv.DB.GetShard(shard).ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

// getShard
func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) KVShard {

	migrateShard := KVShard{
		KVS:       make(map[string]string),
		ConfigNum: ConfigNum,
	}

	for k, v := range KvMap {
		migrateShard.KVS[k] = v
	}

	return migrateShard
}
