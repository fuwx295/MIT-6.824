package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead       int32    // set by Kill()
	configs    []Config // indexed by config num
	Client2Seq map[int64]int
	Index2Cmd  map[int]chan result
}

type Op struct {
	// Your data here.
	Command   string
	ClientId  int64
	CommandId int
	Servers   map[int][]string //Join
	GIDs      []int            //leave
	Shard     int              //Move
	GID       int              //Move
	Num       int              //Query
}

type result struct {
	ClientId  int64
	CommandId int
	Res       Config
}

func (sc *ShardCtrler) Join(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) Leave(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) Move(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) Query(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.Command(args, reply)
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	if sc.killed() {
		reply.Err = WrongLeader
		return
	}
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = WrongLeader
		return
	}
	sc.mu.Lock()
	if sc.Client2Seq[args.ClientId] >= args.CommandId {
		reply.Err = OK
		if args.Op == Query {
			reply.Config = sc.QueryProcess(args.Num)

		}
		sc.mu.Unlock()
		return
	}

	op := Op{
		Command:   args.Op,
		CommandId: args.CommandId,
		ClientId:  args.ClientId,

		Servers: args.Servers, //Join
		GIDs:    args.GIDs,    //leave
		Shard:   args.Shard,   //Move
		GID:     args.GID,     //Move
		Num:     args.Num,     //Query
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = WrongLeader
		sc.mu.Unlock()
		return
	}
	ch := sc.GetChan(index)
	sc.mu.Unlock()

	select {
	case app := <-ch:
		if app.ClientId == op.ClientId && app.CommandId == op.CommandId {
			reply.Err = OK
			reply.Config = app.Res
		} else {

			reply.Err = WrongLeader

		}

	case <-time.After(time.Millisecond * 33):
		reply.Err = WrongLeader
	}

	go func() {
		sc.mu.Lock()
		delete(sc.Index2Cmd, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) GetChan(index int) chan result {

	ch, ok := sc.Index2Cmd[index]
	if !ok {
		ch = make(chan result, 1)
		sc.Index2Cmd[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		select {
		case ch := <-sc.applyCh:
			if ch.CommandValid {
				sc.mu.Lock()

				opchan := sc.GetChan(ch.CommandIndex)
				op := ch.Command.(Op)
				res := result{
					ClientId:  op.ClientId,
					CommandId: op.CommandId,
				}

				if sc.Client2Seq[op.ClientId] < op.CommandId {

					switch op.Command {
					case Join:
						sc.JoinProcess(op.Servers)
					case Leave:
						sc.LeaveProcess(op.GIDs)
					case Move:
						sc.MoveProcess(op.GID, op.Shard)
					case Query:
						res.Res = sc.QueryProcess(op.Num)
					}

					sc.Client2Seq[op.ClientId] = op.CommandId
					sc.mu.Unlock()
					opchan <- res
				} else {

					sc.mu.Unlock()
				}

			}

		}
	}
}

func (sc *ShardCtrler) GetLastConfig() Config {
	if len(sc.configs) > 0 {

		config := sc.configs[len(sc.configs)-1]
		return config
	}
	return Config{}
}

func Group2Shard(config Config) map[int][]int {
	g2s := make(map[int][]int)

	for gid, _ := range config.Groups {
		g2s[gid] = make([]int, 0)
	}

	for k, v := range config.Shards {
		if _, ok := g2s[v]; !ok {
			g2s[v] = make([]int, 0)
		}
		g2s[v] = append(g2s[v], k)
	}

	return g2s
}

func getGid_MinShards(g2s map[int][]int) int {
	// order map
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find gid with minShards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(g2s[gid]) < min {
			index, min = gid, len(g2s[gid])
		}
	}
	return index
}

func getGid_MaxShards(g2s map[int][]int) int {

	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	index, max := -1, -1
	for _, gid := range keys {
		if len(g2s[gid]) > max {
			index, max = gid, len(g2s[gid])
		}
	}
	return index
}

func (sc *ShardCtrler) JoinProcess(Servers map[int][]string) {

	lastConfig := sc.GetLastConfig()
	lastGroup := make(map[int][]string)
	// copy
	for k, v := range lastConfig.Groups {
		lastGroup[k] = v
	}
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: lastGroup,
	}
	for gid, servers := range Servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	g2s := Group2Shard(newConfig)

	// find big and small untill blance
	for {
		from, to := getGid_MaxShards(g2s), getGid_MinShards(g2s)

		if from != 0 && len(g2s[from])-len(g2s[to]) <= 1 {
			break
		}
		g2s[to] = append(g2s[to], g2s[from][0])
		g2s[from] = g2s[from][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	//return &newConfig
}

func (sc *ShardCtrler) LeaveProcess(GIDs []int) {
	lastConfig := sc.GetLastConfig()
	lastGroup := make(map[int][]string)
	orphanShards := make([]int, 0)
	for k, v := range lastConfig.Groups {
		lastGroup[k] = v
	}
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: lastGroup}
	g2s := Group2Shard(newConfig)
	for _, gid := range GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}

	}
	var newShards [NShards]int

	// find target small to add leave group
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			to := getGid_MinShards(g2s)
			g2s[to] = append(g2s[to], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) MoveProcess(gid int, shard int) {
	lastConfig := sc.GetLastConfig()
	lastGroup := make(map[int][]string)
	for k, v := range lastConfig.Groups {
		lastGroup[k] = v
	}
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: lastGroup}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) QueryProcess(num int) Config {
	if num == -1 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}

	return sc.configs[num]
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.Client2Seq = make(map[int64]int)
	sc.Index2Cmd = make(map[int]chan result)

	go sc.apply()
	return sc
}
