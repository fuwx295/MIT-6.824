package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientId  int64
	CommandId int
}

// MakeClerk
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// UpConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.config = ck.sm.Query(-1)
	return ck
}

// Get GetType
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	// ck.CommandId++

	// for {
	// 	args := CommandArgs{
	// 		Key:       key,
	// 		ClientId:  ck.ClientId,
	// 		CommandId: ck.CommandId,
	// 		Op:        GetType,
	// 	}
	// 	shard := key2shard(key)
	// 	gid := ck.config.Shards[shard]
	// 	if servers, ok := ck.config.Groups[gid]; ok {
	// 		// try each server for the shard.
	// 		for si := 0; si < len(servers); si++ {
	// 			srv := ck.make_end(servers[si])
	// 			var reply CommandReply
	// 			ok := srv.Call("ShardKV.Get", &args, &reply)
	// 			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
	// 				return reply.Value
	// 			}
	// 			if ok && (reply.Err == ErrWrongGroup) {
	// 				break
	// 			}
	// 			// ... not ok, or ErrWrongLeader
	// 		}
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// 	// ask controler for the latest configuration.
	// 	ck.config = ck.sm.Query(-1)
	// }
	return ck.Command(&CommandArgs{Key: key, Op: GetType})

}

// PutAppend
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// ck.CommandId++

	// for {
	// 	args := CommandArgs{
	// 		Key:       key,
	// 		Value:     value,
	// 		Op:        Operation(op),
	// 		ClientId:  ck.ClientId,
	// 		CommandId: ck.CommandId,
	// 	}
	// 	shard := key2shard(key)
	// 	gid := ck.config.Shards[shard]
	// 	if servers, ok := ck.config.Groups[gid]; ok {
	// 		for si := 0; si < len(servers); si++ {
	// 			srv := ck.make_end(servers[si])
	// 			var reply CommandReply
	// 			//fmt.Printf("[ ++++Client[%v]++++] : send a Put,args:%+v,serverId[%v]\n", ck.clientId, args, si)
	// 			ok := srv.Call("ShardKV.PutAppend", &args, &reply)
	// 			if ok && reply.Err == OK {
	// 				return
	// 			}
	// 			if ok && reply.Err == ErrWrongGroup {
	// 				break
	// 			}
	// 			// ... not ok, or ErrWrongLeader
	// 		}
	// 	}

	// 	time.Sleep(100 * time.Millisecond)
	// 	// ask controler for the latest configuration.
	// 	ck.config = ck.sm.Query(-1)
	// }
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Operation(op)})
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Command(args *CommandArgs) string {
	args.ClientId = ck.ClientId
	ck.CommandId++
	args.CommandId = ck.CommandId
	for {
		shardId := key2shard(args.Key)
		gid := ck.config.Shards[shardId]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply CommandReply
				ok := srv.Call("ShardKV.Command", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)

	}
}
