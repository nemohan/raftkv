package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
//import "sync/atomic"
//import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	seq    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	value := ""
	seq := ck.seq
	ck.seq++
	arg := &GetArgs{Key : key, Seq : seq}
	reply := &GetReply{}
	if ck.leader != -1{
		ok := ck.servers[ck.leader].Call("RaftKV.Get", arg, reply)
		if !ok || reply.WrongLeader{
			goto next
		}
		return reply.Value
	}
	next:
	for i, c := range ck.servers{
		ok := c.Call("RaftKV.Get", arg, reply)
		if !ok || reply.WrongLeader{
			continue
		}
		ck.leader = i
		return reply.Value
	}
	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seq := ck.seq
	ck.seq++
	arg := &PutAppendArgs{Key: key, Value: value, Op: op, Seq:seq}
	reply := &PutAppendReply{}
	if ck.leader != -1{
		ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", arg, reply)
		if !ok || reply.WrongLeader{
			goto next
		}
		if reply.Err == OK{
			return
		}
	}
	next:
	for i, c := range ck.servers{
		ok := c.Call("RaftKV.PutAppend", arg, reply)
		if !ok || reply.WrongLeader{
			continue
		}
		ck.leader = i
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
