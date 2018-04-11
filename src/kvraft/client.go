package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"
import "time"
//import "sync"

import "fmt"
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	seq    int
	id     uint32
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
	ck.id = getID()
	// You'll have to add code here.
	ck.leader = -1
	return ck
}

var clientID uint32
func getID()uint32{
	return atomic.AddUint32(&clientID, 1)
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
	num := 0
	retry:
	arg := &GetArgs{Key : key, Seq : seq, From: int(ck.id)}
	reply := &GetReply{}
	fmt.Printf("c:%d get key:%s seq:%d\n", ck.id, key, seq)
	if ck.leader != -1{
		ok := ck.servers[ck.leader].Call("RaftKV.Get", arg, reply)
		if !ok {
			fmt.Printf("c:%d get key failed:%s from leader:%d\n", ck.id, key, ck.leader)
			if num < 2{
				time.Sleep(time.Millisecond * 50)
				goto retry
			}
			goto next
		}

		if reply.WrongLeader || reply.Err == ErrTimeout{
			fmt.Printf("c:%d get key failed:%s from leader change:%d\n", ck.id, key, ck.leader)
			goto next
		}

		fmt.Printf("c:%d get key:%s :%v leader index:%d\n", ck.id, key, reply, ck.leader)
		return reply.Value
	}
	next:
	for i, c := range ck.servers{
		reply := &GetReply{}
		ok := c.Call("RaftKV.Get", arg, reply)
		if !ok || reply.WrongLeader || reply.Err == ErrTimeout{
			fmt.Printf("c:%d get key:%s client reply:%v to:%d from:%d failed rpc:%v\n", ck.id, key, reply, i, reply.From, ok)
			continue
		}
		ck.leader = i
		fmt.Printf("c:%d get key:%s 1:%v leader index:%d from:%d\n", ck.id, key, reply, i, reply.From)
		return reply.Value
	}
	time.Sleep(time.Millisecond * 20)
	goto next
	fmt.Printf("c:%d get nothing key:%s\n", ck.id, key)
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
	num := 0

	retry:
	arg := &PutAppendArgs{Key: key, Value: value, Op: op, Seq:seq, From:int(ck.id)}
	fmt.Printf("c:%d client  leader idx:%d put:%s \n", ck.id, ck.leader, arg.String())
	reply := &PutAppendReply{}
	if ck.leader != -1{
		ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", arg, reply)
		if !ok{
			if num < 3{
				time.Sleep(time.Millisecond * 50)
				num++
				goto retry
			}
		}
		if !reply.WrongLeader || reply.Err == ErrTimeout{
			goto next
		}
		if reply.Err == OK{
			fmt.Printf("c:%d client put:%s success leader index:%d \n", ck.id, arg.String(), ck.leader)
			return
		}
	}
	next:
	for i, c := range ck.servers{
		reply := &PutAppendReply{}
		ok := c.Call("RaftKV.PutAppend", arg, reply)
		if !ok || reply.WrongLeader || reply.Err == ErrTimeout{
			continue
		}
		ck.leader = i
		fmt.Printf("c:%d client put:%s ok leader index:%d\n", ck.id, arg.String(), ck.leader)
		return
	}
	goto next
	time.Sleep(time.Millisecond * 20)

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
