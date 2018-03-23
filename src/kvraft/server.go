package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"os"
)

//const Debug = 0
const Debug =1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Value string
	Key string
	ID int
	Cmd string
	From int
}

type PendingOp struct{
	ops map[int]*Op
	notifyCh map[int] chan *Result
}

type Result struct{
	Value string
	Err string
}
type CMD struct{
	resultChan chan *Result 
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commited  map[string] *Op
	//pending   map[int] *Op
	pending  map[int]*PendingOp // key is client id
	clientID map[int]int
	log *log.Logger
	cmdCh chan *CMD
}

func (kv *RaftKV) Log(format string, arg ...interface{}){
	if Debug > 0{
		kv.log.Printf(format, arg...)
	}
}

func (op *Op) String()string{
	return fmt.Sprintf("key:%s value:%v id:%d from:%d cmd:%s", op.Key, op.Value, op.ID, op.From, op.Cmd)
}
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	from := args.From
	kv.mu.Lock()
	max, ok := kv.clientID[from]
	//TODO: how to handle the return value properly
	if ok && args.Seq <= max{
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	 op := Op{
		Key : args.Key,
		ID: args.Seq,
		From: args.From,
		Cmd: "Get",
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.WrongLeader = true
		return
	}
	resultChan := kv.addPending(&op)
	result := <-resultChan
	reply.Value= result.Value
	reply.From = op.From
	reply.Seq = args.Seq
	reply.Err = OK
}


func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Log("putappend: args:%s\n", args.String())
	from := args.From
	kv.mu.Lock()
	max, ok := kv.clientID[from]
	//TODO: how to handle the return value properly
	if ok && args.Seq <= max{
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	 op := Op{
		Key : args.Key,
		Value: args.Value,
		ID: args.Seq,
		From: args.From,
		Cmd: args.Op,
	}

	num := 0
	retry:
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		if num < 3{
			num++
			time.Sleep(time.Millisecond * 500)
			goto retry
		}
		reply.WrongLeader = true
		kv.Log("not leader\n")
		return
	}
	ch := kv.addPending(&op)
	result := <-ch
	reply.Seq = args.Seq
	reply.Err = Err(result.Err)
}

func (kv *RaftKV) addPending(op *Op) chan *Result{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	from := op.From
	kv.clientID[from] = op.ID
	pending, ok := kv.pending[from]
	if !ok{
		pending = &PendingOp{ ops: make(map[int]*Op), notifyCh: make(map[int]chan *Result)}
		kv.pending[from] = pending
	}
	pending.ops[op.ID] = op
	resultChan := make(chan *Result, 1)
	pending.notifyCh[op.ID] = resultChan
	return resultChan
}

func (kv *RaftKV) removePending(op *Op) chan *Result{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Log("remove op:%v\n", *op)
	from := op.From
	pending, ok:= kv.pending[from]
	//We may not the leader
	if !ok{
		//kv.doPanic(op.String())
		return nil
	}
	ch, ok := pending.notifyCh[op.ID]
	if !ok{
		kv.doPanic(op.String())
	}
	delete(pending.notifyCh, op.ID)
	delete(pending.ops, op.ID)
	return ch
}


func (kv *RaftKV) doPanic(info string){
	panic(info)
}

func (kv *RaftKV) timeout(){

}
func (kv *RaftKV) poll(){
	for{
		select {
			case msg := <-kv.applyCh:
				op := msg.Command.(Op)
				ch := kv.removePending(&op)
				if ch == nil{
					break
				}
				result := &Result{}
				kv.mu.Lock()
				switch op.Cmd{
				case "Put":
					_, ok := kv.commited[op.Key]
					if !ok{
						kv.commited[op.Key] = &op
					}
					kv.commited[op.Key].Value = op.Value
					kv.Log("put: arg:%s\n", op.String())
					result.Err = OK
				case "Append":
					_, ok := kv.commited[op.Key]
					if !ok{
						kv.commited[op.Key] = &op
					}
					oldV := kv.commited[op.Key].Value
					newV := oldV + op.Value
					kv.commited[op.Key].Value = newV
					kv.Log("append: arg:%s new:%s\n", op.String(), newV) 
					result.Err = OK
				case "Get":
					v, ok := kv.commited[op.Key]
					if !ok{
						kv.Log("get: arg:%s not find\n", op.String())
						result.Err = ErrNoKey
						break
					}
					result.Err = OK
					result.Value = v.Value
					kv.Log("get:arg:%s, result:%s\n", op.String(), v.Value)
				}
				kv.mu.Unlock()
				ch <-result
			default:
				time.Sleep(time.Millisecond * 10)
		}
	}
}
//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.commited = make(map[string]*Op, 0)
	kv.pending = make(map[int]*PendingOp, 0)
	kv.clientID = make(map[int]int, 0)
	kv.cmdCh = make(chan *CMD, 64)
	prefix := fmt.Sprintf("[%05d] ", me)
	kv.log = log.New(os.Stdout, prefix, log.Ldate | log.Lshortfile | log.Lmicroseconds)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.poll()

	return kv
}
