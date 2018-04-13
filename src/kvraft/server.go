package raftkv

import (
	"bytes"
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
	lastResult map[int]*GetReply
	persister *raft.Persister
	lastIndex int
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
	kv.Log("get key:%s seq:%d max:%d\n", args.Key, args.Seq, max)
	//TODO: how to handle the return value properly
	if ok && args.Seq < max{
		kv.mu.Unlock()
		kv.Log("invalid seq for key:%s seq:%d max:%d\n", args.Key, args.Seq, max)
		return
	}
	if ok && args.Seq == max{
		oldReply, ok := kv.lastResult[args.From]
		if !ok{
			kv.mu.Unlock()
			kv.Log("miss for key:%s arg: from:%d seq:%d\n", args.Key, args.From, args.Seq)
			return
		}
		if oldReply.Seq < args.Seq{
			goto out
		}
		*reply = *oldReply
		kv.mu.Unlock()
		kv.Log("last hit for key:%s reply:%v\n", args.Key, reply)
		return
	}

	out:
	kv.mu.Unlock()
	key := args.Key
	 op := Op{
		Key : args.Key,
		ID: args.Seq,
		From: args.From,
		Cmd: "Get",
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.WrongLeader = true
		reply.Seq = args.Seq
		reply.From = kv.me
		return
	}
	resultChan := kv.addPending(&op)
	select{
		case <- time.After(time.Second):
			reply.Err = ErrTimeout 
			kv.removePending(&op)
			/*
			kv.mu.Lock()
			kv.clientID[from] = lastRequestID
			kv.mu.Unlock()
			*/
			kv.Log("get timeout args:%v key:%s\n", args, args.Key)
		case result := <-resultChan:
			reply.Value= result.Value
			//reply.From = op.From
			reply.From = kv.me
			reply.Seq = args.Seq
			reply.Err = OK
			kv.Log("get result:%s key:%s reply:%v\n", reply.Value, key, reply)
	}
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
	//lastRequestID := max
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
			time.Sleep(time.Millisecond * 100 )
			goto retry
		}
		reply.WrongLeader = true
		kv.Log("not leader\n")
		return
	}
	ch := kv.addPending(&op)
	//handle partition
	select{
		case <-time.After(time.Second):
			reply.Err = ErrTimeout
			kv.removePending(&op)
			/*
			kv.mu.Lock()
			kv.clientID[from] = lastRequestID
			kv.mu.Unlock()
			*/
			kv.Log("put timeout args:%s\n", args.String())
		case result := <-ch:
			reply.Seq = args.Seq
			reply.Err = Err(result.Err)
	}
}

func (kv *RaftKV) addPending(op *Op) chan *Result{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	from := op.From
	//kv.clientID[from] = op.ID
	kv.Log("seq id:%d from:%d\n", op.ID, from)
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
		//kv.doPanic(op.String())
		return nil
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

func (kv *RaftKV) handlePut(op *Op, ch chan *Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.commited[op.Key] = op
	kv.Log("put: arg:%s\n", op.String())
	if ch == nil{
		return
	}

	result := &Result{Err: OK}
	ch <- result
}

func (kv *RaftKV) handleAppend(op *Op, ch chan *Result){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	old, ok := kv.commited[op.Key]
	oldV := ""
	if !ok{
		kv.commited[op.Key] = op
	}else{
		oldV = old.Value
	}

	newV := oldV + op.Value
	kv.commited[op.Key].Value = newV
	kv.commited[op.Key].ID = op.ID
	kv.Log("append: arg:%s new:%s\n", op.String(), newV) 
	if ch == nil{
		return
	}
	result := &Result{Err: OK}
	ch <- result
}

func (kv *RaftKV) handleGet(op *Op, ch chan *Result){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	err := OK
	value := ""
	v, ok := kv.commited[op.Key]
	if !ok{
		kv.Log("get: arg:%s not find\n", op.String())
		err = ErrNoKey
		goto out
	}
	value = v.Value
	kv.Log("get:arg:%s, result:%s\n", op.String(), v.Value)
	kv.lastResult[op.From] = &GetReply{Err: OK, Value: v.Value, Seq: op.ID, From: kv.me}

	out :
	if ch == nil{
		return
	}

	ch <-&Result{Err: err, Value: value}
}

func (kv *RaftKV) checkRepeat(op *Op) (chan *Result, bool){
	kv.mu.Lock()
	//defer kv.mu.Unlock()
	isRepeat := false
	lastRequestID, ok := kv.clientID[op.From]

	kv.Log("reqid:%d last id:%d\n", op.ID, lastRequestID)
	if ok && op.ID <= lastRequestID{
		isRepeat = true
		kv.Log("detect repeat id:%d last id:%d\n", op.ID, lastRequestID)
		goto out
	}
	kv.clientID[op.From] = op.ID
	out:
	kv.mu.Unlock()
	ch := kv.removePending(op)
	if !isRepeat || ch == nil{
		return ch, isRepeat
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if op.Cmd == "Put" || op.Cmd == "Append"{
		ch <- &Result{Err: OK}
	}else{
		value := ""
		v, ok := kv.commited[op.Key]
		if ok{
			value = v.Value
		}
		ch <- &Result{Err: OK, Value:value}
	}
	return ch, isRepeat
}

func (kv *RaftKV) installSnapshot(){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.persister.RaftStateSize() < kv.maxraftstate{
		return
	}
	kv.Log("state size:%d\n", kv.persister.RaftStateSize())
	w := new(bytes.Buffer)
        e := gob.NewEncoder(w)
	e.Encode(kv.commited)
	data := w.Bytes()
	kv.rf.InstallSnapshot(data, kv.lastIndex)
	kv.Log("state size:%d after snapshot\n", kv.persister.RaftStateSize())
}

func (kv *RaftKV) readSnapshot(){

}

func (kv *RaftKV) poll(){
	for{
		select {
			case msg := <-kv.applyCh:
				if msg.UseSnapshot{
					kv.recoverFromSnapshot(msg.Snapshot)
					break
				}
				kv.mu.Lock()
				if kv.lastIndex < msg.Index{
					kv.lastIndex = msg.Index
				}
				kv.mu.Unlock()
				op := msg.Command.(Op)
				ch, isRepeat := kv.checkRepeat(&op)
				if isRepeat{
					continue
				}
				switch op.Cmd{
				case "Put":
					kv.handlePut(&op, ch)
				case "Append":
					kv.handleAppend(&op, ch)
				case "Get":
					kv.handleGet(&op, ch)
				}
			default:
				time.Sleep(time.Millisecond * 10)
				kv.installSnapshot()
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

//We need load snapshot and raft state
//There is window between snapshot and raft state


func (kv *RaftKV) recoverFromSnapshot(snapshot []byte){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot != nil{
		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)
		d.Decode(&kv.commited)
		kv.Log("recover from snapshot\n")
		for k, v := range kv.commited{
			kv.Log("recover key:%s value:%s\n", k, v.Value)
		}
	}
}

func (kv *RaftKV) loadData(){
	data, lastIndex := kv.rf.LoadSnapshot()
	if data != nil{
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&kv.commited)
		for k, v := range kv.commited{
			kv.Log("reboot recover key:%s value:%s\n", k, v.Value)
		}
		for _, op := range kv.commited{
			kv.clientID[op.From] = op.ID
		}
	}
	cmdArray , commitIndex, cmdIndexs:= kv.rf.GetLogs()
	for i, cmd := range cmdArray{
		op := cmd.(Op)
		//BUG: op.ID <= lastIndex, op.ID comes from client, lastIndex is last index of the cmd in snapshot
		//if op.ID > commitIndex || (lastIndex != -1 && op.ID <= lastIndex){
		if op.ID > commitIndex || cmdIndexs[i] <= lastIndex{
			kv.Log("ignore lastindex:%d commd:%v cmdIndex:%d\n", lastIndex, op, cmdIndexs[i])
			continue
		}
		kv.clientID[op.From] = op.ID
		if op.Cmd == "Put"{
			kv.commited[op.Key] = &op
		}
		if op.Cmd == "Append"{
			oldValue := ""
			oldOp, ok := kv.commited[op.Key]
			if ok{
				oldValue = oldOp.Value
				oldOp.Value = oldValue + op.Value
				continue
			}
			kv.commited[op.Key] = &op
		}
		if op.Cmd == "Get"{
			err := ErrNoKey
			oldValue := ""
			oldOp, ok := kv.commited[op.Key]
			if ok{
				err = OK
				oldValue = oldOp.Value
			}
			kv.lastResult[op.From] = &GetReply{Err: Err(err), Value: oldValue, Seq: op.ID, From: kv.me}
		}
	}

	for key, cmd := range kv.commited{
		kv.Log("restore key:%s value:%s\n", key, cmd.Value)
	}
	for from, id := range kv.clientID{
		kv.Log("record from:%d id:%d\n", from, id)
	}
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
	kv.lastResult = make(map[int]*GetReply, 0)
	prefix := fmt.Sprintf("[%05d] ", me)
	kv.log = log.New(os.Stdout, prefix, log.Ldate | log.Lshortfile | log.Lmicroseconds)

	kv.Log("maxraftstate:%d\n", maxraftstate)
	kv.applyCh = make(chan raft.ApplyMsg, 128)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.loadData()
	/*
	cmdArray , commitIndex:= kv.rf.GetLogs()
	for _, cmd := range cmdArray{
		op := cmd.(Op)
		if op.ID > commitIndex{
			continue
		}
		kv.clientID[op.From] = op.ID
		if op.Cmd == "Put"{
			kv.commited[op.Key] = &op
		}
		if op.Cmd == "Append"{
			oldValue := ""
			oldOp, ok := kv.commited[op.Key]
			if ok{
				oldValue = oldOp.Value
				oldOp.Value = oldValue + op.Value
				continue
			}
			kv.commited[op.Key] = &op
		}
		if op.Cmd == "Get"{
			err := ErrNoKey
			oldValue := ""
			oldOp, ok := kv.commited[op.Key]
			if ok{
				err = OK
				oldValue = oldOp.Value
			}
			kv.lastResult[op.From] = &GetReply{Err: Err(err), Value: oldValue, Seq: op.ID, From: kv.me}
		}
	}

	for key, cmd := range kv.commited{
		kv.Log("restore key:%s value:%s\n", key, cmd.Value)
	}
	for from, id := range kv.clientID{
		kv.Log("record from:%d id:%d\n", from, id)
	}
	*/
	go kv.poll()

	return kv
}
