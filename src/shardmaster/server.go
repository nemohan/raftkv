package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "log"
import "fmt"
import "os"
import(
	"time"
)

type PendingOP struct{
	op *Op
	replyChan chan interface{}
}
type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	currentConfigNum int
	log *log.Logger
	shardNum int
	lastRequestID map[int]int
	replyChan chan interface{} //assume only one request 
	commited map[int]*Op
	pendingList map[uint32] *PendingOP
}

const Debug =1

type Op struct {
	// Your data here.	
	CMD string
	Seq uint32
	//Arg interface{}
	Join *JoinArgs
	Query *QueryArgs
	Leave *LeaveArgs
	Move *MoveArgs
	From int
}

const (
	cmdLeave = "LEAVE"
	cmdJoin = "JOIN"
	cmdMove = "MOVE"
	cmdQuery = "QUERY"
)

func (sm *ShardMaster) parseReply(reply interface{}) interface{}{
	return nil

}

func (sm *ShardMaster) addPending(op *Op) chan interface{}{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.Log("add pending req:%v\n", op)
	replyChan := make(chan interface{}, 1)
	sm.pendingList[op.Seq] = &PendingOP{
		op: op,
		replyChan : replyChan, 
	}

	return replyChan
}
func (sm *ShardMaster) removePending(op *Op) chan interface{}{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	v, ok := sm.pendingList[op.Seq]
	if !ok{
		return nil
	}
	delete(sm.pendingList, op.Seq)
	return v.replyChan
}

func (sm *ShardMaster) performRequest(op *Op) interface{}{
	wrongLeader := false
	//leaderID := -1
	err := Err(OK)
	/*
	newOp := Op{CMD: op.CMD,
		Seq: op.Seq,
		Arg: op.Arg,
	}
	*/
	var replyChan chan interface{}
	_, _, isLeader := sm.rf.Start(*op)
	if !isLeader{
		//leaderID = sm.rf.GetLeader()
		wrongLeader = true
		goto out

	}
	replyChan = sm.addPending(op)
	//var reply interface{}
	select{
		case reply := <-replyChan:
			sm.Log("reply:%v\n", reply)
			return reply
		case <-time.After(time.Millisecond *200):
			sm.Log("timeout for op:%v\n", op)
			err = ErrTimeout
			sm.removePending(op)
	}

	out:
	//var r interface{}
	var reply interface{}
	switch op.CMD{
		case cmdJoin:
			r := &JoinReply{}
			r.Err = err
			r.WrongLeader = wrongLeader
			r.Seq = op.Seq
			reply = r
		case cmdLeave:
			r := &LeaveReply{}
			r.Err = err
			r.WrongLeader = wrongLeader
			r.Seq = op.Seq
			reply = r
		case cmdQuery:
			r := &QueryReply{}
			r.Err = err
			r.WrongLeader = wrongLeader
			r.Seq = op.Seq
			reply = r
		case cmdMove:
			r := &MoveReply{
				Err:err,
				WrongLeader: wrongLeader,
				Seq: op.Seq,} 
			reply = r
	}
	return reply
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.Log("join args:%v\n", args)
	//TODO: this code is mesteries
	op := &Op{
		CMD : cmdJoin,
		From: 0,
		Seq: args.Seq,
		Join: args,
	}

	//TODO: can't use interface in op
	/*
	copyArg := JoinArgs{Seq: args.Seq, Servers: make(map[int][]string, 0)}
	for k, v := range args.Servers{
		copyArg.Servers[k] = v
	}
	op.Arg = copyArg
	*/
	//op.Arg = Info{Info: "nihao"}
	//op.Arg = map[int]string{1: "ni", 2:"hao"} 
	r := sm.performRequest( op)
	v := r.(*JoinReply)
	reply.Err = v.Err
	reply.WrongLeader = v.WrongLeader
}

func (sm *ShardMaster) rebalance( /*newShards []int*/){
	gids := make([]int, 0)
	shards := &(sm.configs[sm.currentConfigNum].Shards)
	for i := 0; i < sm.shardNum; i++{
		gids = append(gids, shards[i])
	}

	for i := sm.shardNum; i < NShards; i++{
		if sm.shardNum == 0{
			shards[i] = 0
			continue
		}
		shards[i] = gids[i%sm.shardNum]	
	}
	sm.Log("rebalance: shards:%v\n", shards)
}


func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.Log("leave args:%v\n", args)
	op := &Op{
		CMD : cmdLeave,
		From: 0,
		//Arg: args,
		Leave: args,
		Seq: args.Seq,
	}
	r := sm.performRequest( op)
	v := r.(*LeaveReply)
	reply.Err = v.Err
	reply.WrongLeader = v.WrongLeader
	reply.Seq = v.Seq
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.Log("move args:%v\n", args)
	op := &Op{
		CMD : cmdMove,
		From: 0,
		Move: args,
		Seq: args.Seq,
	}
	r := sm.performRequest( op)
	v := r.(*MoveReply)
	reply.Err = v.Err
	reply.WrongLeader = v.WrongLeader
	reply.Seq = v.Seq
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code hJoinReplyJoinReplyere.
	sm.Log("query args:%v num:%d seq:%d\n", args, args.Num, args.Seq)
	op := &Op{
		CMD : cmdQuery,
		From: 0,
		//Arg: args,
		Query: args,
		Seq: args.Seq,
	}
	r := sm.performRequest( op)
	v := r.(*QueryReply)
	reply.Err = v.Err
	reply.WrongLeader = v.WrongLeader
	reply.Seq = v.Seq
	reply.Config = v.Config
	sm.Log("query_reply:%v\n", reply)
}


func (sm *ShardMaster) handleJoin( op *Op)interface{}{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	args := op.Join
	sm.Log("handle_join:%v\n", args)
	gid := 0

	oldConfigNum := sm.currentConfigNum
	lastConfig := sm.configs[oldConfigNum]
	for k, _ := range args.Servers{
		gid = k
	}

	for _, oldGid := range lastConfig.Shards{
		if oldGid == gid{
			return &JoinReply{Err: OK, WrongLeader: false}
		}
	}
	sm.currentConfigNum++
	config := Config{
		Num:  sm.currentConfigNum,		//int
		//Shards:[NShards]int{gid}, 		//int
		//Groups:args.Servers, //map[int][]string
	}

	for i := 0; i < sm.shardNum; i++{
		config.Shards[i] = lastConfig.Shards[i]
	}
	config.Shards[sm.shardNum] = gid
	sm.shardNum++
	config.Groups = make(map[int][]string, 1)
	for k, v := range lastConfig.Groups{
		config.Groups[k] = v
	}
	config.Groups[gid] = args.Servers[gid]
	//free shard
	sm.Log(" config:%d last shards:%v last groups:%v\n", oldConfigNum, lastConfig.Shards, lastConfig.Groups)
	sm.configs = append(sm.configs, config)
	sm.rebalance()
	newCfg := &sm.configs[sm.currentConfigNum]
	sm.Log("config_after_join num:%d shards:%v groups:%v\n", sm.currentConfigNum, newCfg.Shards, newCfg.Groups)
	return &JoinReply{Err: OK, WrongLeader: false}
}


func (sm *ShardMaster) isGIDValid(args *LeaveArgs)bool{
	lastConfig := sm.configs[sm.currentConfigNum]
	for i := 0; i < sm.shardNum; i++{
		for _, gid := range args.GIDs{
			if lastConfig.Shards[i] == gid{
				return true
			}
		}
	}
	return false
}

func (sm *ShardMaster) handleLeave(op *Op)interface{}{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	args := op.Leave
	sm.Log("handle_leave: leave args:%v\n", args)

	if !sm.isGIDValid(args){
		return &LeaveReply{
			Seq: op.Seq,
			WrongLeader:false,
			Err: OK,}
	}
	oldConfigNum := sm.currentConfigNum
	sm.currentConfigNum++
	newCfg := Config{Num: sm.currentConfigNum,
		Groups: make(map[int][]string, 1),
		}
	lastConfig := sm.configs[oldConfigNum]

	newShardNum := sm.shardNum


	j := 0
	for i := 0; i < newShardNum; i++{
		oldGid := lastConfig.Shards[i]
		for _, gid := range args.GIDs{
			if gid == oldGid{
				goto next
			}
		}
		newCfg.Shards[j] = oldGid
		j++
		newCfg.Groups[oldGid] = lastConfig.Groups[oldGid]
		sm.shardNum--
		next:
	}

	sm.configs = append(sm.configs, newCfg)
	sm.Log("config_before_leave: config:%d shards:%v groups:%v\n", oldConfigNum, lastConfig.Shards, lastConfig.Groups)
	sm.rebalance()
	curCfg := &sm.configs[sm.currentConfigNum]
	sm.Log("config_after_leave: config:%d shards:%v group:%v\n", sm.currentConfigNum, curCfg.Shards, curCfg.Groups)

	return &LeaveReply{
		Seq: op.Seq,
		WrongLeader:false,
		Err: OK,}
}
func (sm *ShardMaster) handleQuery(op *Op)interface{}{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	args := op.Query
	sm.Log("handle_query:%v NUM:%d seq:%d config num:%d\n", args, args.Num, args.Seq, len(sm.configs))
	reply := &QueryReply{}
	if args.Num < 0 || args.Num > sm.currentConfigNum{
		reply.Config = sm.configs[sm.currentConfigNum]
		reply.Err = OK
		return reply
	}
	for _, cfg := range sm.configs{
		sm.Log("after_query  seq:%d cfg:%v \n", args.Seq, cfg)
	}
	reply.Config = sm.configs[args.Num]
	reply.Err = OK
	reply.Seq = op.Seq
	return reply
}

func (sm *ShardMaster) handleMove(op *Op)interface{}{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	arg := op.Move
	sm.Log("handle_move, arg:%v arg:%v\n", op, arg)
	oldConfigNum := sm.currentConfigNum
	lastCfg := sm.configs[oldConfigNum]
	for i, gid := range lastCfg.Shards{
		if i == arg.Shard && gid == arg.GID{
			return &MoveReply{Err: OK, Seq: op.Seq}
		}
	}
	sm.currentConfigNum++
	newCfg := Config{
		Num: sm.currentConfigNum,
	}
	newCfg.Groups = make(map[int][]string, len(lastCfg.Groups))

	//move the shard to new group, and remove old shard
	//before move:
	//shard 1 -> group 1
	//shard 2 -> group 2

	//now move 1 to 2. shard 1 -> group 2, shard 2-> group 2 
	for i, gid := range lastCfg.Shards{
		if i == arg.Shard{
			newCfg.Shards[i] = arg.GID
			continue
		}
		newCfg.Shards[i] = gid
		newCfg.Groups[gid] = lastCfg.Groups[gid]
	}
	sm.configs = append(sm.configs, newCfg)

	sm.Log("config_move_before: num:%d shards:%v config:%v\n", oldConfigNum, lastCfg.Shards, lastCfg.Groups)
	//sm.rebalance()
	sm.Log("config_move_after: num:%d shards:%v config:%v\n", sm.currentConfigNum,
		sm.configs[sm.currentConfigNum].Shards, sm.configs[sm.currentConfigNum].Groups)
	return &MoveReply{Err:OK, Seq: op.Seq}
}

func (sm *ShardMaster) backgroudTask(){
	for{
		select {
			case msg := <-sm.applyCh:
				v := msg.Command.(Op)
				op := &v
				replyChan := sm.removePending(op)
				var reply interface{}
				switch op.CMD{
					case cmdJoin:
						reply = sm.handleJoin(op)
					case cmdLeave:
						reply = sm.handleLeave(op)
					case cmdQuery:
						reply = sm.handleQuery(op)
					case cmdMove:
						reply = sm.handleMove(op)
				}
				if replyChan != nil{
					//sm.Log("block before :%d\n", op.Seq)
					replyChan <- reply
					//sm.Log("block after\n")
				}
			default:
		}

	}

}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) recoverFromDisk(){
	log, commitIndex, _ := sm.rf.GetLogs()
	for idx, c := range log{
		cmd := c.(Op)
		if idx > commitIndex || cmd.CMD == cmdQuery{
			continue
		}
		sm.applyCh <- raft.ApplyMsg{Index: idx, Command: c}
		sm.Log("replay cmd:%v\n", c)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	prefix := fmt.Sprintf("shard[%06d] ", me)
	sm.log = log.New(os.Stdout, prefix, log.Ldate | log.Lshortfile | log.Lmicroseconds)
	// Your code here.
	//sm.configs[0].Groups[0] = nil 
	sm.replyChan = make(chan interface{})
	sm.pendingList = make(map[uint32] *PendingOP, 0)
	sm.lastRequestID = make(map[int]int)
	//sm.Log("reboot\n")
	//data := persister.ReadRaftState()
	go sm.backgroudTask()
	sm.recoverFromDisk()
	return sm
}

func (sm *ShardMaster) Log(format string, arg ...interface{}){
        if Debug > 0{
                sm.log.Printf(format, arg...)
        }
}

