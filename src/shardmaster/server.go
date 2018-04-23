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
	groupNum int
	freeGroup map[int][]string
	//groupTable map[int]int //key is gid, value is shard number
	groupTable []*GroupInfo
}

type GroupInfo struct{
	gid int
	weight int
	isFree bool
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
	GID  int
	Server string
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
		//GID: args.
	}

	//for debug
	for i, s := range args.Servers{
		op.GID = i
		op.Server = fmt.Sprintf("%s:%s:%s", s[0], s[1], s[2])
	}

	//for debug end
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

func (sm *ShardMaster) getMinAndMax()(*GroupInfo, *GroupInfo){
		min := 10
		max := -1
		maxGid := 0
		minGid := 0
		weight := make(map[int]int)

	lastCfg := &(sm.configs[sm.currentConfigNum])
		for _, g := range lastCfg.Shards{
			weight[g]++
		}
		for g, w := range weight{
			if w < min{
				min = w
				minGid = g
			}
			if w > max{
				max = w
				maxGid = g
			}
		}
		return &GroupInfo{gid: minGid, weight: min}, &GroupInfo{gid: maxGid, weight: max}
}

func (sm *ShardMaster) isBalance(isLeave bool)bool{
	min := 10
	max := -1
	//Choose the group which responsible shards
	//because the group number may be greater than shard number
	for _, g := range sm.groupTable{
		if  isLeave && g.isFree{
			continue
		}
		if g.weight <= min{
			min = g.weight
		}
		if g.weight >= max{
			max = g.weight
		}
	}
	if min == max || max -min == 1{
		return true
	}
	return false
}

func (sm *ShardMaster) rebalance(isLeave bool, leaveGrp *GroupInfo){
	//No more shard to assign to new group
	if sm.groupNum > NShards && !isLeave{
		sm.Log("need not to rebalance. groupNum:%d\n", sm.groupNum)
		return
	}
	lastCfg := &(sm.configs[sm.currentConfigNum])
	sm.Log("rebalance_before:%v leaveGrp:%v isLeave:%v \n", lastCfg.Shards, leaveGrp, isLeave)

	addHeader := false
	if !isLeave && sm.groupNum == 1{
		sm.groupTable = append(sm.groupTable, &GroupInfo{gid: 0, weight:20})
		addHeader = true
	}
	if isLeave && sm.groupNum == 0{
		sm.groupTable = append(sm.groupTable, &GroupInfo{gid:0, weight:10})
		addHeader = true
	}

	for i, g := range sm.groupTable{
		sm.Log("rebalance_n:i:%d %v\n", i, g)
	}
	for !sm.isBalance(isLeave){
		min := 10
		max := -1
		minIdx := 0
	        maxIdx := 0
		for i, g := range sm.groupTable{
			weight := g.weight
			if weight <= min{
				min = weight
				minIdx = i
			}
			if weight >= max{
				max = weight
				maxIdx = i
			}
		}

		minGrp := sm.groupTable[minIdx]
		maxGrp := sm.groupTable[maxIdx]

		for s, gid := range lastCfg.Shards{
			if gid != maxGrp.gid {
				continue
			}
			lastCfg.Shards[s] = minGrp.gid
			minGrp.isFree = false
			minGrp.weight++
			maxGrp.weight--
			sm.Log("weight:%d grp:%d for shard:%d\n", minGrp.weight, minGrp.gid, s)
			break
		}
	}
	if addHeader{
		size := len(sm.groupTable)
		sm.groupTable = append(sm.groupTable[:size-1], sm.groupTable[size:]...)
	}
	sm.Log("rebalance_after:%v leaveGrp:%v\n", lastCfg.Shards, leaveGrp)
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

	//check repeat
	for oldGid, _ := range lastConfig.Groups{
		if gid == oldGid{
			return &JoinReply{Err:OK}
		}
	}
	sm.currentConfigNum++
	config := Config{
		Num:  sm.currentConfigNum,		//int
		//Shards:[NShards]int{gid}, 		//int
		//Groups:args.Servers, //map[int][]string
	}
	sm.groupNum++
	for i, g := range lastConfig.Shards{
		config.Shards[i] = g
	}
	config.Groups = make(map[int][]string, 1)
	sm.groupTable = append(sm.groupTable, &GroupInfo{gid:gid, weight:0, isFree: true})
	for k, v := range lastConfig.Groups{
		config.Groups[k] = v
	}
	config.Groups[gid] = args.Servers[gid]
	//free shard
	sm.Log(" config:%d last shards:%v last groups:%v\n", oldConfigNum, lastConfig.Shards, lastConfig.Groups)
	sm.configs = append(sm.configs, config)
	sm.rebalance(false, &GroupInfo{gid:gid})
	newCfg := &sm.configs[sm.currentConfigNum]
	sm.Log("config_after_join num:%d shards:%v groups:%v grp num:%d\n", sm.currentConfigNum, newCfg.Shards, newCfg.Groups, sm.groupNum)
	return &JoinReply{Err: OK, WrongLeader: false}
}


func (sm *ShardMaster) isGIDValid(args *LeaveArgs)bool{
	curCfg := sm.configs[sm.currentConfigNum]
	/*
	for i := 0; i < sm.groupNum; i++{
		for _, gid := range args.GIDs{
			/*
			if lastConfig.Shards[i] == gid{
				return true
			}
		}
	}
	*/
	for _, gid := range args.GIDs{
		if _, ok := curCfg.Groups[gid]; ok{
			return true
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
		sm.Log("handle_leave not find:%v  config:%d\n", args, sm.currentConfigNum)
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

	for gid, grp := range lastConfig.Groups{
		for _, leaveGid := range args.GIDs{
			if gid == leaveGid{
				sm.groupNum--
				goto next
			}
		}
		newCfg.Groups[gid] = grp
		next:
	}

	//TODO: assume there is only leave gid
	weight := 0
	leaveGid := args.GIDs[0]
	for i, g := range lastConfig.Shards{
		newCfg.Shards[i] = g
		if g == leaveGid{
			weight++
		}
	}

	leaveIdx := 0
	for i, g := range sm.groupTable{
		if g.gid == leaveGid{
			//The move operation rebalanced the shard 
			if !sm.isBalance(true){
				g.weight *= 2
			}else{
				g.weight += weight *2
			}
			leaveIdx = i
			break
		}
	}
	sm.configs = append(sm.configs, newCfg)
	sm.Log("config_before_leave: config:%d shards:%v groups:%v\n", oldConfigNum, lastConfig.Shards, lastConfig.Groups)
	sm.rebalance(true, &GroupInfo{gid: args.GIDs[0]})
	sm.groupTable = append(sm.groupTable[:leaveIdx], sm.groupTable[leaveIdx+1:]...)
	curCfg := &sm.configs[sm.currentConfigNum]
	sm.Log("config_after_leave: config:%d shards:%v group:%v group num:%d\n", sm.currentConfigNum, curCfg.Shards, curCfg.Groups, sm.groupNum)

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
	var dstGrp *GroupInfo
	for _, g := range sm.groupTable{
		if g.gid == arg.GID{
			dstGrp = g
			break
		}
	}

	//now move 1 to 2. shard 1 -> group 2, shard 2-> group 2
	for i, gid := range lastCfg.Shards{
		if i == arg.Shard{
			srcGid := lastCfg.Shards[i]
			newCfg.Shards[i] = arg.GID
			//sm.freeGroup[arg.GID] = lastCfg.Groups[gid]
			newCfg.Groups[gid] = lastCfg.Groups[gid]
			dstGrp.weight++
			for _, g := range sm.groupTable{
				if g.gid == srcGid{
					g.weight--
					break
				}
			}
			continue
		}
		newCfg.Shards[i] = gid
		newCfg.Groups[gid] = lastCfg.Groups[gid]
	}
	sm.configs = append(sm.configs, newCfg)

	sm.Log("config_move_before: num:%d shards:%v config:%v\n", oldConfigNum, lastCfg.Shards, lastCfg.Groups)
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
	sm.freeGroup = make(map[int][]string)
	//sm.groupTable = make(map[int]int)
	sm.groupTable = make([]*GroupInfo, 0)
	go sm.backgroudTask()
	sm.recoverFromDisk()
	return sm
}

func (sm *ShardMaster) Log(format string, arg ...interface{}){
        if Debug > 0{
                sm.log.Printf(format, arg...)
        }
}

