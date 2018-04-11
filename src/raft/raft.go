package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import(
	"time"
	"fmt"
	"math/rand"
	"log"
	"os"
	"sort"
	"sync/atomic"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	stateNone = iota
	stateFollower
	stateCandidate
	stateLeader
)

var stateTable = map[int]string{
	stateFollower: "FOLLOWER",
	stateCandidate: "CANDIDATE",
	stateLeader: "LEADER",
}
const invalidPrevLogIndex = -1
//
// A Go object implementing a single Raft peer.
//
type Command struct{
	Index int
	Term int
	Cmd interface{}
}
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastHeartbeat time.Time
	electionTimeout time.Time
	nextIndex map[int]int
	matchIndex map[int]int
	currentTerm int
	commitIndex int
	voted bool
	state int
	killChan chan bool
	timeoutValue time.Duration
	heartbeatTimeoutValue time.Duration
	votedTerm int // we should rember the votedTerm, we may never vote for others if we don't 
	log map[int]*Command // the index is key
	cmdIndex int
	applyCh chan ApplyMsg
	logHandle *log.Logger
	lastApplied int
}

type Voter struct{
	counted bool
	voted bool
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == stateLeader{
		isleader = true
	}
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	/*
	rf.mu.Lock()
	defer rf.mu.Unlock()
	*/
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voted)
	e.Encode(rf.commitIndex)
	indexs := make([]int, 0)
	logCopy := make(map[int]*Command, len(rf.log) -1)
		for i, cmd := range rf.log{
			if i == 0{
				continue
			}
			logCopy[i] = cmd
			indexs = append(indexs, i)
		}
		e.Encode(logCopy)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.logHandle.Printf("save term:%d voted:%v\n", rf.currentTerm, rf.voted)
	/*
	for _, cmd := range logCopy{
		//rf.logHandle.Printf("save cmd:%v\n", *cmd)
	}
	*/
	sort.Slice(indexs, func(i, j int)bool{ return indexs[i] < indexs[j]})
	//rf.logHandle.Printf("save:%v\n", indexs)
}

//
// restore previously persisted state.
//
func (rf *Raft) ReadPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voted)
	d.Decode(&rf.commitIndex)
	d.Decode(&rf.log)
	rf.logHandle.Printf("load term:%d voted:%v\n", rf.currentTerm, rf.voted)
	index := 0
	indexs := make([]int,0) 
	for i, cmd := range rf.log{
		//rf.logHandle.Printf("load cmd:%v\n", *cmd)
		if cmd.Index > index{
			index = cmd.Index
		}
		indexs = append(indexs, i)
	}
	sort.Slice(indexs, func(i, j int)bool{return indexs[i] < indexs[j]})
	//rf.logHandle.Printf("load cmd:%v\n", indexs)
	rf.cmdIndex = index+1
}


func (rf *Raft) GetLogs()([]interface{}, int){
	if len(rf.log) == 0{
		return nil, -1
	}
	cmdArray := make([]interface{}, len(rf.log) -1)
	indexs := make([]int, 0)
	for i, cmd := range rf.log{
		if cmd.Index == 0{
			continue
		}
		indexs = append(indexs, i)
	}
	sort.Slice(indexs, func(i, j int)bool{return indexs[i] < indexs[j]})

	for i, cmdID := range indexs{
		cmdArray[i] = rf.log[cmdID].Cmd 
	}
	return cmdArray, rf.commitIndex
}
var seqSource uint32 = 0
func getSeq()uint32{
	return atomic.AddUint32(&seqSource, 1)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
	Seq uint32
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
	From int
	Seq uint32
}


type AppendEntryArg struct{
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries []*Command
	CommitIndex int
	Seq uint32
}

type AppendEntryReply struct{
	Term int
	OK bool
	From int
	LastCommitIndex int
}

type CommonReply struct{
	ok bool
	from int
	reply *AppendEntryReply
	arg *AppendEntryArg
}

func (a *AppendEntryArg) String() string{
	return fmt.Sprintf("Term:%d LeaderID:%d PrevLogIndex:%d PrevLogTerm:%d CommitIndex:%d seq:%d",
		a.Term, a.LeaderID, a.PrevLogIndex, a.PrevLogTerm, a.CommitIndex, a.Seq)
}
func (r *RequestVoteArgs) String()string{
	return fmt.Sprintf("Term:%d from:%d LastLogIndex:%d LastLogTerm:%d seq:%d", r.Term, r.CandidateID, r.LastLogIndex, r.LastLogTerm, r.Seq)
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) updateTerm(term int){
	if term < rf.currentTerm{
		return
	}
	rf.currentTerm = term
	rf.persist()
	if rf.state == stateFollower{
		return
	}
	rf.state = stateFollower
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logHandle.Printf("me:%d request vote arg:%s\n", rf.me, args.String())
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	rf.updateTerm(args.Term)
	if rf.voted && rf.votedTerm >= args.Term{
		reply.VoteGranted = false
		//reply.Term = rf.currentTerm
		rf.logHandle.Printf("me:%d i'm already voted\n", rf.me)
		return
	}
	//check logIdx and logTerm
	lastIdx, lastTerm := rf.getLastLogInfo()
	if args.LastLogTerm < lastTerm{
		reply.VoteGranted = false
		rf.logHandle.Printf("candidata's term:%d less than ours:%d\n", args.LastLogTerm, lastTerm)
		return
	}
	if args.LastLogTerm == lastTerm && args.LastLogIndex < lastIdx{
		reply.VoteGranted = false
		rf.logHandle.Printf("candidate's last log id:%d less than ours:%d\n", args.LastLogIndex, lastIdx)
		return
	}
	reply.VoteGranted = true
	rf.voted = true
	rf.votedTerm = rf.currentTerm
	rf.electionTimeout = time.Now()
}

func (rf *Raft) AppendEntry(arg AppendEntryArg, reply *AppendEntryReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logHandle.Printf("me:%d AppendEntryArg:%s entrys:%d\n", rf.me, arg.String(), len(arg.Entries))
	/*
	for _, e := range arg.Entries{
		rf.logHandle.Printf("entry:%v\n", *e)
	}
	*/
	if arg.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.OK = false
		rf.logHandle.Printf("me:%d reject append entry:%s\n", rf.me, arg.String())
		return
	}
	rf.updateTerm(arg.Term)
	rf.electionTimeout = time.Now()
	rf.voted = false
	if arg.PrevLogIndex == invalidPrevLogIndex{
		reply.OK = true
		return
	}
	rf.checkLog(&arg, reply)
}

//TODO: this code is buggy
/*
func (rf *Raft) deleteLogAfter(idx int){
	for idx, c := range rf.log{
		if idx >= c.Index{
			delete(rf.log, idx)
		}
	}
}
*/
func (rf *Raft) deleteLogAfter(idx int){
	for _, c := range rf.log{
		if c.Index >= idx{
			delete(rf.log, c.Index)
		}
	}
}
func (rf *Raft) getMaxIndex()int{
	max := -1
	for idx, _ := range rf.log{
		if idx > max{
			max = idx
		}
	}
	return max
}

func (rf *Raft) commit(start, end int){
	msgs := make([]ApplyMsg, 0)
	for idx, v := range rf.log{
		if idx > start && idx <= end{
			//rf.applyCh <- ApplyMsg{Index: idx, Command:v.Cmd}	
			//rf.logHandle.Printf("me:%d commit cmd:%v at index:%d on term:%d\n", rf.me, v.Cmd, idx, v.Term)
			msgs = append(msgs, ApplyMsg{Index: idx, Command: v.Cmd})
		}
	}
	sort.Slice(msgs, func(i, j int) bool{return msgs[i].Index < msgs[j].Index})
	for _, m := range msgs{
		rf.applyCh <- m
		rf.logHandle.Printf("me:%d commit cmd:%v at index:%d on term:%d s:%d e:%d\n", rf.me, m.Command, m.Index, rf.log[m.Index].Term, start, end)
	}
	for i, _ := range rf.peers{
		rf.nextIndex[i] = end +1
		rf.matchIndex[i] = end
		rf.logHandle.Printf("peer:%d matchIndex:%d\n", i, end)
	}
	//TODO: update our cmd index
	/*
	rf.cmdIndex = end + 1
	rf.logHandle.Printf("cmdIndex:%d end:%d\n", rf.cmdIndex, end)
	*/
}

func (rf *Raft) checkLog(arg *AppendEntryArg, reply *AppendEntryReply){
	rf.logHandle.Printf("me:%d entry:%d\n", rf.me, len(arg.Entries))
	//TODO: this situation will happen when crashed and reboot
	c, ok := rf.log[arg.PrevLogIndex]
	if !ok{
		rf.logHandle.Printf("me:%d hasn't log at index:%d seq:%d\n", rf.me, arg.PrevLogIndex, arg.Seq)
		reply.OK = false
		reply.LastCommitIndex = rf.commitIndex
		if reply.LastCommitIndex == 0{
			reply.LastCommitIndex = 1
		}
		return
	}
	if c.Term != arg.PrevLogTerm{
		rf.logHandle.Printf("me:%d term doesn't match at index:%d my term:%d leader:%d\n", rf.me, arg.PrevLogIndex, c.Term, arg.PrevLogTerm)
		reply.OK = false
		reply.LastCommitIndex = rf.commitIndex
		if reply.LastCommitIndex == 0{
			reply.LastCommitIndex = 1
		}
		return
	}
	//TODO: Entries's number greater than one when we crashed and reboot
	//TODO: log conflict after leader crash
	for _, c := range arg.Entries{
		v, ok := rf.log[c.Index]
		if ok && v.Term != c.Term{
			rf.deleteLogAfter(c.Index)
		}
	}

	for _, c := range arg.Entries{
		rf.log[c.Index] = c
		rf.persist()
		rf.logHandle.Printf("me:%d add new cmd:%v index:%d term:%d\n", rf.me, c.Cmd, c.Index, c.Term)
	}
	max := rf.getMaxIndex()
	rf.cmdIndex = max+1
	//max := rf.getMaxIndex()
	if arg.CommitIndex > rf.commitIndex{
		oldCommit := rf.commitIndex
		rf.commitIndex = arg.CommitIndex
		if rf.commitIndex >max{
			rf.commitIndex = max
		}
		rf.commit(oldCommit, rf.commitIndex)
		//rf.cmdIndex = rf.commitIndex + 1
		/*
		rf.cmdIndex = rf.commitIndex +1
		rf.cmdIndex++
		*/
		rf.logHandle.Printf("cmd index:%d\n", rf.cmdIndex)
	}
	//TODO: let this happen before commit
	/*
	for _, c := range arg.Entries{
		rf.log[c.Index] = c
		rf.persist()
		rf.logHandle.Printf("me:%d add new cmd:%v index:%d term:%d\n", rf.me, c.Cmd, c.Index, c.Term)
	}
	max = rf.getMaxIndex()
	rf.cmdIndex = max+1
	*/
	reply.OK = true
}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, arg AppendEntryArg, reply *AppendEntryReply) bool{
	return rf.peers[server].Call("Raft.AppendEntry", arg, reply)
}


func (rf *Raft) broadcast(handler func(int)){
	for id, _ := range rf.peers{
		if id == rf.me{
			continue
		}
		go handler(id)
	}
}

func (rf *Raft) startVote(){
	rf.mu.Lock()
	if rf.state == stateLeader{
		rf.mu.Unlock()
		return
	}
	now := time.Now()
	if !now.After(rf.electionTimeout.Add(rf.timeoutValue)){
		rf.mu.Unlock()
		return
	}
	rf.logHandle.Printf("me:%d election timeout, start vote\n",rf.me)
	rf.state = stateCandidate
	rf.voted = true
	rf.currentTerm++
	rf.votedTerm = rf.currentTerm
	rf.electionTimeout = now
	lastIdx, lastTerm := rf.getLastLogInfo()
	arg := RequestVoteArgs{
		Term : rf.currentTerm,
		CandidateID: rf.me,
		LastLogIndex: lastIdx,
		LastLogTerm: lastTerm,
	}

	rf.mu.Unlock()
	rf.prepareVote(&arg)
}

func (rf *Raft) prepareVote(arg *RequestVoteArgs){
	majority := len(rf.peers) / 2 + 1
	arg.Seq = getSeq()
	//reply := &RequestVoteReply{}
	resultChan := make(chan *RequestVoteReply, len(rf.peers) -1)
	rf.broadcast(func(id int){
		reply := &RequestVoteReply{}
		if ok := rf.sendRequestVote(id, *arg, reply); !ok{
			rf.logHandle.Printf("me:%d Failed request vote on id:%d arg:%s seq:%d\n", rf.me, id, arg.String(), arg.Seq)
		}
		reply.From = id
		reply.Seq = arg.Seq
		resultChan <-reply})
	num := 1
	resultNum := 0
	//voter := make(map[int]bool)
	voter := make(map[int]*Voter)
	for {

		r := <-resultChan
		if r.VoteGranted{
			_, ok := voter[r.From]
			if !ok{
				voter[r.From] = &Voter{counted:false, voted: true}
			}
			//num++
		}
		rf.logHandle.Printf("me:%d vote reply:%v from :%d seq:%d\n", rf.me, r, r.From, r.Seq)
		//our term out of date, so revert to follower
		rf.mu.Lock()
		if r.Term > rf.currentTerm{
			rf.state = stateFollower
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for _, v := range voter{
			if v.voted && !v.counted{
				num++
				v.counted = true
			}
		}
		resultNum++
		if resultNum >= majority || num >= majority{
			rf.logHandle.Printf("majority:%d voters:%v\n", majority, voter)
			break
		}
	}
	if num < majority{
		return
	}
	rf.mu.Lock()
	//May be our state revert to follower from candidate, after we got majority vote
	//so check again
	state := rf.state
	if rf.state == stateCandidate{
		rf.state = stateLeader
		state = stateLeader
	}
	rf.voted = false
	rf.electionTimeout = time.Now()
	rf.mu.Unlock()
	rf.logHandle.Printf("me:%d current state:%s on term:%d\n", rf.me, stateTable[state], rf.currentTerm)

}

func (rf *Raft) getLastLogID()int{
	prevIndex := -1
	for idx, _ := range rf.log{
		if idx > prevIndex{
			prevIndex = idx
		}
	}
	return prevIndex
}
func(rf *Raft) getLastLogInfo()(int, int){
	prevIndex := rf.getLastLogID()
	return prevIndex, rf.log[prevIndex].Term
}

//TODO: is this prevIndex correct
func (rf *Raft) getPrevLogInfo(isNew bool)(int, int){
	prevIndex := rf.getLastLogID()
	prevTerm := -1
	rf.logHandle.Printf(" new:%v cmd index:%d prevIndex:%d\n", isNew, rf.cmdIndex, prevIndex)
	if isNew{
		prevIndex--
	}
	v, ok := rf.log[prevIndex]
	if ok{
		prevTerm = v.Term
	}
	return prevIndex,prevTerm 
}

func (rf *Raft) heartbeat(){
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.state != stateLeader{
		rf.mu.Unlock()
		return
	}
	now := time.Now()
	if now.Before(rf.lastHeartbeat.Add(rf.heartbeatTimeoutValue)) {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = now
	rf.mu.Unlock()
	rf.logHandle.Printf("heartbeat\n")
	rf.doAppendEntry(false)
}

//TODO: Try to use context
func (rf *Raft) doAppendEntry(isNew bool){
	rf.mu.Lock()
	prevIndex, prevTerm := rf.getPrevLogInfo(isNew)
	args := make(map[int]*AppendEntryArg, len(rf.peers) -1)
	for idx, _ := range rf.peers{
		if idx == rf.me{
			continue
		}
		nextIdx := rf.nextIndex[idx]
		arg := AppendEntryArg{
			Term : rf.currentTerm,
			LeaderID: rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm: prevTerm,
			Entries : nil,
			CommitIndex: rf.commitIndex,
		}
		arg.Entries = make([]*Command, 0)
		for i, c := range rf.log{
			if i < nextIdx{
				continue 
			}
			//rf.logHandle.Printf("add entry:%v, nextIdx:%d to node:%d\n", c, nextIdx, idx)
			arg.Entries = append(arg.Entries, c)
		}//end for

		//TODO: let the crashed node cache up
		//TODO: case 2: leader start many commands before followr receive
		//so arg.PrevLogIndex = nextIdx -1
		if nextIdx < prevIndex{
			arg.PrevLogIndex = nextIdx -1
			cmd, ok := rf.log[nextIdx -1]
			term := -1
			if ok{
				term = cmd.Term
			}
			//arg.PrevLogTerm = rf.log[nextIdx -1].Term
			arg.PrevLogTerm = term
			rf.logHandle.Printf("prev index:%d prev term:%d node:%d\n", arg.PrevLogIndex, arg.PrevLogTerm,idx)
		}
		/*
		*/
		args[idx] = &arg
	}
	if isNew{
		rf.lastHeartbeat = time.Now()
	}
	rf.mu.Unlock()
	seq := getSeq()
	for i, arg := range args{
		arg.Seq = seq
		go rf.doAppendBottom(i, arg, seq)
	}
}

func (rf *Raft) doAppendBottom(serverID int, arg *AppendEntryArg, seq uint32){
	reply := &AppendEntryReply{}
	ok := rf.sendAppendEntry(serverID, *arg, reply)
	//TODO: Failed to send and failed to reply
	if !ok{
		rf.logHandle.Printf("failed to append entry to :%d seq:%d \n", serverID, seq)
		return
	}
	if !rf.isLeader(reply.Term) || len(arg.Entries) == 0{
		return
	}
	if !reply.OK{
		rf.mu.Lock()
		//TODO: nextIndex can't less than 1
		if rf.nextIndex[serverID] > 0{
			//rf.nextIndex[serverID]--
			if reply.LastCommitIndex != 0{
				rf.nextIndex[serverID] = reply.LastCommitIndex
			}
			rf.logHandle.Printf("update nextIndex:%d seq:%d node:%d\n", rf.nextIndex[serverID], seq, serverID)

		}
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	nextIdx  := rf.nextIndex[serverID]
	matchIdx  := rf.matchIndex[serverID]
	//TODO: if we don't use oldNext, there is bug
	//bug description: the next idx is 4 when we sent [1, 2, 3] successfully,
	//Then we send [1, 2, 3] again, the next idx will add one
	//oldNext := nextIdx
	incrNext := false
	for _, v := range arg.Entries{
		i := v.Index
		if i >= nextIdx {
			nextIdx = i
			incrNext = true
		}
		if i > matchIdx{
			matchIdx = i
		}
	}
	rf.logHandle.Printf("matchIndex:%d nextIdx:%d seq:%d node:%d\n", matchIdx, nextIdx, seq, serverID)
	if incrNext{
		rf.nextIndex[serverID] = nextIdx + 1
	}
	rf.matchIndex[serverID] = matchIdx
	rf.mu.Unlock()
	rf.leaderCommit()
}

//TODO: The slower node may affect us
func (rf *Raft) handleResult(resultChan <-chan *CommonReply){
}

func (rf *Raft) leaderCommit(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majority := len(rf.peers)/2 + 1
	oldCommit := rf.commitIndex
	for{
		passedNum := 0
		for _, i := range rf.matchIndex{
			//if i > rf.commitIndex && rf.log[i].Term == rf.currentTerm{
			if i > rf.commitIndex{
				passedNum++
			}
		}
		if passedNum < majority{
			break
		}
		rf.commitIndex++
	}
	if oldCommit == rf.commitIndex{
		return
	}
	/*
	*/
	rf.persist()
	msgs := make([]ApplyMsg, 0)
	for i, v := range rf.log{
		if i > oldCommit && i <= rf.commitIndex{
			msgs = append(msgs, ApplyMsg{Command: v.Cmd, Index: v.Index})
		}
	}

	sort.Slice(msgs, func(i, j int) bool{return msgs[i].Index < msgs[j].Index})
	for _, v := range msgs{
		rf.applyCh <- v
		rf.logHandle.Printf("leader commit cmd:%v at index:%d on term:%d s:%d e:%d\n", v.Command, v.Index, rf.log[v.Index].Term, oldCommit, rf.commitIndex)
	}
}

func (rf *Raft) isLeader(term int)bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm{
		rf.state = stateFollower
		rf.electionTimeout = time.Now()
		return false
	}
	return true
}

func (rf *Raft) updateIndex(from int, reply *AppendEntryReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[from] = reply.LastCommitIndex
}

func (rf *Raft) backgroundTask(){
	for{
		select{
			case <-rf.killChan:
				return
			default:
				rf.startVote()
				rf.heartbeat()
				time.Sleep(10 *time.Millisecond)
		}

	}
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != stateLeader{
		return index, term, false
	}
	index = rf.cmdIndex
	term = rf.currentTerm
	cmd := &Command{Index:index,Cmd:command, Term: term}
	rf.log[index] = cmd
	rf.logHandle.Printf("****************me:%d start cmd:%v on index:%d at term:%d\n", rf.me, command, index, term)
	rf.cmdIndex++
	rf.matchIndex[rf.me] = index
	rf.persist()
	go rf.doAppendEntry(true)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.killChan <- true
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here.
	rf.log = make(map[int]*Command, 0)
	rf.state = stateFollower
	rf.killChan = make(chan bool)
	start := me + 2
	v := rand.Intn(start * rand.Intn(100000)) % 500
	if v == 0{
		v = 2
	}
	for v < 100 {
		v *=  start
	}
	rf.timeoutValue = time.Duration(v) * time.Millisecond
	rf.heartbeatTimeoutValue = 40 * time.Millisecond
	rf.electionTimeout = time.Now()
	rf.lastHeartbeat = time.Now()
	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.nextIndex = make(map[int]int, len(peers))
	rf.matchIndex = make(map[int]int, len(peers))
	rf.log[0] = &Command{ Index:0, Term: -1, Cmd: nil} //place holder
	rf.cmdIndex = 1
	rf.logHandle = log.New(os.Stdout, fmt.Sprintf("[me:%03d] ", rf.me), log.Ltime | log.Lmicroseconds | log.Lshortfile)
	rf.logHandle.Printf("me:%d timeout value:%d\n", rf.me, v)
	for idx, _ := range rf.peers{
		rf.nextIndex[idx] = 1
		rf.matchIndex[idx] = 0
	}
	go rf.backgroundTask()
	// initialize from state persisted before a crash
	rf.ReadPersist(persister.ReadRaftState())


	return rf
}
