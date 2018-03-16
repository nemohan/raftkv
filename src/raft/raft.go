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

// import "bytes"
// import "encoding/gob"
import(
	"time"
	"fmt"
	"math/rand"
	"log"
	"os"
	"sort"
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
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
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
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
	From int
}


type AppendEntryArg struct{
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	Entries []*Command
	CommitIndex int
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
	return fmt.Sprintf("Term:%d LeaderID:%d PrevLogIndex:%d PrevLogTerm:%d CommitIndex:%d",
		a.Term, a.LeaderID, a.PrevLogIndex, a.PrevLogTerm, a.CommitIndex)
}
func (r *RequestVoteArgs) String()string{
	return fmt.Sprintf("Term:%d from:%d LastLogIndex:%d LastLogTerm:%d", r.Term, r.CandidateID, r.LastLogIndex, r.LastLogTerm)
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) updateTerm(term int){
	if term < rf.currentTerm{
		return
	}
	rf.currentTerm = term
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
	rf.logHandle.Printf("me:%d AppendEntryArg:%s\n", rf.me, arg.String())	
	if arg.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.OK = true	
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

func (rf *Raft) deleteLogAfter(idx int){
	for idx, c := range rf.log{
		if idx >= c.Index{
			delete(rf.log, idx)
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
		rf.logHandle.Printf("me:%d commit cmd:%v at index:%d on term:%d\n", rf.me, m.Command, m.Index, rf.log[m.Index].Term)
	}
	for i, _ := range rf.peers{
		rf.nextIndex[i] = end +1
		rf.matchIndex[i] = end
	}
}

func (rf *Raft) checkLog(arg *AppendEntryArg, reply *AppendEntryReply){
	rf.logHandle.Printf("me:%d entry:%d\n", rf.me, len(arg.Entries))
	//TODO: this situation will happen when crashed and reboot
	c, ok := rf.log[arg.PrevLogIndex]
	if !ok{
		rf.logHandle.Printf("me:%d hasn't log at index:%d\n", rf.me, arg.PrevLogIndex)
		reply.OK = false
		reply.LastCommitIndex = rf.commitIndex
		return
	}
	if c.Term != arg.PrevLogTerm{
		rf.logHandle.Printf("me:%d term doesn't match at index:%d my term:%d leader:%d\n", rf.me, arg.PrevLogIndex, c.Term, arg.PrevLogTerm)
		reply.OK = false
		reply.LastCommitIndex = rf.commitIndex
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

	max := rf.getMaxIndex()
	if arg.CommitIndex > rf.commitIndex{
		oldCommit := rf.commitIndex
		rf.commitIndex = arg.CommitIndex
		if rf.commitIndex >max{
			rf.commitIndex = max
		}
		rf.commit(oldCommit, rf.commitIndex)
		rf.cmdIndex = rf.commitIndex
	}
	for _, c := range arg.Entries{
		rf.log[c.Index] = c
		rf.logHandle.Printf("me:%d add new cmd:%v index:%d term:%d\n", rf.me, c.Cmd, c.Index, c.Term)
	}
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
	reply := &RequestVoteReply{}
	resultChan := make(chan *RequestVoteReply, len(rf.peers) -1)
	rf.broadcast(func(id int){
		if ok := rf.sendRequestVote(id, *arg, reply); !ok{
			rf.logHandle.Printf("me:%d Failed request vote on id:%d arg:%s\n", rf.me, id, arg.String())
		}
		reply.From = id
		resultChan <-reply})
	num := 1
	resultNum := 0
	for {

		r := <-resultChan
		if r.VoteGranted{
			num++
		}
		rf.logHandle.Printf("me:%d vote reply:%v from :%d\n", rf.me, r, r.From)
		//our term out of date, so revert to follower
		rf.mu.Lock()
		if r.Term > rf.currentTerm{
			rf.state = stateFollower
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		resultNum++
		if resultNum >= majority || num >= majority{
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

func (rf *Raft) getPrevLogInfo()(int, int){
	prevIndex := rf.getLastLogID()
	prevTerm := -1
	prevIndex--
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
	rf.doAppendEntry()
}

//TODO: Try to use context
func (rf *Raft) doAppendEntry(){
	prevIndex, prevTerm := rf.getPrevLogInfo()
	//resultChan := make(chan *CommonReply, len(rf.peers) -1)
	rf.mu.Lock()
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
			rf.logHandle.Printf("add entry:%v, nextIdx:%d to node:%d\n", c, nextIdx, idx)
			arg.Entries = append(arg.Entries, c)
		}//end for
		//TODO: let the crashed node cache up
		if nextIdx < prevIndex{
			arg.PrevLogIndex = nextIdx
			arg.PrevLogTerm = rf.log[nextIdx].Term
		}
		/*
		reply := &AppendEntryReply{}
		go func(serverID int){
			ok := rf.sendAppendEntry(serverID, arg, reply)
			r := &CommonReply{ok: ok, 
				from:serverID, 
				reply:reply,
				arg: &arg,
				}
			resultChan <- r
		}(idx)
		*/
		args[idx] = &arg
	}
	rf.mu.Unlock()
	//rf.handleResult(resultChan)
	for i, arg := range args{
		go rf.doAppendBottom(i, arg)
	}
}

func (rf *Raft) doAppendBottom(serverID int, arg *AppendEntryArg){
	reply := &AppendEntryReply{}
	ok := rf.sendAppendEntry(serverID, *arg, reply)
	if !ok{
		rf.logHandle.Printf("failed to append entry to :%d\n", serverID)
		return
	}
	if !rf.isLeader(reply.Term) || len(arg.Entries) == 0{
		return
	}
	if !reply.OK{
		rf.mu.Lock()
		rf.nextIndex[serverID]--
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	nextIdx  := rf.nextIndex[serverID]
	matchIdx  := rf.matchIndex[serverID]
	for _, v := range arg.Entries{
		i := v.Index
		if i > nextIdx {
			nextIdx = i
		}
		if i > matchIdx{
			matchIdx = i
		}
	}
	rf.nextIndex[serverID] = nextIdx + 1
	rf.matchIndex[serverID] = matchIdx
	rf.mu.Unlock()
	rf.leaderCommit()
}

//TODO: The slower node may affect us
func (rf *Raft) handleResult(resultChan <-chan *CommonReply){
	num := 0
	max := len(rf.peers) -1
	nextIdx := 0
	matchIdx := 0
	for c := range resultChan{
		//Failed to send request
		num++
		from := c.from
		reply := c.reply
		arg := c.arg
		if !c.ok{
			rf.logHandle.Printf("failed to append entry to :%d\n", c.from)
			goto next
		}
		if !rf.isLeader(reply.Term){
			return
		}
		if !reply.OK{
			rf.mu.Lock()
			rf.nextIndex[from]--
			rf.mu.Unlock()
			goto next
		}
		if len(arg.Entries) == 0{
			goto next
		}
		nextIdx  = rf.nextIndex[from]
		matchIdx  = rf.matchIndex[from]
		for _, v := range arg.Entries{
			i := v.Index
			if i > nextIdx {
				nextIdx = i
			}
			if i > matchIdx{
				matchIdx = i
			}
		}
		rf.nextIndex[from] = nextIdx + 1
		rf.matchIndex[from] = matchIdx
		next:
		if num == max{
			break
		}
	}

	rf.leaderCommit()
}

func (rf *Raft) leaderCommit(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	majority := len(rf.peers)/2 + 1
	oldCommit := rf.commitIndex
	for{
		passedNum := 0
		for _, i := range rf.matchIndex{
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
	for i, v := range rf.log{
		if i > oldCommit && i <= rf.commitIndex{
			rf.applyCh <- ApplyMsg{Command: v.Cmd, Index: v.Index}
			rf.logHandle.Printf("leader commit cmd:%v at index:%d on term:%d\n", v.Cmd, v.Index, v.Term)
		}
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
	go rf.doAppendEntry()
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
	rf.readPersist(persister.ReadRaftState())


	return rf
}
