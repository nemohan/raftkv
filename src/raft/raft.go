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
//
// A Go object implementing a single Raft peer.
//
type Command struct{
	index int
	term int
	cmd interface{}
}
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	heartbeatTimeout time.Time
	electionTimeout time.Time
	nextIndexs []int
	currentTerm int
	commitIndex int
	voted bool
	state int
	killChan chan bool
	timeoutValue time.Duration
	heartbeatTimeoutValue time.Duration
	votedTerm int // we should rember the votedTerm, we may never vote for others if we don't 
	//log map[int]*Command // the index is key
	log []*Command
	cmdIndex int
	applyCh chan ApplyMsg
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
}

type CommonReply struct{
	ok bool
	from int
	reply *AppendEntryReply
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
	
	fmt.Printf("me:%d request vote arg:%s\n", rf.me, args.String())
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	rf.updateTerm(args.Term)
	if rf.voted && rf.votedTerm >= args.Term{
		reply.VoteGranted = false
		//reply.Term = rf.currentTerm
		fmt.Printf("me:%d i'm already voted\n", rf.me)
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
	fmt.Printf("me:%d AppendEntryArg:%s\n", rf.me, arg.String())	
	if arg.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.OK = true	
		fmt.Printf("me:%d reject append entry:%s\n", rf.me, arg.String())
		return
	}
	rf.updateTerm(arg.Term)	
	rf.electionTimeout = time.Now()
	rf.voted = false
	
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
	fmt.Printf("me:%d election timeout, start vote\n",rf.me)
	rf.state = stateCandidate
	rf.voted = true
	rf.currentTerm++
	rf.votedTerm = rf.currentTerm
	rf.electionTimeout = now   	
	arg := RequestVoteArgs{
		Term : rf.currentTerm,
		CandidateID: rf.me,
		LastLogIndex: 0,
		LastLogTerm: 0,
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
			fmt.Printf("me:%d Failed request vote on id:%d arg:%s\n", rf.me, id, arg.String())
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
		fmt.Printf("me:%d vote reply:%v from :%d\n", rf.me, r, r.From)
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
	fmt.Printf("me:%d current state:%s on term:%d\n", rf.me, stateTable[state], rf.currentTerm)

}


func (rf *Raft) heartbeat(cmd *Command){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != stateLeader{
		return
	}
	now := time.Now()
	if now.Before(rf.heartbeatTimeout.Add(rf.heartbeatTimeoutValue)){
		return
	}
	rf.heartbeatTimeout = now
	arg := AppendEntryArg{
		Term : rf.currentTerm,
		LeaderID: rf.me,
		PrevLogIndex: -1
		PrevLogTerm: -1,
		Entries : nil,
		CommitIndex: rf.commitIndex,
	}
	if cmd != nil{
		arg.Entries = make([]*Command, 1)
		arg.Entries[0] = cmd 
	}
	logSize := len(rf.log)
	if len(rf.log) != 0{
		arg.PrevLogIndex = rf.log[logSize -1].index
		arg.PrevLogTerm = rf.log[logSize-1].term
	}
	rf.prepareAppend(&arg, cmd)
}

func (rf *Raft) prepareAppend(arg *AppendEntryArg, cmd *Command){
	majority := len(rf.peers) /2 + 1
	reply := &AppendEntryReply{} 
	resultChan := make(chan *CommonReply, len(rf.peers) -1)
	rf.broadcast(func(id int){
		ok := rf.sendAppendEntry(id, *arg, reply)
		r := &CommonReply{ok: ok, from:id, reply:reply}
		resultChan <- r})

	num := 0
	for{
		r1 := <-resultChan
		r := r1.reply
		if !r1.ok && cmd != nil{
			rf.nextIndexs[r1.from] = cmd.index
			fmt.Printf("me:%d failed append entry:%v\n", rf.me, r)
		}
		if r.Term > rf.currentTerm{
			rf.state = stateFollower
			rf.electionTimeout = time.Now()
			return
		}
		num++
		if num == len(rf.peers) -1{
			break
		}
	}

	if num < majority{
		return
	}
	if cmd == nil{
		return
	}
	rf.commitIndex++
	rf.applyCh <- ApplyMsg{Command: rf.log[rf.commitIndex].cmd, Index:rf.commitIndex}	
}
func (rf *Raft) backgroundTask(){
	for{
		select{
			case <-rf.killChan:
				return 
			default:
				rf.startVote( )	
				rf.heartbeat(nil)		
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
	cmd := &Command{index:index,cmd:command, term: term} 
	//rf.log[index] = cmd 
	rf.log = append(rf.log, cmd)
	go rf.heartbeat(cmd)	
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
	//rm.log = make(map[int]*Command, 0)
	rf.log = make([]*Command, 0)
	rf.state = stateFollower
	rf.killChan = make(chan bool)
	start := me + 2
	v := rand.Intn(start * rand.Intn(100000)) % 500
	if v == 0{
		v = 2
	}
	for v < 200 {
		v *=  start
	}
	rf.timeoutValue = time.Duration(v) * time.Millisecond 
	rf.heartbeatTimeoutValue = 50 * time.Millisecond
	fmt.Printf("me:%d timeout value:%d\n", rf.me, v)	
	rf.electionTimeout = time.Now()
	rf.commitIndex = -1
	rf.applyCh = applyCh
	go rf.backgroundTask()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
