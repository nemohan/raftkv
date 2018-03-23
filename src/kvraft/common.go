package raftkv

import "fmt"
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string


// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq int
	From int
}

func (p *PutAppendArgs) String()string{
	return fmt.Sprintf("key:%s value:%s op:%s seq:%d from:%d\n", p.Key, p.Value, p.Op, p.Seq, p.From)
}
type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Seq         int
	To	    int 
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq int
	From int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Seq         int
	From 	    int
}
