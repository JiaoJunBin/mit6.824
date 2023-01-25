package raft

import (
	// "fmt"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"mit6.824/labrpc"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.resetTicker()
	// fmt.Printf("%d ask %d to vote, with term %d\n", args.CandidateId, rf.me, args.Term)
	// Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == rf.CurrentTerm { // avoid double vote
		// If votedFor is null or candidateId,
		//and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
			if args.LastLogTerm >= rf.logTerm() && args.LastLogIndex >= len(rf.Log) {
				rf.VotedFor = args.CandidateId
				reply.VoteGranted = true
				rf.CurrentTerm = args.Term
				// fmt.Printf("%d voted for %d, and changed to term %d\n", rf.me, args.CandidateId, rf.CurrentTerm)
			}
		}
	} else {
		if args.LastLogTerm >= rf.logTerm() && args.LastLogIndex >= len(rf.Log) {
			rf.VotedFor = args.CandidateId
			reply.VoteGranted = true
			rf.CurrentTerm = args.Term
			// fmt.Printf("%d voted for %d, and changed to term %d\n", rf.me, args.CandidateId, rf.CurrentTerm)
		}
	}
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.State = 2 // candidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.resetTicker()

	vote := 1  // number of vote candidate has got
	voter := 1 // number of server who have voted
	var mu sync.Mutex
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateId:  rf.me,
			LastLogTerm:  rf.logTerm(),
			LastLogIndex: len(rf.Log),
		}
		reply := &RequestVoteReply{-1, false}

		go func(peer *labrpc.ClientEnd) {
			ok := peer.Call("Raft.RequestVote", args, reply)
			if ok {
				rf.resetTicker()
			}
			mu.Lock()
			defer mu.Unlock()
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.becomeFollower()
			}
			if reply.VoteGranted {
				vote += 1
			}
			voter += 1
		}(peer)
	}

	minRequire := len(rf.peers)/2 + 1
	for {
		if vote+len(rf.peers)-voter < minRequire {
			return
		}
		if vote >= minRequire {
			rf.becomeLeader()
			break
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.State = 0
	fmt.Printf("%d become leader\n", rf.me)
	// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
	// repeat during idle periods to prevent election timeouts
	go rf.heartBeat()

	rf.reinitVolatileState()
}

// Reinitialized after election
func (rf *Raft) reinitVolatileState() {
	for i := range rf.peers {
		fmt.Printf("re-initing\n")
		if len(rf.NextIndex) <= i {
			rf.NextIndex = append(rf.NextIndex, len(rf.Log)+1)
			rf.MatchIndex = append(rf.MatchIndex, 0)
		} else {
			rf.NextIndex[i] = len(rf.Log) + 1
			rf.MatchIndex[i] = 0
		}

	}
}

func (rf *Raft) becomeFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.State = 1
	rf.StopHeartBeat <- true
	fmt.Printf("%d become follower\n", rf.me)

}

func (rf *Raft) resetTicker() {
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(MAX_ELECTION_TIMEOUT-MIN_ELECTION_TIMEOUT) + MIN_ELECTION_TIMEOUT // MIN ~ MAX
	rf.ElectionTicker.Reset(time.Millisecond * time.Duration(r))
}
