package raft

import (
	"time"

	"mit6.824/labrpc"
)

var (
	HEARTBEAT_SLEEP_TIME = time.Millisecond * 100
)

type AppendEntryArgs struct {
	Term         int         // leader’s term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIndex entry
	Log          []*logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex
}

type AppendEntryReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) heartBeat() {
	for {
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			args := &AppendEntryArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.Log),
				PrevLogTerm:  rf.logTerm(),
				//Log: ,
				LeaderCommit: rf.CommitIndex,
			}
			reply := &AppendEntryReply{-1, false}
			go func(peer *labrpc.ClientEnd) {
				ok := peer.Call("Raft.AppendEntry", args, reply)
				if ok {
					rf.resetTicker()
				}
			}(peer)
		}
		time.Sleep(HEARTBEAT_SLEEP_TIME)
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.resetTicker()
	if args.Term > rf.CurrentTerm {
		if rf.State == 0 || rf.State == 2 {
			rf.becomeFollower()
		}
	} else if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	for args.LeaderCommit > rf.LastApplied {
		rf.LastApplied += 1
		rf.ApplyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.LastApplied],
			CommandIndex: len(rf.Log),
		}
	}
}

func (rf *Raft) logTerm() int {
	if len(rf.Log) == 0 {
		return 0
	} else {
		return rf.Log[len(rf.Log)-1].Term
	}
}
