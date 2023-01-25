package raft

import (
	"fmt"
	"time"

	"mit6.824/labrpc"
)

var (
	HEARTBEAT_SLEEP_TIME   = time.Millisecond * 100
	CHECKCOMMIT_SLEEP_TIME = time.Millisecond * 100
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
		select {
		case <-rf.StopHeartBeat:
			fmt.Printf("%d heart beat stopped", rf.me)
			return
		default:
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
				ok := peer.Call("Raft.AppendEntry", args, reply)
				if ok {
					rf.resetTicker()
				}
			}
		}
		time.Sleep(HEARTBEAT_SLEEP_TIME)
	}
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		fmt.Printf("Leader %d has term %d < server %d currentTerm %d\n", args.LeaderId, args.Term, rf.me, rf.CurrentTerm)
		fmt.Printf("reconnect server log:%v\n", rf.Log)
		return
	}
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.CurrentTerm {
		if rf.State == 0 || rf.State == 2 {
			rf.becomeFollower()
		}
	}
	if len(args.Log) == 0 {
		rf.resetTicker()
		go rf.serverCheckCommit(args)
		return
	}

	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// if len(rf.Log) >= args.PrevLogIndex && args.PrevLogIndex > 0 {
	// 	if args.PrevLogTerm != rf.Log[args.PrevLogIndex-1].Term {
	// 		reply.Success = false
	// 		fmt.Printf("%d log doesn't match with %d\n", rf.me, args.LeaderId)
	// 		return
	// 	}
	// }
	if len(rf.Log) < args.PrevLogIndex {
		reply.Success = false
		fmt.Printf("%d log doesn't match with %d\n", rf.me, args.LeaderId)
		return
	}
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if len(rf.Log) >= args.PrevLogIndex && args.PrevLogIndex > 0 {
		if args.PrevLogTerm != rf.Log[args.PrevLogIndex-1].Term {
			rf.Log = rf.Log[:args.PrevLogIndex-1]
			fmt.Printf("%d log conflict\n", rf.me)
		}
	}

	// Append any new entries not already in the log
	rf.Log = append(rf.Log, args.Log...)
	// fmt.Printf("%d append command, term: %d, log index: %d, command is: %v\n",
	// 	rf.me, rf.CurrentTerm, len(args.Log), args.Log[len(args.Log)-1])

	reply.Success = true

	go rf.serverCheckCommit(args)
}

// start aggrement on other servers
func (rf *Raft) startAggrement() {
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		// If last log index ≥ nextIndex for a follower:
		// send AppendEntries RPC with log entries starting at nextIndex
		if len(rf.Log) >= rf.NextIndex[i] {
			// fmt.Printf("before go, %d has next index %d\n", i, rf.NextIndex[i])
			go func(i int, peer *labrpc.ClientEnd) {
			retry:
				logToSent := make([]*logEntry, 0)
				logToSent = append(logToSent, rf.Log[rf.NextIndex[i]-1:]...)
				args := &AppendEntryArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.NextIndex[i] - 1,
					PrevLogTerm:  rf.logTerm(),
					Log:          logToSent,
					LeaderCommit: rf.CommitIndex,
				}
				reply := &AppendEntryReply{-1, false}
				// fmt.Printf("before call, %d has match Index %d, PrevLogIndex=%d\n", i, rf.MatchIndex[i], args.PrevLogIndex)
				// fmt.Printf("call args are: %v\n", args)
				rf.mu.Lock()
				state := rf.State
				rf.mu.Unlock()
				if state != 0 {
					fmt.Printf("%d is not leader, can not call\n", rf.me)
					return
				} else {
					ok := peer.Call("Raft.AppendEntry", args, reply)
					if ok {
						rf.resetTicker()
					}
				}

				// fmt.Printf("%d ask %d to append %v", rf.me, i, logToSent)

				// If successful: update nextIndex and matchIndex for follower
				if reply.Success {
					// MatchIndex = prevLogIndex + len(entries[])
					rf.MatchIndex[i] = args.PrevLogIndex + len(logToSent)
					rf.NextIndex[i] = rf.MatchIndex[i] + 1
					fmt.Printf("reply success, %d 's match Index %d, PrevLogIndex=%d\n", i, rf.MatchIndex[i], args.PrevLogIndex)
				} else if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					fmt.Printf("leader %d match index: %v, next index: %v\n", rf.me, rf.MatchIndex, rf.NextIndex)
					rf.becomeFollower()
					return
				} else if reply.Term == -1 {
					// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
					fmt.Printf("%d log inconsistent with leader %d\n", i, rf.me)
					fmt.Printf("leader %d args: %v\n", rf.me, args)
					if rf.NextIndex[i] > 1 {
						rf.NextIndex[i] -= 1
						fmt.Printf("(term=-1)leader %d match index: %v, next index: %v\n", rf.me, rf.MatchIndex, rf.NextIndex)
						fmt.Printf("%d next index is %d\n", i, rf.NextIndex[i])
					}
					goto retry
				}
			}(i, peer)
		}
	}
}

// If there exists an N such that N > commitIndex,
// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) leaderCheckCommit() {
	for {
		time.Sleep(CHECKCOMMIT_SLEEP_TIME)

		n := rf.CommitIndex + 1
		cnt := 0
		// fmt.Printf("rf.matchindex= %d, n= %d\n", rf.MatchIndex, n)
		for i, _ := range rf.peers {
			if rf.MatchIndex[i] >= n {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2+1 && len(rf.Log) > n-1 {
			if rf.Log[n-1].Term == rf.CurrentTerm {
				rf.CommitIndex = n
				for rf.LastApplied < rf.CommitIndex {
					rf.ApplyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.Log[rf.LastApplied].Command,
						CommandIndex: rf.LastApplied + 1,
					}
					fmt.Printf("%d applied command, term: %d, log term: %d, log index: %d, command:%v\n",
						rf.me, rf.CurrentTerm, rf.Log[rf.LastApplied].Term, rf.LastApplied+1, rf.Log[rf.LastApplied].Command)
					rf.LastApplied++
				}

			}

		}
	}
}

func (rf *Raft) serverCheckCommit(args *AppendEntryArgs) {
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		if args.LeaderCommit <= len(rf.Log) {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = len(rf.Log)
		}
	}
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	for rf.CommitIndex > rf.LastApplied {
		rf.ApplyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.LastApplied].Command,
			CommandIndex: rf.LastApplied + 1,
		}
		fmt.Printf("%d applied command, term: %d, log term: %d, log index: %d, command:%v\n",
			rf.me, rf.CurrentTerm, rf.Log[rf.LastApplied].Term, rf.LastApplied+1, rf.Log[rf.LastApplied].Command)
		rf.LastApplied += 1
	}
}

func (rf *Raft) logTerm() int {
	if len(rf.Log) == 0 {
		return 0
	} else {
		return rf.Log[len(rf.Log)-1].Term
	}
}
