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

import (
	//	"bytes"

	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Log     string
	TermId  int
	Command interface{}
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term    int
	Success bool
}

var LEADER, CANDIDATE, FOLLOWER string = "LEADER", "CANDIDATE", "FOLLOWER"

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//---------------------------------------
	state       string
	currentTerm int
	voteFor     int
	log         []logEntry

	commitIndex int
	lastApplied int

	nextIndex      []int
	matchIndex     []int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm

	//______________________________

	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lstTerm := -1
	if rf.lastLogIndex() >= 0 {
		lstTerm = rf.log[rf.lastLogIndex()].TermId
	}
	fmt.Println("节点", rf.me, "收到来自", args.CandidateId, "的投票请求，当前/请求任期", rf.currentTerm, args.Term, args.LastLogTerm)
	if args.Term < rf.currentTerm ||
		(args.LastLogTerm < lstTerm ||
			args.LastLogTerm == lstTerm && args.LastLogIndex < rf.lastLogIndex()) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Println(rf.me, "requestVote false 请求任期小于当前任期", "logtermId", args.LastLogTerm, rf.log[rf.lastLogIndex()].TermId)
		return
	}
	if args.Term > rf.currentTerm && (args.LastLogTerm > lstTerm || args.LastLogTerm == lstTerm && args.LastLogIndex >= rf.lastLogIndex()) {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = FOLLOWER
	}
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Println(rf.me, "已投票给", rf.voteFor)
		return
	}
	rf.voteFor = args.CandidateId
	rf.electionTimer.Reset(randomElectionTimeOut())
	reply.VoteGranted = true
	fmt.Println(rf.me, "vote for", args.CandidateId)
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		fmt.Println(rf.me, "当前任期大于请求任期，忽略请求")
		return
	}
	rf.electionTimer.Reset(randomElectionTimeOut())
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteFor = -1
		fmt.Println("更新信息")
	}
	if args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogIndex > -1 && rf.log[args.PrevLogIndex].TermId != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		fmt.Println("日志不匹配")
	} else {
		reply.Success = true
		if len(args.Entries) > 0 {
			//不匹配的日志需要被删除
			if rf.lastLogIndex() > 0 && args.PrevLogIndex < rf.lastLogIndex() && args.PrevLogIndex > -1 {
				rf.log = rf.log[:args.PrevLogIndex+1]
				fmt.Println("匹配日志Index", args.PrevLogIndex)
			}
			for item := range args.Entries {
				rf.log = append(rf.log, args.Entries[item])
			}
		}
		if rf.lastLogIndex() < args.LeaderCommit {
			rf.commitIndex = rf.lastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		fmt.Println(time.Now(), rf.me, "收到来自", args.LeaderId, "的心跳，当前/请求任期", rf.currentTerm, args.Term, "当前记录已同步,日志长度", len(rf.log), args.PrevLogIndex+1)
		reply.Term = rf.currentTerm
	}
	// var prevLogTerm int
	// if len(rf.log) > 0 && args.PrevLogIndex > -1 {
	// 	prevLogTerm = rf.log[args.PrevLogIndex].TermId
	// 	if prevLogTerm > args.PrevLogTerm {
	// 		reply.Success = false
	// 		fmt.Println(rf.me, prevLogTerm, args.PrevLogTerm, "当前日志任期大于请求日志任期，忽略请求")
	// 		return
	// 	}
	// }
	//将收到的消息的日志写入自身
}

func randomElectionTimeOut() time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(500))
	return time.Duration(n.Int64()+200) * time.Millisecond
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntriese(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if term, isLeader = rf.GetState(); !isLeader {
		fmt.Println("日志+ not leader", rf.me)
		return index, term, isLeader
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		index = rf.lastLogIndex()
		term = rf.currentTerm
		rf.log = append(rf.log, logEntry{TermId: term, Command: command})
		fmt.Println("日志+", rf.me, "长度", len(rf.log), command)
		return rf.lastLogIndex() + 1, term, isLeader
	}

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			fmt.Println(rf.me, "已是LEADER")
			continue
		}
		rf.state = CANDIDATE
		rf.mu.Unlock()
		go rf.startElection()
	}
}

func (rf *Raft) startElection() {
	//竞选任期号为当前任期+1
	//投票给自己
	//重置计时器
	rf.mu.Lock()
	rf.currentTerm++
	fmt.Println(time.Now(), rf.me, "发起选举，任期", rf.currentTerm)
	rf.voteFor = rf.me
	rf.electionTimer.Reset(randomElectionTimeOut())
	args := RequestVoteArgs{CandidateId: rf.me, Term: rf.currentTerm, LastLogIndex: rf.lastLogIndex(), LastLogTerm: -1}
	if rf.lastLogIndex() >= 0 {
		args.LastLogTerm = rf.log[rf.lastLogIndex()].TermId
	}
	rf.mu.Unlock()

	//向其他server发送requestVoteRPC
	voteGrantedNum := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.state == LEADER {
			break
		}
		go func(i int) {
			reply := RequestVoteReply{}
			fmt.Println("候选人：", rf.me, "投票人：", i)
			rf.sendRequestVote(i, &args, &reply)
			if granted := reply.VoteGranted; !granted {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "FOLLOWER"
					rf.voteFor = -1
				}
				rf.mu.Unlock()
				return
			}

			voteGrantedNum++
			fmt.Println(rf.me, "获得选票数", voteGrantedNum)
			if voteGrantedNum > len(rf.peers)/2 && rf.state == CANDIDATE {
				rf.state = LEADER
				fmt.Println(rf.me, "当选,任期号:", rf.currentTerm)
				rf.mu.Lock()
				//下一个要同步的日志从最乐观的情况开始猜测，即假设每一个peer都拥有最新的日志
				//当前已同步的日志从最悲观的情况开始猜测，即假设没有任何一条日志已同步
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.lastLogIndex() + 1
					rf.matchIndex[i] = -1
				}
				fmt.Println("leader nextIndex重置", rf.nextIndex)
				rf.mu.Unlock()
				rf.startHeartbeat()
			}
		}(i)
	}
}

//在发送心跳时更新nextIndex和matchIndex，但不发送entry
//收到返回值为true的reply时，即可更新matchIndex为上次发送时的nextIndex-1，再更新next

func (rf *Raft) appendEntryBroadcast() {

}

func (rf *Raft) startHeartbeat() {
	fmt.Println(rf.me, "开始发送心跳")
	m := sync.Mutex{}
	c := sync.NewCond(&m)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			c.L.Lock()
			for !rf.killed() && rf.state == LEADER {
				//等待信号唤醒
				c.Wait()
				rf.mu.Lock()
				fmt.Println(rf.me, "向", i, "发送心跳、nextIndex", rf.nextIndex)
				term := rf.currentTerm
				args := AppendEntryArgs{LeaderId: rf.me, Term: term}
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[i] - 1
				fmt.Println(rf.me, "已同步位置", i, args.PrevLogIndex, "已提交", rf.commitIndex)
				if args.PrevLogIndex >= 0 {
					args.PrevLogTerm = rf.log[args.PrevLogIndex].TermId
				}
				if len(rf.log) > 0 && len(rf.log)-1 >= rf.nextIndex[i] {
					args.Entries = rf.log[rf.nextIndex[i] : rf.nextIndex[i]+1]
					// fmt.Println("args.entrys", i, args.Entries[0])
				}
				rf.mu.Unlock()
				reply := AppendEntryReply{}
				go func() {
					rf.sendAppendEntriese(i, &args, &reply)
					if reply.Term == 0 {
						return
					}
					fmt.Println("节点", rf.me, "发送心跳给", i, reply)
					if ok := reply.Success; !ok && rf.nextIndex[i] > 0 {
						rf.nextIndex[i]--
					} else if len(args.Entries) > 0 && reply.Success {
						rf.matchIndex[i] = rf.nextIndex[i]
						rf.nextIndex[i]++
						rf.mu.Lock()
						countSuc := 1
						for j := 0; j < len(rf.peers); j++ {
							if rf.matchIndex[j] >= rf.matchIndex[i] {
								countSuc++
							}
						}
						if countSuc > len(rf.peers)/2 && rf.commitIndex < rf.matchIndex[i] {
							rf.commitIndex = rf.matchIndex[i]
							fmt.Println(rf.me, "日志已提交", rf.commitIndex)
						}
						rf.mu.Unlock()
						// fmt.Println("match index", rf.matchIndex[i])
					}
					if reply.Term > term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.voteFor = -1
						rf.state = FOLLOWER
						rf.mu.Unlock()
						return
					}
				}()
			}

			c.L.Unlock()
			fmt.Println(rf.me, "停止发送心跳")
		}(i)

	}
	//定时发送心跳
	rf.heartbeatTimer = time.NewTimer(time.Millisecond * 50)

	for {
		<-rf.heartbeatTimer.C
		rf.heartbeatTimer.Reset(time.Millisecond * 50)
		if rf.killed() || rf.state != LEADER {
			break
		}
		c.L.Lock()
		c.Broadcast()
		c.L.Unlock()
	}
	fmt.Println("心跳发送异常")
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	dur := randomElectionTimeOut()
	rf.electionTimer = time.NewTimer(dur)
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	rf.voteFor = -1
	rf.commitIndex = -1
	// if rf.me != 0 {
	// 	// rf.electionTimer.Stop()
	// 	rf.electionTimer = *time.NewTimer(time.Hour)
	// }

	// start ticker goroutine to start elections
	go rf.ticker()
	fmt.Println("编号：", rf.me)
	return rf
}
