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

	"bytes"
	"crypto/rand"
	"encoding/gob"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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

type SnapShot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
}

type logEntry struct {
	Index   int
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
	SnapShot     []byte
}
type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapShotReply struct {
	Term int
}
type AppendEntryReply struct {
	Term         int
	Success      bool
	LastLogIndex int
}

var LEADER, CANDIDATE, FOLLOWER string = "LEADER", "CANDIDATE", "FOLLOWER"

var enableSnapshoting bool = false

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	peersMu   []sync.Mutex

	//---------------------------------------
	state       string
	CurrentTerm int
	VoteFor     int
	Log         []logEntry

	CommitIndex        int
	lastApplied        int
	LogIndexOfSnapshot int
	TermOfSnapshot     int

	nextIndex      []int
	matchIndex     []int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	logSyncChan    []chan int
	applyChan      chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.state == LEADER
}

func (rf *Raft) lastLogIndex() int {
	if rf == nil || len(rf.Log) == 0 {
		return rf.LogIndexOfSnapshot
	}
	return rf.LogIndexOfSnapshot + len(rf.Log)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	byt, err := StatEncoder(rf)
	if err == nil {
		rf.persister.SaveRaftState(byt)

	}
	// fmt.Print("stat saved", err, rf.persister.raftstate, byt)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// fmt.Println("read persist", data)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	var rf2 Raft
	StatDecoder(data, &rf2)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf2.Log != nil {
		rf.Log = rf2.Log
	}
	rf.CurrentTerm = rf2.CurrentTerm
	// rf.peers = rf2.peers
	rf.VoteFor = rf2.VoteFor
	rf.LogIndexOfSnapshot = rf2.LogIndexOfSnapshot
	rf.TermOfSnapshot = rf2.TermOfSnapshot
	rf.CommitIndex = rf2.CommitIndex
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	fmt.Println(rf.me, "开始安装快照", snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndexS int
	var xlog []interface{}
	if d.Decode(&lastIncludedIndexS) == nil &&
		d.Decode(&xlog) == nil {
		fmt.Println(rf.me, "安装快照成功", lastIncludedIndexS, xlog)
		rf.Log = make([]logEntry, 0)
		// rf.Log = xlog
		rf.LogIndexOfSnapshot = lastIncludedIndex
		rf.TermOfSnapshot = lastIncludedTerm
		rf.CommitIndex = rf.LogIndexOfSnapshot
		return true
	}
	return false
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Println(rf.me, "log长度", len(rf.Log), "当前提交index", index, "rf.logIndexOfSnapshot:", rf.LogIndexOfSnapshot)
	if len(rf.Log) > 0 && index-1 > rf.LogIndexOfSnapshot {

		byt, err := StatEncoder(rf)
		if err == nil {
			rf.persister.SaveStateAndSnapshot(byt, snapshot)
			fmt.Println(rf.me, "快照存储成功", snapshot)
		}
		rf.TermOfSnapshot = rf.Log[index-rf.LogIndexOfSnapshot-2].TermId
		if !enableSnapshoting {
			return
		}
		if rf.lastLogIndex() <= index-1 {
			rf.Log = make([]logEntry, 0)
			fmt.Println(rf.me, "log clear")
		} else {
			rf.Log = rf.Log[index-rf.LogIndexOfSnapshot-2:]
			fmt.Println(rf.me, "log not clear,len", len(rf.Log))
		}
		rf.LogIndexOfSnapshot = index - 1
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	rf.applyChan <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: args.Term}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lstTerm := -1
	if rf.lastLogIndex() >= 0 {
		if rf.lastLogIndex() > rf.LogIndexOfSnapshot {
			lstTerm = rf.Log[rf.lastLogIndex()-rf.LogIndexOfSnapshot-1].TermId
		} else {
			lstTerm = rf.TermOfSnapshot
		}
	}
	if args.Term <= rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	fmt.Println("节点", rf.me, "收到来自", args.CandidateId, "的投票请求，当前/请求任期", rf.CurrentTerm, args.Term, lstTerm, args.LastLogTerm)
	rf.state = CANDIDATE
	if args.LastLogTerm > lstTerm || args.LastLogTerm == lstTerm && args.LastLogIndex >= rf.lastLogIndex() {
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		rf.state = FOLLOWER
		if rf.VoteFor != -1 && rf.VoteFor != args.CandidateId {
			reply.VoteGranted = false
			reply.Term = rf.CurrentTerm
			fmt.Println(rf.me, "已投票给", rf.VoteFor)
			return
		}
		rf.VoteFor = args.CandidateId
		rf.electionTimer.Reset(randomElectionTimeOut())
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		fmt.Println(rf.me, "vote for", args.CandidateId)
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		fmt.Println("节点", rf.me, "收到来自", args.CandidateId, "的投票请求，日志不匹配，当前/请求任期", rf.CurrentTerm, args.Term, lstTerm, args.LastLogTerm, args.LastLogIndex, rf.lastLogIndex())
		return
	}

}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}
	rf.electionTimer.Reset(randomElectionTimeOut())
	// fmt.Println(rf.me, "收到", args.LeaderId, "的心跳")
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.state = FOLLOWER
		rf.VoteFor = -1
	}
	reply.Term = rf.CurrentTerm
	reply.LastLogIndex = rf.lastLogIndex()
	if args.LeaderCommit < -1 {
		reply.Success = true
		return
	}
	if args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogIndex > rf.LogIndexOfSnapshot && rf.Log[args.PrevLogIndex-rf.LogIndexOfSnapshot-1].TermId != args.PrevLogTerm {
		fmt.Println(time.Now(), rf.me, "收到来自", args.LeaderId, "的不匹配日志，当前/请求任期", rf.CurrentTerm, args.Term,
			"rf.lastLogIndex", rf.lastLogIndex(),
			"args.PrevLogIndex", args.PrevLogIndex,
			"args.PrevLogTerm", args.PrevLogTerm)
		reply.Success = false
		return
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		fmt.Println(rf.me, "日志更新")
		//不匹配的日志需要被删除
		if rf.lastLogIndex() > 0 && args.PrevLogIndex < rf.lastLogIndex() && args.PrevLogIndex > -1 {
			fmt.Println(rf.me, rf.Log, "b")
			rf.Log = rf.Log[:args.PrevLogIndex-rf.LogIndexOfSnapshot]
			fmt.Println(rf.me, rf.Log, "a")
		}
		for item := range args.Entries {
			rf.Log = append(rf.Log, args.Entries[item])
		}
		go rf.persist()
	}
	cmtIdxO := rf.CommitIndex
	if rf.lastLogIndex() < args.LeaderCommit {
		rf.CommitIndex = rf.lastLogIndex()
	} else {
		rf.CommitIndex = args.LeaderCommit
	}
	if rf.CommitIndex >= 0 && rf.CommitIndex != cmtIdxO && rf.CommitIndex > rf.LogIndexOfSnapshot {
		rf.applyLog(cmtIdxO)
	}
	fmt.Println(time.Now(), rf.me, "收到来自", args.LeaderId, "的日志，当前/请求任期", rf.CurrentTerm, args.Term, "lastLogIndex", rf.lastLogIndex(), "nextIndex:", args.PrevLogIndex+1)

}

func (rf *Raft) SendApp(msg ApplyMsg) {
	rf.applyChan <- msg
	fmt.Println(rf.me, "发送反馈消息完成", msg)
}

func StatEncoder(stat interface{}) ([]byte, error) {
	if stat == nil {
		return nil, bytes.ErrTooLarge
	}
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(stat)
	return buf.Bytes(), err
}

func StatDecoder(data []byte, to interface{}) error {
	red := bytes.NewReader(data)
	return gob.NewDecoder(red).Decode(to)
}

func randomElectionTimeOut() time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(300))
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
func (rf *Raft) sendInstallSnap(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
		return index, term, isLeader
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		term = rf.CurrentTerm
		rf.Log = append(rf.Log, logEntry{Index: rf.lastLogIndex(), TermId: term, Command: command})
		go func() {
			rf.logSyncSignal()
			rf.persist()
		}()
		fmt.Println("日志+", rf.me, "长度", len(rf.Log), command)
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
		<-rf.electionTimer.C
		fmt.Println(rf.me, "选举计时器到期")
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			fmt.Println(rf.me, "已是LEADER")
			rf.electionTimer.Reset(randomElectionTimeOut())
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
	rf.CurrentTerm++
	fmt.Println(time.Now(), rf.me, "发起选举，任期", rf.CurrentTerm)
	rf.VoteFor = rf.me
	rf.electionTimer.Reset(randomElectionTimeOut())
	args := RequestVoteArgs{CandidateId: rf.me, Term: rf.CurrentTerm, LastLogIndex: rf.lastLogIndex(), LastLogTerm: -1}
	args.LastLogTerm = rf.getTermId(rf.lastLogIndex())
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
			rf.sendRequestVote(i, &args, &reply)
			if granted := reply.VoteGranted; !granted {
				rf.mu.Lock()
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.state = "FOLLOWER"
					rf.VoteFor = -1
				}
				rf.mu.Unlock()
				return
			}
			voteGrantedNum++
			fmt.Println(rf.me, "获得选票数", voteGrantedNum)
			if voteGrantedNum > len(rf.peers)/2 && rf.state == CANDIDATE {
				rf.state = LEADER
				fmt.Println(rf.me, "当选,任期号:", rf.CurrentTerm)
				rf.mu.Lock()
				//下一个要同步的日志从最乐观的情况开始猜测，即假设每一个peer都拥有最新的日志
				//当前已同步的日志从最悲观的情况开始猜测，即假设没有任何一条日志已同步
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.lastLogIndex() + 1
					rf.matchIndex[i] = -1
				}
				fmt.Println("leader nextIndex重置", rf.nextIndex)
				rf.mu.Unlock()
				go rf.syncLog()
				rf.startHeartbeat()
			}
		}(i)
	}
}

func (rf *Raft) getTermId(index int) int {
	if index < 0 {
		return 0
	}
	if index > rf.LogIndexOfSnapshot {
		return rf.Log[index-rf.LogIndexOfSnapshot-1].TermId
	} else {
		return rf.TermOfSnapshot
	}

}

func (rf *Raft) syncLog() {
	fmt.Println(rf.me, "开始复制日志")
	cmttd := -1
	for i := range rf.peers {
		if i != rf.me {
			rf.logSyncChan[i] <- 1
		}
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for range rf.logSyncChan[i] {
				// for len(rf.logSyncChan[i]) > 0 {
				// 	fmt.Println("chan 中仍有消息")
				// 	// <-rf.logSyncChan[i]
				// }
				if rf.state != LEADER {
					fmt.Println("当前节点不是LEADER")
					return
				}
				if rf.nextIndex[i]-1 > rf.lastLogIndex() {
					fmt.Println("当前节点无日志可复制")
					continue
				}
				rf.mu.Lock()
				term := rf.CurrentTerm
				args := AppendEntryArgs{LeaderId: rf.me, Term: term}
				reply := AppendEntryReply{}
				args.LeaderCommit = rf.CommitIndex
				args.PrevLogIndex = rf.nextIndex[i] - 1
				fmt.Println(rf.me, "已同步位置", i, args.PrevLogIndex, "已提交", rf.CommitIndex)
				if args.PrevLogIndex-rf.LogIndexOfSnapshot > 0 && args.PrevLogIndex <= rf.lastLogIndex() {
					args.PrevLogTerm = rf.Log[args.PrevLogIndex-rf.LogIndexOfSnapshot-1].TermId
				} else if args.PrevLogIndex-rf.LogIndexOfSnapshot == 0 {
					args.PrevLogTerm = rf.TermOfSnapshot
				}
				if len(rf.Log) > 0 && rf.lastLogIndex() >= rf.nextIndex[i] && rf.nextIndex[i] >= 0 {
					fmt.Println("复制日志", rf.me, i, "logIndexOfSnapshot", rf.LogIndexOfSnapshot)
					args.Entries = rf.Log[rf.nextIndex[i]-rf.LogIndexOfSnapshot-1 : rf.nextIndex[i]-rf.LogIndexOfSnapshot]
				}
				rf.mu.Unlock()
				msgOk := rf.sendAppendEntriese(i, &args, &reply)
				if reply.Term == 0 || !msgOk {
					rf.logSyncChan[i] <- 1
					// rf.mu.Unlock()
					continue
				}
				if ok := reply.Success; !ok && rf.nextIndex[i] > 0 {
					if rf.nextIndex[i] > reply.LastLogIndex {
						rf.nextIndex[i] = reply.LastLogIndex
					} else {
						rf.nextIndex[i]--
					}
				} else if len(args.Entries) > 0 && reply.Success {
					rf.matchIndex[i] = rf.nextIndex[i]
					rf.nextIndex[i]++
					countSuc := 1
					for j := 0; j < len(rf.peers); j++ {
						if rf.matchIndex[j] >= rf.matchIndex[i] {
							countSuc++
						}
					}
					cmtIdxO := rf.CommitIndex
					rf.mu.Lock()
					if countSuc > len(rf.peers)/2 && rf.CommitIndex < rf.matchIndex[i] && rf.CommitIndex <= rf.lastLogIndex() {
						rf.CommitIndex = rf.matchIndex[i]
						rf.applyLog(cmtIdxO)
						fmt.Println("额外日志复制，commit，", rf.me, rf.CommitIndex)
						rf.logSyncSignal()
						if cmttd < rf.CommitIndex {
							cmttd = rf.CommitIndex
						}
					}
					rf.mu.Unlock()
				}
				if rf.nextIndex[i] <= rf.lastLogIndex() {
					rf.logSyncChan[i] <- 1
				}
				if reply.Term > term {
					rf.CurrentTerm = reply.Term
					rf.VoteFor = -1
					rf.state = FOLLOWER
					return
				}
				// rf.mu.Unlock()
			}
		}(i)
	}

}

func (rf *Raft) logSyncSignal() {
	for i := range rf.peers {
		if i != rf.me {
			rf.logSyncChan[i] <- 1
		}
	}
}

func (rf *Raft) applyLog(cmtIdxO int) {
	for ci := cmtIdxO + 1; ci <= rf.CommitIndex; ci++ {
		if rf.CommitIndex > rf.lastLogIndex() {
			return
		}
		fmt.Println(rf.me, "日志已提交", ci, rf.LogIndexOfSnapshot, rf.lastLogIndex())
		rf.SendApp(ApplyMsg{CommandValid: true, CommandIndex: ci + 1, Command: rf.Log[ci-rf.LogIndexOfSnapshot-1].Command})
	}
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
				term := rf.CurrentTerm
				args := AppendEntryArgs{LeaderId: rf.me, Term: term}
				reply := AppendEntryReply{}
				args.LeaderCommit = -10
				args.PrevLogIndex = rf.nextIndex[i] - 1
				if enableSnapshoting && args.PrevLogIndex < rf.LogIndexOfSnapshot && args.PrevLogIndex >= 0 {
					go func() {
						insArgs := InstallSnapShotArgs{Term: rf.CurrentTerm,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.LogIndexOfSnapshot + 1,
							LastIncludedTerm:  rf.TermOfSnapshot,
							Data:              rf.persister.ReadSnapshot()}
						fmt.Println("待同步", args.PrevLogIndex, insArgs)
						insRply := InstallSnapShotReply{}
						rf.sendInstallSnap(i, &insArgs, &insRply)
						rf.matchIndex[i] = rf.LogIndexOfSnapshot
						rf.nextIndex[i] = rf.LogIndexOfSnapshot + 1
					}()
					rf.mu.Unlock()
					go rf.sendAppendEntriese(i, &args, &reply)
				} else {
					rf.mu.Unlock()
					go func() {
						msgOk := rf.sendAppendEntriese(i, &args, &reply)
						if reply.Term == 0 || !msgOk {
							return
						}
						if reply.Term > term {
							rf.CurrentTerm = reply.Term
							rf.VoteFor = -1
							rf.state = FOLLOWER
							return
						}
					}()
				}
			}

			c.L.Unlock()
			fmt.Println(rf.me, "停止发送心跳")
		}(i)

	}
	//定时发送心跳
	rf.heartbeatTimer = time.NewTimer(time.Millisecond * 150)

	for {
		<-rf.heartbeatTimer.C
		rf.heartbeatTimer.Reset(time.Millisecond * 150)
		if rf.killed() || rf.state != LEADER {
			break
		}
		c.L.Lock()
		c.Broadcast()
		c.L.Unlock()
	}
	fmt.Println(rf.me, "心跳发送异常")
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
	rf.peersMu = make([]sync.Mutex, len(peers))
	rf.logSyncChan = make([]chan int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.logSyncChan[i] = make(chan int, 1000)
	}

	// initialize from state persisted before a crash
	rf.VoteFor = -1
	rf.CommitIndex = -1
	rf.LogIndexOfSnapshot = -1
	rf.applyChan = applyCh
	// if rf.me != 0 {
	// 	// rf.electionTimer.Stop()
	// 	rf.electionTimer = *time.NewTimer(time.Hour)
	// }

	rf.readPersist(rf.persister.raftstate)
	fmt.Println(rf.me, "读取快照，log", rf.Log, rf.LogIndexOfSnapshot)
	// start ticker goroutine to start elections
	go rf.ticker()
	fmt.Println("编号：", rf.me)
	return rf
}
