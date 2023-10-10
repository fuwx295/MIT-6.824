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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	Time        time.Time // check leader elect timeout
	state       int       // server state
	currentTerm int       // term
	votedFor    int       // voted to which server
	voteCount   int       // receive vote number
	logs        []Log     // log entry

	// 2B
	commitIndex int // commit
	lastApplied int
	nextIndex   []int         // nextIndex send to follower
	matchIndex  []int         // follower match Index
	applyCh     chan ApplyMsg // send to service command apply

	// 2D
	Lastindex int // snap last index
	LastTerm  int // snap last term

}

type Log struct {
	Command interface{}
	Term    int
	// Index int
}

// server state
const (
	Follower = iota
	Candidate
	Leader
)

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) == 1 {
		return rf.LastTerm
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) getLastIndex() int {
	return rf.Lastindex + len(rf.logs) - 1
}

func (rf *Raft) getLogTerm(index int) int {
	if index > rf.Lastindex {
		return rf.logs[index-rf.Lastindex].Term
	}
	return rf.LastTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	rf.persister.Save(rf.getRaftState(), rf.persister.snapshot)
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.Lastindex)
	e.Encode(rf.LastTerm)
	return w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votefor int     //Votefor
	var currentTerm int //term
	var logs []Log      //Logs
	var index int       //index
	var term int        //term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if d.Decode(&votefor) != nil ||
		d.Decode(&currentTerm) != nil || d.Decode(&logs) != nil || d.Decode(&index) != nil || d.Decode(&term) != nil {
	} else {
		rf.votedFor = votefor
		rf.currentTerm = currentTerm
		rf.logs = logs
		rf.Lastindex = index
		rf.LastTerm = term
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.Lastindex || index > rf.commitIndex {
		return
	}
	// Clipping log
	count := 1
	oldIndex := rf.Lastindex
	for offset, value := range rf.logs {
		if offset == 0 {
			continue
		}
		count++
		rf.Lastindex = offset + oldIndex
		rf.LastTerm = value.Term
		if offset+oldIndex == index {
			break
		}
	}

	newLog := make([]Log, 1)
	newLog = append(newLog, rf.logs[count:]...)
	rf.logs = newLog

	rf.persister.Save(rf.getRaftState(), snapshot)
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
	// overdue vote
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// log unqualified
	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastIndex()) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.state = Follower
			rf.persist()
		}
		return
	}

	// receive a vote should stay follower and vote
	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		rf.state = Follower
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.Time = time.Now()
		rf.persist()
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// vote rule
	if !ok || rf.state != Candidate || reply.Term != rf.currentTerm {
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		// exceed half win the election
		if rf.voteCount > len(rf.peers)/2 {
			rf.SetLeader()
		}
	} else {
		// vote rule
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.Time = time.Now()
			rf.persist()
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Index   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// term not match
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.Time = time.Now()
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	} else {
		//term equal
		rf.Time = time.Now()
		rf.state = Follower
		reply.Term = args.Term
	}

	//lack some logs
	if rf.getLastIndex() < args.PrevLogIndex {

		reply.Index = rf.getLastIndex()
		return
	}

	if rf.Lastindex > args.PrevLogIndex {
		if args.PrevLogIndex+len(args.Entries) <= rf.Lastindex {
			reply.Index = rf.Lastindex
			return
		}
		args.PrevLogTerm = args.Entries[rf.Lastindex-args.PrevLogIndex-1].Term
		args.Entries = args.Entries[rf.Lastindex-args.PrevLogIndex:]
		args.PrevLogIndex = rf.Lastindex
	}

	if args.PrevLogTerm != rf.getLogTerm(args.PrevLogIndex) {
		reply.Index = rf.lastApplied
		if reply.Index > rf.Lastindex {
			reply.Index = rf.Lastindex
		}
		if reply.Index > args.PrevLogIndex-1 {
			reply.Index = args.PrevLogIndex - 1
		}
		return
	}

	// sync sucess
	reply.Success = true
	//latest condition
	if rf.getLastIndex() == args.PrevLogIndex && args.PrevLogTerm == rf.getLastLogTerm() {
		if args.LeaderCommit > rf.commitIndex {
			tmp := rf.getLastIndex()
			if tmp > args.LeaderCommit {
				tmp = args.LeaderCommit
			}
			rf.commitIndex = tmp
		}
	}
	//heart beat
	if len(args.Entries) == 0 {
		return
	}
	// overdue entries
	if rf.getLastIndex() >= args.PrevLogIndex+len(args.Entries) && rf.getLogTerm(args.PrevLogIndex+len(args.Entries)) == args.Entries[len(args.Entries)-1].Term {
		return
	}

	i := args.PrevLogIndex + 1
	for i <= rf.getLastIndex() && i-args.PrevLogIndex-1 < len(args.Entries) {
		break
	}
	if i-args.PrevLogIndex-1 >= len(args.Entries) {
		return
	}
	// append to itself
	rf.logs = rf.logs[:i-rf.Lastindex]
	rf.logs = append(rf.logs, args.Entries[i-args.PrevLogIndex-1:]...)
	// commit and persist
	if args.LeaderCommit > rf.commitIndex {
		tmp := rf.getLastIndex()
		if tmp > args.LeaderCommit {
			tmp = args.LeaderCommit
		}
		rf.commitIndex = tmp
	}
	rf.persist()

}

func (rf *Raft) sendAppenEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	//log.Println("send hb")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if !reply.Success {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1

			rf.persist()
		} else {
			args.PrevLogIndex = reply.Index
			if args.PrevLogIndex < 0 {
				return
			}
			if args.PrevLogIndex-rf.Lastindex < 0 {
				// send snap (2D)
			} else {
				// retry
				args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
				entry := make([]Log, rf.getLastIndex()-args.PrevLogIndex)
				copy(entry, rf.logs[args.PrevLogIndex-rf.Lastindex+1:])
				args.Entries = entry
				// go syncLog
				go rf.sendAppenEntries(server, args, reply)
			}
		}

	} else {
		// sync log success
		if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			// commit log
			rf.UpdateCommit()
		}
		if rf.nextIndex[server] < args.PrevLogIndex+len(args.Entries)+1 {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		}
	}
}

type InstallSnapshotRPC struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	//offset           int
	Data []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapshotRPC, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	if args.LastIncludeIndex <= rf.Lastindex {
		return
	}
	rf.Time = time.Now()
	// remove old log
	tmpLog := make([]Log, 1)
	if rf.getLastIndex() > args.LastIncludeIndex+1 {
		tmpLog = append(tmpLog, rf.logs[args.LastIncludeIndex+1-rf.Lastindex:]...)
	}
	rf.Lastindex = args.LastIncludeIndex
	rf.LastTerm = args.LastIncludeTerm
	rf.logs = tmpLog

	if args.LastIncludeIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludeIndex
	}
	if args.LastIncludeIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludeIndex
	}

	rf.persister.Save(rf.getRaftState(), args.Data)
	msg := ApplyMsg{
		Snapshot:      args.Data,
		SnapshotValid: true,
		SnapshotTerm:  rf.LastTerm,
		SnapshotIndex: rf.Lastindex,
	}
	go func() { rf.applyCh <- msg }()
}

func (rf *Raft) sendInstallsnapshot(server int, args *InstallSnapshotRPC, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	} else {
		if rf.matchIndex[server] < args.LastIncludeIndex {
			rf.matchIndex[server] = args.LastIncludeIndex
			rf.UpdateCommit()
		}
		if rf.nextIndex[server] < args.LastIncludeIndex {
			rf.nextIndex[server] = args.LastIncludeIndex + 1
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		return index, term, false
	}

	newLog := Log{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, newLog)
	rf.persist()

	rf.matchIndex[rf.me] = len(rf.logs) - 1 + rf.Lastindex
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if rf.matchIndex[i] < rf.Lastindex {
			//do nothing
		} else {
			entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])
			copy(entry, rf.logs[rf.matchIndex[i]+1-rf.Lastindex:])
			// sync Log
			nargs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.matchIndex[i],
				PrevLogTerm:  rf.getLogTerm(rf.matchIndex[i]),
				LeaderCommit: rf.commitIndex,
				Entries:      entry,
			}
			go rf.sendAppenEntries(i, &nargs, &AppendEntriesReply{})
		}
	}
	return len(rf.logs) - 1 + rf.Lastindex, newLog.Term, isLeader
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

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	// vote to itself
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, &args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) Broadcast() {
	if rf.state != Leader {
		return
	}
	prelogindex := rf.getLastIndex()
	prelogterm := rf.getLastLogTerm()
	rf.UpdateCommit()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// log not match
		if (rf.nextIndex[i] <= prelogindex || rf.nextIndex[i]-rf.matchIndex[i] != 1) && rf.getLastIndex() != 0 {
			if rf.matchIndex[i] < rf.Lastindex {
				// need log is remove
				// send snapshot (2D)
				margs := InstallSnapshotRPC{
					Term:             rf.currentTerm,
					LeaderId:         rf.me,
					LastIncludeIndex: rf.Lastindex,
					LastIncludeTerm:  rf.LastTerm,
					Data:             rf.persister.snapshot,
				}

				go rf.sendInstallsnapshot(i, &margs, &InstallSnapshotReply{})

			} else {

				// send synclog
				entry := make([]Log, rf.getLastIndex()-rf.matchIndex[i])
				copy(entry, rf.logs[rf.matchIndex[i]+1-rf.Lastindex:])

				nargs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.matchIndex[i],
					PrevLogTerm:  rf.getLogTerm(rf.matchIndex[i]),
					LeaderCommit: rf.commitIndex,
					Entries:      entry,
				}
				go rf.sendAppenEntries(i, &nargs, &AppendEntriesReply{})
			}

		} else {
			// log match send heartbeat
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prelogindex,
				PrevLogTerm:  prelogterm,
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppenEntries(i, &args, &AppendEntriesReply{})
		}
	}
}

func (rf *Raft) SetLeader() {
	rf.state = Leader
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLastIndex() + 1
	}
	rf.matchIndex[rf.me] = rf.getLastIndex()
	rf.nextIndex[rf.me] = rf.getLastIndex() + 1
	rf.Broadcast()
}

func randomTimeout() time.Duration {
	return time.Duration(100+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		switch rf.state {
		case Follower:
			if time.Since(rf.Time) > randomTimeout() {
				go rf.StartElection()
			}
		case Candidate:
			if time.Since(rf.Time) > randomTimeout() {
				go rf.StartElection()
			}
		case Leader:
			rf.Broadcast()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 30 + (rand.Int63() % 30)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

type ByKey []int

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i] < a[j] }

func (rf *Raft) UpdateCommit() {
	if rf.state != Leader {
		return
	}
	commit := make(ByKey, len(rf.peers))
	copy(commit, rf.matchIndex)
	sort.Sort(ByKey(commit))

	mid := len(rf.peers) / 2
	if commit[mid] >= rf.Lastindex && rf.logs[commit[mid]-rf.Lastindex].Term == rf.currentTerm && commit[mid] > rf.commitIndex {
		rf.commitIndex = commit[mid]
	}

}

func (rf *Raft) apply() {
	for !rf.killed() {

		rf.mu.Lock()
		oldApply := rf.lastApplied
		oldCommit := rf.commitIndex

		//after crash
		if oldApply < rf.Lastindex {
			rf.lastApplied = rf.Lastindex
			rf.commitIndex = rf.Lastindex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}
		if oldCommit < rf.Lastindex {

			rf.commitIndex = rf.Lastindex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}

		if oldApply == oldCommit || (oldCommit-oldApply) >= len(rf.logs) {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 5)
			continue
		}

		entry := make([]Log, oldCommit-oldApply)
		copy(entry, rf.logs[oldApply+1-rf.Lastindex:oldCommit+1-rf.Lastindex])
		rf.mu.Unlock()
		// apply
		for key, value := range entry {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: key + oldApply + 1,
				Command:      value.Command,
			}

		}

		rf.mu.Lock()
		if rf.lastApplied < oldCommit {
			rf.lastApplied = oldCommit
		}
		if rf.lastApplied > rf.commitIndex {
			rf.commitIndex = rf.lastApplied
		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * 30)
	}

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
	rf.Time = time.Now()
	rf.state = Follower
	rf.votedFor = -1
	rf.logs = make([]Log, 0)
	rf.logs = append(rf.logs, Log{Term: 0})

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.apply()

	return rf
}
