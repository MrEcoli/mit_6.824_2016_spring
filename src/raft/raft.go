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
import (
    "bytes"
    "encoding/gob"
    "labrpc"
    "math/rand"
    "time"
    _"fmt"
    "fmt"
)

// import "bytes"
// import "encoding/gob"

var StateLeader = 2
var StateCandidate = 1
var StateFollower = 0

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu          sync.Mutex
    peers       []*labrpc.ClientEnd
    persister   *Persister
    me          int // index into peers[]
    AppliedMsgs chan ApplyMsg
    requests    chan int

                    // Your data here.
                    // Look at the paper's Figure 2 for a description of what
                    // state a Raft server must maintain.

                    //consist variable in all servers
    CurrentTerm int
    VotedFor    interface{}
    State       int

                    //volatile variable in all servers
    CommitIndex int
    LastApplied int
    Logs        []Entry

                    //volatile variable in leader
    NextIndex   []int
    MatchIndex  []int
}

type Entry struct {
    Command interface{}
    Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var isleader bool
    if rf.State == StateLeader {
        isleader = true
    } else {
        isleader = false
    }

    return rf.CurrentTerm, isleader
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

    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.CurrentTerm)
    e.Encode(rf.VotedFor)
    e.Encode(rf.Logs)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
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

    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.VotedFor)
    d.Decode(&rf.CurrentTerm)
    d.Decode(&rf.Logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
    // Your data here.
    CandidateTerm int
    Candidate     int
    LastLogIndex  int
    LastLogTerm   int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
                   // Your data here
    VoteGrant bool //vote result
    Term      int  //Term of target Server
}


//
// example RequestVote RPC handler.
//

//You should start by implementing Raft leader election. Fill in the RequestVoteArgs
// and RequestVoteReply structs, and modify Make() to create a background goroutine
// that starts an election (by sending out RequestVote RPCs) when it hasn't heard from
// another peer for a while. For election to work, you will also need to implement the
// RequestVote() RPC handler so that servers will vote for one another.

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here.

    // Candidate's log should up-to-date
    reply.Term = rf.CurrentTerm
    fmt.Println("server:", rf.me, "recevie vote request from server", args.Candidate, "Candidate Term", args.CandidateTerm)
    if rf.CurrentTerm > args.CandidateTerm {
        reply.VoteGrant = false
    } else {
        if rf.CurrentTerm < args.CandidateTerm {
            rf.CurrentTerm = args.CandidateTerm
            rf.VotedFor = nil
            if rf.isLogUpToDate(args) {
                fmt.Println("server:", rf.me, "grant vote request from server", args.Candidate)
                rf.VotedFor = args.Candidate
                reply.VoteGrant = true
                reply.Term = rf.CurrentTerm
            } else {
                reply.VoteGrant = false
                reply.Term = rf.CurrentTerm
            }
        } else {
            if rf.VotedFor == nil || rf.VotedFor == args.Candidate {
                fmt.Println("server:", rf.me, "grant vote request from server", args.Candidate)
                if rf.isLogUpToDate(args) {
                    reply.VoteGrant = true
                    reply.Term = rf.CurrentTerm
                    rf.VotedFor = args.Candidate
                }
            } else {
                fmt.Println("server:", rf.me, "failed to grant vote request from server", args.Candidate)
                reply.VoteGrant = false
                reply.Term = rf.CurrentTerm
            }
        }
    }
    fmt.Println("RequestVoteRPC Handler Done, server:", rf.me, "; Raft server status:", rf.State)
    fmt.Println(time.Now())
    rf.requests <- 1
}

func (rf *Raft) isLogUpToDate(args RequestVoteArgs) bool {
    n := len(rf.Logs)
    if n == 0 {
        return true
    }
    lastEntry := rf.Logs[n - 1]

    //todo: logIndex == len(logs) ??
    if args.LastLogTerm > lastEntry.Term || (args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= n - 1) {
        return true
    } else {
        return false
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply, ch chan int) {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    fmt.Println(reply, ok)
    if ok {
        ch <- server
    } else {
        ch <- -1
    }
}

type AppendEntriesArgs struct {
    LeaderTerm          int     // term of leader
    LeaderId            int     // identifier of leader
    LastLogIndex        int     // data appendable postion in the log
    LastLogTerm         int     // term of the check entry
    data                []Entry // entries of leader sends to follower
    LeaderCommitedIndex int     // commited Index in leader server
}

type AppendEntriesReply struct {
    Term    int  // term in the follower
    Success bool // whether append succuess
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    if (rf.CurrentTerm > args.LeaderTerm) {
        reply.Success = false
        reply.Term = rf.CurrentTerm
    } else {
        if rf.CurrentTerm < args.LeaderTerm {
            rf.CurrentTerm = args.LeaderTerm
        }
        reply.Term = rf.CurrentTerm

        if args.LastLogIndex >= len(rf.Logs) {
            reply.Success = false
        } else {
            if rf.Logs[args.LastLogIndex].Term == args.LastLogTerm {
                if len(args.data) > 0 {
                    for i := 0; i < len(args.data); i += 1 {
                        if i + args.LastLogIndex >= len(rf.Logs) {
                            rf.Logs = append(rf.Logs, args.data[i])
                        } else {
                            rf.Logs[i + args.LastLogIndex] = args.data[i]
                        }
                    }
                    curIndex := len(args.data) + args.LastLogIndex - 1

                    if curIndex < args.LeaderCommitedIndex {
                        rf.CommitIndex = curIndex
                    } else {
                        rf.CommitIndex = args.LeaderCommitedIndex
                    }

                }
                reply.Success = true
            } else {

                for i := args.LastLogIndex; i < len(rf.Logs); i += 1 {
                    rf.Logs[i] = Entry{}
                }
                reply.Success = false
            }
        }
    }
    //fmt.Println("server:", rf.me, "recevie AppendEntriesRPC from server:", args.LeaderId, "; Term of leader: ", args.LeaderTerm)
    rf.requests <- 2
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
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
    term, isLeader = rf.GetState()

    if isLeader {
        index = len(rf.Logs)
        rf.Logs = append(rf.Logs, Entry{Command: command, Term: rf.CurrentTerm})
    }

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
}

func heartbeatsTimeout() time.Duration {
    return time.Duration(rand.Int31n(322) + 200)
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

func (rf *Raft) makeRequestArgs() *RequestVoteArgs {
    req_args := &RequestVoteArgs{}
    req_args.CandidateTerm = rf.CurrentTerm
    req_args.Candidate = rf.me
    req_args.LastLogIndex = len(rf.Logs) - 1
    req_args.LastLogTerm = rf.Logs[len(rf.Logs) - 1].Term
    return req_args
}

func (rf *Raft) voteResult(rpc_results chan int, replies []*RequestVoteReply, ch chan bool) {
    //fmt.Println("voteResult function start by server: ", rf.me)
    if rf.State != StateCandidate{
        ch <- false
    }
    count := 1
    n := len(rf.peers)
    for {
        if rf.State != StateCandidate {
            return
        } else {
            server := <-rpc_results
            //fmt.Println("Request vote request of Candidate:", rf.me, "; Result: ", server)
            if server < 0 {
                continue
            } else {
                if replies[server].VoteGrant {
                    count += 1
                    //fmt.Println("server:", rf.me, "recevie voteGrant from server:", server)
                    //fmt.Println("server:", rf.me, "vote count", count)
                    if count * 2 > n {
                        rf.State = StateLeader
                        rf.MatchIndex = make([]int, len(rf.peers))
                        rf.NextIndex = make([]int, len(rf.peers))
                        for i := range rf.peers {
                            rf.MatchIndex[i] = -1
                            rf.NextIndex[i] = len(rf.Logs)
                        }
                        //fmt.Println("Leader:", rf.me)
                        ch <- true
                    }
                } else {
                    if replies[server].Term > rf.CurrentTerm {
                        rf.State = StateFollower
                        rf.CurrentTerm = replies[server].Term
                        rf.VotedFor = server
                    }
                }
            }
        }
    }
}

func (rf *Raft) election(ch chan bool) {
    if rf.State != StateCandidate {
        ch <- false
    }
    n := len(rf.peers)
    req_args := RequestVoteArgs{}
    req_args.CandidateTerm = rf.CurrentTerm
    req_args.Candidate = rf.me
    if len(rf.Logs) > 0 {
        req_args.LastLogIndex = len(rf.Logs)
        req_args.LastLogTerm = rf.Logs[len(rf.Logs) - 1].Term
    } else {
        req_args.LastLogIndex = -1
        req_args.LastLogTerm = 0
    }

    replies := make([]*RequestVoteReply, n)
    for i := range replies {
        replies[i] = &RequestVoteReply{}
    }
    rpc_results := make(chan int)

    for idx := range rf.peers {
        if idx != rf.me {
            go rf.sendRequestVote(idx, req_args, replies[idx], rpc_results)
            //fmt.Println("server:",rf.me, "sendRequestVoteRPC to server", idx)
        }
    }
    go rf.voteResult(rpc_results, replies, ch)
}

func (rf *Raft) leaderSyncFollower(server int) {

    //fmt.Println("leader work loop start, leader:", rf.me)
    args := AppendEntriesArgs{}
    args.LeaderId = rf.me
    args.LeaderTerm = rf.CurrentTerm

    for rf.State == StateLeader {
        time.Sleep(time.Millisecond * 50)
        reply := AppendEntriesReply{}
        if rf.MatchIndex[server] + 1 == rf.NextIndex[server] && rf.NextIndex[server] == len(rf.Logs) {
            args.data = make([]Entry, 0)
            args.LastLogIndex = rf.NextIndex[server]
            args.LastLogTerm = rf.CurrentTerm
        } else {
            if rf.MatchIndex[server] + 1 != rf.NextIndex[server] {
                args.LastLogIndex = rf.NextIndex[server] - 1
                args.LastLogIndex = rf.Logs[rf.NextIndex[server] - 1].Term
                args.data = rf.Logs[rf.NextIndex[server] - 1:]
            } else {
                args.LastLogIndex = rf.NextIndex[server]
                args.LastLogTerm = rf.Logs[rf.NextIndex[server]].Term
                args.data = rf.Logs[rf.NextIndex[server]:]
            }
        }
        ok := rf.sendAppendEntries(server, args, &reply)
        if ok {
            if !reply.Success {
                if reply.Term > rf.CurrentTerm {
                    rf.State = StateFollower
                } else {
                    if len(args.data) > 0 {
                        rf.NextIndex[server] -= 1
                    }
                }
            } else {
                if len(args.data) > 0 {
                    rf.MatchIndex[server] = len(args.data) + args.LastLogIndex - 1
                    rf.NextIndex[server] = len(args.data) + args.LastLogIndex

                    rf.mu.Lock()
                    if rf.CommitIndex < rf.MatchIndex[server] && rf.Logs[rf.MatchIndex[server]].Term == rf.CurrentTerm {
                        count := 0
                        for idx := range rf.peers {
                            if idx == rf.me || idx == server {
                                count += 1
                            } else {
                                if rf.MatchIndex[idx] >= rf.MatchIndex[server] {
                                    count += 1
                                }
                            }
                        }
                        if count * 2 > len(rf.peers) {
                            rf.CommitIndex = rf.MatchIndex[server]
                        }
                    }
                    rf.mu.Unlock()
                }
            }

        }
    }

    return
}

func (rf *Raft) leaderWork() {
    for idx := range rf.peers {
        if idx != rf.me {
            go rf.leaderSyncFollower(idx)
        }
    }

    for rf.State == StateLeader{
        time.Sleep(100 * time.Millisecond)
        fmt.Println("server:", rf.me, "Leader")
    }
}

func (rf *Raft) loop() {
    //fmt.Println("server", rf.me, "starting loop")
    //time.Sleep(time.Second)
    for {
        if rf.State == StateFollower {
            select {
            case <-time.After(time.Millisecond * (heartbeatsTimeout())):
                {
                    fmt.Println("server:", rf.me, " to Candidate, ", "at time: ", time.Now())
                    rf.CurrentTerm += 1
                    rf.State = StateCandidate
                    rf.VotedFor = rf.me
                    //for x := range rf.requests {
                    //    fmt.Println("value in r.requests", x)
                    //}
                    fmt.Println("rf.requests", rf.requests)
                }
            case <-rf.requests:
                {
                    //var s string
                    //if v == 1{
                    //    s = "RequestVoteRPC"
                    //} else {
                    //    s = "ApppendEntriesRPC"
                    //}
                    //fmt.Println("server:", rf.me, "recevie RPC requests: ", s)
                    fmt.Println("server:", rf.me, ":Follower; time: ", time.Now())
                    continue

                }
            }
        } else if rf.State == StateCandidate {
            //fmt.Println("in canditate:", rf.me)
            ch := make(chan bool)
            go rf.election(ch)
            select {
            case <-time.After(3 * time.Millisecond * 1000):
                //fmt.Println("election time out")
                continue
            case <-ch:
                //fmt.Println("election ends", rf.me, ":", v)
                continue
            }

        } else if rf.State == StateLeader {
            //fmt.Println("leader loop start: ", rf.me)
            rf.leaderWork()
        }
    }
}

func Make(peers []*labrpc.ClientEnd, me int,
persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me
    // Your initialization code here.
    rf.CurrentTerm = 0
    rf.VotedFor = nil
    rf.State = StateFollower
    rf.CommitIndex = 0
    rf.Logs = make([]Entry, 0)
    rf.MatchIndex = make([]int, len(rf.peers))
    rf.NextIndex = make([]int, len(rf.peers))
    rf.requests = make(chan int, 3)
    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
    //fmt.Println("server", me)

    go rf.loop()
    return rf
}
