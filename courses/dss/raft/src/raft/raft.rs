pub use crate::proto::raftpb::*;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::select;
use futures::SinkExt;
use futures::StreamExt;
use rand::{self, Rng};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::persister::*;
use super::progress::*;
use super::{errors::*, raftlog};

const ELECTION_TIMEOUT: u64 = 150;
const HEARTBEAT_TIMEOUT: u64 = 20;
const SLEEP_DURATION: u64 = 5;

const PRINT_ELECTION: bool = false;
const PRINT_APPEND: bool = false;
const PRINT_APPLY: bool = false;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
    Shutdown,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

// Own addition
#[derive(Copy, PartialEq, Clone, Debug)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

pub struct Router<T> {
    pub tx: UnboundedSender<T>,
    pub rx: Option<UnboundedReceiver<T>>,
}

impl<T> Router<T> {
    pub fn new() -> Router<T> {
        let (tx, rx): (UnboundedSender<T>, UnboundedReceiver<T>) = unbounded();
        Router { tx, rx: Some(rx) }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    pub me: usize,
    peer_ids: Vec<usize>,

    state: RaftState,
    term: u64,
    vote: Option<usize>,

    leader: Option<usize>,

    election_elapsed: u64,
    heartbeat_elapsed: u64,
    heatbeat_timeout: u64,
    heartbeat_round: u64,
    election_timeout: u64,
    randomized_election_timeout: u64,

    prs: ProgressTracker,

    raft_log: raftlog::RaftLog,
    pub append_entries_router: Router<Reply>,
    // msgs: Vec<Msg>,
    apply_ch: UnboundedSender<ApplyMsg>,
    pub apply_flag_router: ApplyFlagRouter,

    applying: bool,
    shutdown: bool,
}

pub struct ApplyFlagRouter {
    tx: std::sync::mpsc::Sender<ApplyFlag>,
    pub rx: Option<std::sync::mpsc::Receiver<ApplyFlag>>,
}

impl ApplyFlagRouter {
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        ApplyFlagRouter { tx, rx: Some(rx) }
    }

    pub fn send(
        &self,
        flag: ApplyFlag,
    ) -> std::result::Result<(), std::sync::mpsc::SendError<ApplyFlag>> {
        self.tx.send(flag)
    }
}

pub enum ApplyFlag {
    Apply,
    Snapshot(ApplyMsg),
    Shutdown,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        let mut peer_ids = Vec::new();
        for i in 0..peers.len() {
            peer_ids.push(i);
        }
        let prs = ProgressTracker::new(&peer_ids);
        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            peer_ids,
            state: RaftState::Follower,
            term: 0,
            vote: None,
            leader: None,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            heartbeat_round: 0,
            randomized_election_timeout: gen_randomized_timeout(),
            election_timeout: ELECTION_TIMEOUT,
            heatbeat_timeout: HEARTBEAT_TIMEOUT,

            prs,

            raft_log: raftlog::RaftLog::new(0, 0),
            // vote_router: Router::new(),
            append_entries_router: Router::new(),

            apply_ch,
            apply_flag_router: ApplyFlagRouter::new(),

            applying: false,
            shutdown: false,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    // todo: Shutdown should be done more elegantly
    pub fn shutdown(&mut self) {
        self.shutdown = true;

        futures::executor::block_on(async {
            self.append_entries_router
                .tx
                .send(Reply::Shutdown)
                .await
                .unwrap();
        });

        self.apply_flag_router.send(ApplyFlag::Shutdown).unwrap();
    }

    pub fn get_commit_index(&self) -> u64 {
        self.raft_log.commit_idx
    }

    // First, I will implement naive rejection which means when leader discovers that the folower does not have a
    // matched log entry, it just decrease the pr.next_index by 1.
    pub fn handle_append_entry(&mut self, arg: AppendEntryArgs) -> AppendEntryReply {
        if PRINT_APPEND {
            if arg.entries.len() != 0 {
                println!(
                    "[{}] receive entries({} - {}) from [{}] with term {} and commit index {}",
                    self.me,
                    arg.entries[0].index,
                    arg.entries[arg.entries.len() - 1].index,
                    arg.id,
                    arg.term,
                    arg.commit_index
                );
            }
        }
        assert!(arg.entries.len() != 0);

        let mut reply = AppendEntryReply {
            id: self.me as u64,
            term: arg.term,
            index: 0,
            success: false,
            log_term: 0,
        };

        // 1. reply false if arg.term < self.term
        if arg.term < self.term {
            reply.term = self.term;
            return reply;
        }

        if self.state != RaftState::Follower {
            self.become_follower(arg.term, Some(arg.id as usize));
        }

        if arg.entries[arg.entries.len() - 1].index < self.raft_log.commit_idx {
            reply.success = true;
            reply.index = self.raft_log.commit_idx;
            return reply;
        }

        // 2. Check whether there is a entry matching the leader's prev log entry
        if !self
            .raft_log
            .match_term(arg.lastlog_index, arg.lastlog_term)
        {
            if PRINT_APPEND {
                println!("[{}] (last_index {}, last_term {}) has not match index with [{}] (prev_index {}, prev_term{})", 
                    self.me, self.raft_log.last_index(),  self.raft_log.term(self.raft_log.last_index()), arg.id, arg.lastlog_index, arg.lastlog_term);
            }

            // 返回可能产生分歧的最新的log
            // 即找到最新的log，其index <= arg.index 并且 term <= arg.term
            let mut hint_index = std::cmp::min(self.raft_log.last_index(), arg.lastlog_index);
            hint_index = self
                .raft_log
                .find_conflict_by_term(hint_index, arg.lastlog_term);
            let hint_term = self.raft_log.term(hint_index);

            reply.index = hint_index;
            reply.log_term = hint_term;
            return reply;
        }

        reply.success = true;
        reply.index = arg.lastlog_index + arg.entries.len() as u64;

        // 3. Find conflict entries
        let conflict_index = self.raft_log.find_conflict_entry(&arg.entries);
        if PRINT_APPEND {
            println!(
                "[{}] conflict_index {} self.commit_index {}",
                self.me, conflict_index, self.raft_log.commit_idx
            );
        }
        assert!(conflict_index == 0 || conflict_index >= self.raft_log.commit_idx);

        // conflict_index == 0 means that this follower already has all ents
        if conflict_index != 0 {
            if conflict_index <= self.raft_log.last_index() {
                let real_index = self.raft_log.real_index(conflict_index);
                self.raft_log.trunct(real_index as usize);
            }

            // todo: 保证这个是对的
            let begin_index = (conflict_index - (arg.lastlog_index + 1)) as usize;

            if PRINT_APPEND {
                println!(
                    "[{}] success append entry from {} to {}",
                    self.me,
                    arg.entries[begin_index].index,
                    arg.entries[arg.entries.len() - 1].index
                );
            }

            self.raft_log.append_entries(&arg.entries[begin_index..]);
            self.persist();
        }
        if self.raft_log.commit(arg.commit_index) {
            if PRINT_APPEND {
                println!(
                    "[{}] update commit index to {}",
                    self.me, self.raft_log.commit_idx,
                );
            }
            self.send_apply_flag();
        }

        reply
    }

    pub fn handle_append_resp(&mut self, resp: AppendEntryReply) {
        if PRINT_APPEND {
            println!("[{}] received append resp {:?}", self.me, resp);
        }
        if self.state != RaftState::Leader {
            return;
        }

        if resp.term > self.term {
            self.become_follower(resp.term, None);
            return;
        }

        if resp.success {
            if self.prs.may_update(resp.id as usize, resp.index) {
                self.may_commit_and_apply();
            }
        } else {
            let mut pr = self.prs.progress_map.get_mut(&(resp.id as usize)).unwrap();

            let mut next_index = resp.index;
            if resp.log_term > 0 {
                next_index = self
                    .raft_log
                    .find_conflict_by_term(next_index, resp.log_term);
            }
            pr.next_index = std::cmp::min(pr.next_index - 1, next_index);
            pr.next_index = std::cmp::max(pr.next_index, pr.matched_index + 1);

            let tx = self.append_entries_router.tx.clone();
            self.send_append(resp.id as usize, tx);
        }
    }

    pub fn handle_hb_resp(&mut self, resp: HeartbeatReply) {
        if self.state != RaftState::Leader {
            return;
        }

        if resp.term > self.term {
            self.become_follower(resp.term, None);
            return;
        }

        if self.heartbeat_round > 5 {
            self.heartbeat_round = 0;
            let pr = self.prs.progress_map.get(&(resp.id as usize)).unwrap();
            if pr.matched_index < self.raft_log.last_index() {
                self.send_append(resp.id as usize, self.append_entries_router.tx.clone());
            }
        }
    }

    fn may_commit_and_apply(&mut self) {
        let index = self.prs.get_commit_index();

        if self.raft_log.commit(index) {
            if PRINT_APPEND {
                println!(
                    "[{}] update commit index to {}",
                    self.me, self.raft_log.commit_idx,
                );
            }
            self.send_apply_flag();
        }
    }

    fn send_apply_flag(&mut self) {
        if let Err(e) = self.apply_flag_router.send(ApplyFlag::Apply) {
            println!("Warning: {}", e);
        }
    }

    // todo : handle commit index
    pub fn handle_heartbeat(&mut self, arg: HeartbeatArgs) -> HeartbeatReply {
        if arg.term >= self.term {
            self.become_follower(arg.term, Some(arg.id as usize));
            if self.raft_log.commit(arg.commit_index) {
                if PRINT_APPEND {
                    println!(
                        "[{}] update commit index to {}",
                        self.me, self.raft_log.commit_idx,
                    );
                }
                self.send_apply_flag();
            }
        }
        HeartbeatReply {
            id: self.me as u64,
            term: self.term,
        }
    }

    pub fn bcast_heatbeat(&mut self) {
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        let arg = HeartbeatArgs {
            id: self.me as u64,
            term: self.term,
            commit_index: self.raft_log.commit_idx,
        };
        self.heartbeat_round += 1;
        for &id in &self.peer_ids {
            if id == self.me {
                continue;
            }
            let peer = &self.peers[id];
            let peer_clone = peer.clone();
            let mut arg_clone = arg.clone();
            let mut pr = self.prs.progress_map.get_mut(&id).unwrap();
            arg_clone.commit_index = std::cmp::min(arg_clone.commit_index, pr.matched_index);
            let mut tx = self.append_entries_router.tx.clone();

            let follower_state = pr.state.clone();
            if follower_state == PrState::SnapshotState {
                pr.snap_timeout += 1;
            }
            peer.spawn(async move {
                if PRINT_APPEND {
                    println!(
                        "[{}] send heartbeat to [{}], term {}, commit index {} follower state {:?}",
                        arg_clone.id, id, arg_clone.term, arg_clone.commit_index, follower_state,
                    );
                }
                // todo: now, we don't need the result of hb
                let resp = peer_clone.heartbeat(&arg_clone).await.map_err(Error::Rpc);
                if let Ok(resp) = resp {
                    tx.send(Reply::HeartbeatReply(resp))
                        .await
                        .expect("Something wrong when receiving hb");
                }
            });
        }
    }

    fn become_candidate(&mut self, term: u64) {
        self.state = RaftState::Candidate;
        self.term = term + 1;
        self.leader = None;
        self.vote = Some(self.me);
        self.prs.record_vote(self.me, true);

        if PRINT_ELECTION {
            println!("[{}] becomes candidate at term {}", self.me, self.term);
        }

        self.persist();
    }

    fn become_leader(&mut self) {
        self.election_elapsed = 0;
        self.state = RaftState::Leader;
        self.vote = None;
        self.raft_log.prev_last_log_index = self.raft_log.last_index();

        let me = self.me;
        let last_index = self.raft_log.last_index();

        // // todo: I have now removed no-op to match some tests. How to handle commit previous term's entry.
        // self.raft_log.append(Entry {
        //     term: self.term,
        //     index: last_index + 1,
        //     data: Vec::new(),
        // });
        // last_index += 1;

        self.prs.reset_progress(|id: usize, pr: &mut Progress| {
            pr.matched_index = 0;
            pr.next_index = last_index + 1;
            if id == me {
                pr.matched_index = last_index;
            };
        });

        if PRINT_ELECTION {
            println!("[{}] becomes leader at term {}", self.me, self.term);
        }

        self.persist();
        // self.bcast_append();
    }

    fn become_follower(&mut self, term: u64, leader: Option<usize>) {
        self.state = RaftState::Follower;
        assert!(term >= self.term);
        if term > self.term {
            self.vote = None;
            self.term = term;
            self.persist();
        }
        self.leader = leader;
        self.election_elapsed = 0;
        self.prs.reset_vote_record();

        if PRINT_ELECTION {
            println!(
                "[{}] becomes follower at term {} commit index {}",
                self.me, term, self.raft_log.commit_idx
            );
        }
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    fn poll(&mut self, resp: RequestVoteReply) -> VoteResult {
        if resp.term == self.term {
            if PRINT_ELECTION {
                println!(
                    "[{}] received vote from {} at term {}, {:?}",
                    self.me, resp.id, self.term, resp
                );
            }
            self.prs.record_vote(resp.id as usize, resp.grant);

            // If it is rejected by someone, penalty the timeout, which means
            // more rejections, more penalty.
            if !resp.grant && self.log_behind(resp.log_term, resp.log_index) {
                self.randomized_election_timeout += gen_randomized_timeout();
                println!(
                    "[{}]'s vote is rejected by [{}] randomzied election timeout now: {}",
                    self.me, resp.id, self.randomized_election_timeout
                );
            }
        }
        self.prs.tally_vote()
    }

    pub fn handle_vote_req(&mut self, req: RequestVoteArgs) -> RequestVoteReply {
        let last_index = self.raft_log.last_index();
        // 首先log一定得up to date，然后是term更大 或者 相等，但是相等的时候要么之前就是投的他或者没投过
        if self.log_up_to_date(req.log_term, req.log_index)
            && (req.term > self.term
                || (self.term == req.term
                    && (self.vote == Some(req.id as usize) || self.vote == None)))
        {
            if PRINT_ELECTION {
                println!(
                    "[{}] grant vote for {} at term {} candidate[{}]'s log (index {}, term {}), follower[{}]'s log (index{}, term{})",
                    self.me, req.id, self.term, req.id, req.log_index, req.log_term, self.me, self.raft_log.last_index(), self.raft_log.term(self.raft_log.last_index())
                );
            }
            self.become_follower(req.term, None);
            self.vote = Some(req.id as usize);
            self.persist();

            RequestVoteReply {
                grant: true,
                id: self.me as u64,
                term: req.term,
                log_index: last_index,
                log_term: self.raft_log.term(last_index),
            }
        } else {
            if PRINT_ELECTION {
                println!(
                    "[{}] reject vote for {} at term {}",
                    self.me, req.id, self.term
                );
            }
            if req.term > self.term {
                self.become_follower(req.term, None);
            }
            RequestVoteReply {
                grant: false,
                id: self.me as u64,
                term: self.term,
                log_index: last_index,
                log_term: self.raft_log.term(last_index),
            }
        }
    }

    fn log_up_to_date(&self, log_term: u64, log_index: u64) -> bool {
        let last_index = self.raft_log.last_index();
        let last_term = self.raft_log.term(last_index);
        if log_term > last_term || (log_term == last_term && log_index >= last_index) {
            true
        } else {
            if PRINT_ELECTION {
                println!("Voter's log (term {}, index {}) is not update to date than votee's log (term {}, index {})", 
                    log_term, log_index, self.raft_log.term(last_index), last_index);
            }
            false
        }
    }

    // Is my log is older than `log_term` and `log_index`
    fn log_behind(&self, log_term: u64, log_index: u64) -> bool {
        let last_index = self.raft_log.last_index();
        let last_term = self.raft_log.term(last_index);
        if last_term < log_term || (last_term == log_term && last_index < log_index) {
            true
        } else {
            false
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);

        let vote;
        match self.vote {
            Some(v) => vote = v,
            None => vote = usize::MAX,
        }

        let raft_state = RaftDurableState {
            term: self.term,
            vote: vote as u64,
            raft_log: Some(self.raft_log.clone().into()),
        };

        let mut buf = vec![];
        labcodec::encode(&raft_state, &mut buf).expect("Persist error!!!");
        self.persister.save_raft_state(buf);
        if self.raft_log.last_index_in_snapshot != self.raft_log.entries[0].index {
            assert!(self.raft_log.last_index_in_snapshot == self.raft_log.entries[0].index);
        }
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }

        let raft_state: RaftDurableState =
            labcodec::decode(data).expect("Something wrong when decoding");
        if raft_state.vote as usize == usize::MAX {
            self.vote = None;
        } else {
            self.vote = Some(raft_state.vote as usize);
        }
        self.term = raft_state.term;
        self.raft_log = raft_state.raft_log.unwrap().into();
        if self.raft_log.last_index_in_snapshot != self.raft_log.entries[0].index {
            assert!(self.raft_log.last_index_in_snapshot == self.raft_log.entries[0].index);
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    pub fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
        mut sender: UnboundedSender<RequestVoteReply>,
    ) {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            if let Ok(res) = res {
                let _ = sender.send(res).await;
            }
        });
    }

    pub fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        let term = self.term;
        let next_log_index = self.raft_log.last_index() + 1;
        let entry = Entry {
            term,
            index: next_log_index,
            data: buf,
        };
        self.raft_log.append(entry);
        self.persist();

        let pr = self.prs.progress_map.get_mut(&self.me).unwrap();
        pr.matched_index = self.raft_log.last_index();

        if PRINT_APPEND {
            println!(
                "[{}] start command of index {} and term {}",
                self.me, next_log_index, term
            );
        }
        self.bcast_append();

        Ok((next_log_index, term))
    }

    fn bcast_append(&mut self) {
        let peers_id = self.peer_ids.clone();
        for &id in &peers_id {
            if id == self.me {
                continue;
            }
            self.send_append(id, self.append_entries_router.tx.clone());
        }
    }

    fn send_append(&mut self, to: usize, mut sender: UnboundedSender<Reply>) {
        let peer = &self.peers[to];
        let peer_clone = peer.clone();

        let mut pr = self.prs.progress_map.get_mut(&to).unwrap();
        if pr.state == PrState::SnapshotState {
            if pr.snap_timeout >= 5 {
                pr.snap_timeout = 0;
            } else {
                return;
            }
        }

        // Check the existance of the log entry
        if pr.next_index <= self.raft_log.last_index_in_snapshot {
            // case of sending snapshot
            let snapshot = self.persister.snapshot();
            let arg = SnapshotArgs {
                id: self.me as u64,
                last_included_index: self.raft_log.last_index_in_snapshot,
                last_included_term: self.raft_log.last_term_in_snapshot,
                snapshot,
                term: self.term,
            };
            println!("[{}] send snapshot to [{}]", self.me, to);
            pr.state = PrState::SnapshotState;
            peer.spawn(async move {
                let res = peer_clone.install_snapshot(&arg).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    let _ = sender.send(Reply::SnapshotReply(res)).await;
                }
            })
        } else {
            let mut entries = Vec::new();
            for log_index in pr.next_index..=self.raft_log.last_index() {
                entries.push(self.raft_log.get_entry(log_index).clone());
            }
            if entries.len() == 0 {
                return;
            }

            let arg = AppendEntryArgs {
                id: self.me as u64,
                term: self.term,
                lastlog_index: pr.next_index - 1,
                lastlog_term: self.raft_log.term(pr.next_index - 1),
                commit_index: self.raft_log.commit_idx,
                entries,
            };

            peer.spawn(async move {
                let res = peer_clone.append_entries(&arg).await.map_err(Error::Rpc);
                if let Ok(res) = res {
                    let _ = sender.send(Reply::AppendReply(res)).await;
                }
            });

            if PRINT_APPEND {
                println!(
                    "[{}] send entries from {} to {} to peer {}",
                    self.me,
                    pr.next_index,
                    self.raft_log.last_index(),
                    to
                );
            }
        }
    }

    pub fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        println!(
            "[{}] install snapshot prev index {} prev term {}",
            self.me, last_included_index, last_included_term
        );
        if self.applying {
            return false;
        }
        let commit_index = self.raft_log.commit_idx;
        let commit_term = self.raft_log.term(commit_index);
        if last_included_term < commit_term || last_included_index < commit_index {
            return false;
        }
        self.become_follower(self.term, self.leader);
        self.raft_log = raftlog::RaftLog::new(last_included_index, last_included_term);
        self.raft_log.apply_idx = last_included_index;
        self.raft_log.commit_idx = last_included_index;

        self.persist();

        self.persister
            .save_state_and_snapshot(self.persister.raft_state(), snapshot.to_vec());

        true
    }

    pub fn apply_snapshot(&mut self, arg: SnapshotArgs) -> (bool, u64) {
        if self.applying {
            return (false, 0);
        }
        if arg.term < self.term {
            return (false, 0);
        }
        if arg.last_included_index < self.raft_log.commit_idx {
            return (false, 0);
        }
        let index = arg.last_included_index;

        let snap = ApplyMsg::Snapshot {
            data: arg.snapshot,
            term: arg.last_included_term,
            index: arg.last_included_index,
        };

        if let Err(e) = self.apply_flag_router.send(ApplyFlag::Snapshot(snap)) {
            println!("Warning: {}", e);
        }

        (true, index)
    }

    pub fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        assert!(index >= self.raft_log.last_index_in_snapshot);
        println!("[{}] snapshot, index {}", self.me, index);
        let term = self.raft_log.term(index);
        let real_index = self.raft_log.real_index(index);
        let mut remain_entries = self.raft_log.entries.split_off(real_index as usize);
        std::mem::swap(&mut self.raft_log.entries, &mut remain_entries);
        self.raft_log.last_index_in_snapshot = index;
        self.raft_log.last_term_in_snapshot = term;
        assert!(self.raft_log.last_index_in_snapshot == self.raft_log.entries[0].index);
        self.persist();
        self.persister
            .save_state_and_snapshot(self.persister.raft_state(), snapshot.to_vec());
    }

    pub fn is_leader(&self) -> bool {
        self.state == RaftState::Leader
    }
}

pub fn gen_randomized_timeout() -> u64 {
    let mut rng = rand::thread_rng();
    // 增加随机时间的权重，可以减少拥有old log的node一直成为candidate扰乱选举的现象
    rng.gen_range(ELECTION_TIMEOUT, ELECTION_TIMEOUT * 3)
}

pub fn background_worker(rf: Arc<Mutex<Raft>>) {
    // sleep some ms below SLEEP_DURATION
    loop {
        thread::sleep(Duration::from_millis(SLEEP_DURATION));

        let mut rf_locked = rf.lock().unwrap();
        if rf_locked.shutdown {
            break;
        }
        rf_locked.election_elapsed += SLEEP_DURATION;
        rf_locked.heartbeat_elapsed += SLEEP_DURATION;
        if rf_locked.election_elapsed
            < rf_locked.randomized_election_timeout + rf_locked.election_timeout
        {
            // Not timeout
            if rf_locked.state == RaftState::Leader
                && rf_locked.heartbeat_elapsed > rf_locked.heatbeat_timeout
            {
                rf_locked.bcast_heatbeat();
            }
            continue;
        }
        rf_locked.randomized_election_timeout = gen_randomized_timeout();
        if PRINT_ELECTION {
            println!(
                "[{}] randomzied election timeout now: {}",
                rf_locked.me, rf_locked.randomized_election_timeout
            );
        }

        let (mut tx_timeout, mut rx_timeout): (UnboundedSender<bool>, UnboundedReceiver<bool>) =
            unbounded();

        let term = rf_locked.term;
        rf_locked.become_candidate(term);
        let compaign_duration = rf_locked.election_timeout + rf_locked.randomized_election_timeout;

        // 超时提醒
        let _ = thread::spawn(move || {
            thread::sleep(Duration::from_millis(compaign_duration));
            let _ = futures::executor::block_on(async { tx_timeout.send(true).await });
        });

        let last_index = rf_locked.raft_log.last_index();
        let vote_request = RequestVoteArgs {
            id: rf_locked.me as u64,
            term: rf_locked.term as u64,
            log_index: last_index, // todo: works
            log_term: rf_locked.raft_log.term(last_index),
        };

        let (tx_vote, mut rx_vote): (
            UnboundedSender<RequestVoteReply>,
            UnboundedReceiver<RequestVoteReply>,
        ) = unbounded();
        for &id in &rf_locked.peer_ids {
            if id == rf_locked.me {
                continue;
            }
            let req = vote_request.clone();
            rf_locked.send_request_vote(id, req, tx_vote.clone());
        }
        drop(rf_locked);

        futures::executor::block_on(async {
            loop {
                select! {
                    _ = rx_timeout.next() => {
                        let mut rf_locked = rf.lock().unwrap();
                        let term = rf_locked.term;
                        rf_locked.become_follower(term, None);
                        println!("[{}] compaign fails due to timeout", rf_locked.me);
                        break;
                    }
                    vote_resp = rx_vote.next() => {
                        if let Some(vote_resp) = vote_resp
                        {
                            let mut rf_locked = rf.lock().unwrap();

                            // 首先判断现在还是不是candidate
                            let vote_resp = rf_locked.poll(vote_resp);
                            if vote_resp == VoteResult::VoteWin {
                                rf_locked.become_leader();
                                break;
                            } else if vote_resp == VoteResult::VoteLost {
                                let term = rf_locked.term;
                                rf_locked.become_follower(term, None);
                                println!("[{}] compaign fails due to rejection, the randomized_election_timeout now is: {}", rf_locked.me, rf_locked.randomized_election_timeout);
                                break;
                            }
                            if PRINT_ELECTION {
                                println!("[{}] compaign result {:?}", rf_locked.me, vote_resp);
                            }
                        }
                    }
                };
            }
        })
    }
}

pub enum Reply {
    AppendReply(AppendEntryReply),
    HeartbeatReply(HeartbeatReply),
    SnapshotReply(SnapshotReply),
    Shutdown,
}

pub fn handle_resp_worker(rf: Arc<Mutex<Raft>>, mut rx: UnboundedReceiver<Reply>) {
    loop {
        futures::executor::block_on(async {
            while let Some(res) = rx.next().await {
                match res {
                    Reply::AppendReply(res) => {
                        let mut rf_locked = rf.lock().unwrap();
                        if rf_locked.shutdown {
                            return;
                        }

                        rf_locked.handle_append_resp(res);
                    }
                    Reply::HeartbeatReply(res) => {
                        let mut rf_locked = rf.lock().unwrap();
                        if rf_locked.shutdown {
                            return;
                        }

                        rf_locked.handle_hb_resp(res);
                    }
                    Reply::SnapshotReply(res) => {
                        if !res.reject {
                            let mut rf_locked = rf.lock().unwrap();
                            let mut pr = rf_locked
                                .prs
                                .progress_map
                                .get_mut(&(res.id as usize))
                                .unwrap();
                            pr.state = PrState::NormalState;
                            pr.matched_index = res.last_included_index;
                            pr.next_index = res.last_included_index + 1;
                        }
                    }
                    Reply::Shutdown => return,
                }
            }
        })
    }
}

pub fn apply_worker(rf: Arc<Mutex<Raft>>, rx: std::sync::mpsc::Receiver<ApplyFlag>) {
    loop {
        match rx.recv().unwrap() {
            ApplyFlag::Apply => {
                let mut rf_locked = rf.lock().unwrap();
                let mut apply_msgs = Vec::new();
                let mut sender = rf_locked.apply_ch.clone();

                for index in (rf_locked.raft_log.apply_idx + 1)..=rf_locked.raft_log.commit_idx {
                    let ent = rf_locked.raft_log.get_entry(index);
                    let apply_msg = ApplyMsg::Command {
                        data: ent.data.clone(),
                        index,
                    };
                    apply_msgs.push(apply_msg);
                }
                let commit_idx = rf_locked.raft_log.commit_idx;
                rf_locked.applying = true;
                if PRINT_APPLY {
                    println!("[{}] is applying", rf_locked.me);
                }
                drop(rf_locked);

                for msg in apply_msgs {
                    let _ = futures::executor::block_on(async { sender.send(msg).await });
                }

                let mut rf_locked = rf.lock().unwrap();
                rf_locked.raft_log.apply_idx = commit_idx;
                rf_locked.applying = false;
                if PRINT_APPLY {
                    println!("[{}] finishes applying", rf_locked.me);
                }
            }
            ApplyFlag::Snapshot(msg) => {
                let rf_locked = rf.lock().unwrap();
                let mut sender = rf_locked.apply_ch.clone();
                let _ = futures::executor::block_on(async { sender.send(msg).await });
            }
            ApplyFlag::Shutdown => {
                return;
            }
        }
    }
}
