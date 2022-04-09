pub use crate::proto::raftpb::*;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::lock::MutexGuard;
use futures::select;
use futures::SinkExt;
use futures::StreamExt;
use prost::Message;
use rand::{self, Rng};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::errors::*;
use super::persister::*;
use super::progress::*;

const ELECTION_TIMEOUT: u64 = 300;
const SLEEP_DURATION: u64 = 30;

const PRINT_ELECTION: bool = true;

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
}

enum MsgType {
    RequestVote,
    AppendLog,
}

struct Msg {
    msg_type: MsgType,

    term: u64,
    index: u64,
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

pub fn gen_randomized_timeout() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(0, ELECTION_TIMEOUT)
}

pub fn background_worker(rf: Arc<Mutex<Raft>>) {
    // sleep some ms below SLEEP_DURATION
    let mut rng = rand::thread_rng();
    loop {
        let sleep_time = rng.gen_range(0, SLEEP_DURATION);
        thread::sleep(Duration::from_millis(sleep_time));

        let mut rf_locked = rf.lock().unwrap();
        rf_locked.election_elapsed += sleep_time;
        if rf_locked.election_elapsed
            < rf_locked.randomized_election_timeout + rf_locked.election_timeout
        {
            // Not timeout
            if rf_locked.state == RaftState::Leader {
                rf_locked.bcast_heatbeat();
            }
            continue;
        }
        rf_locked.randomized_election_timeout = gen_randomized_timeout();

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

        let vote_request = RequestVoteArgs {
            id: rf_locked.me as u64,
            term: rf_locked.term as u64,
            log_index: 0, // todo: works
            log_term: rf_locked.term,
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
                        rf_locked.become_follower(term);
                        break;
                    }
                    vote_resp = rx_vote.next() => {
                        if let Some(vote_resp) = vote_resp
                        {
                            let mut rf_locked = rf.lock().unwrap();
                            let vote_resp = rf_locked.poll(vote_resp);
                            if vote_resp == VoteResult::VoteWin {
                                rf_locked.become_leader();
                                break;
                            } else if vote_resp == VoteResult::VoteLost {
                                let term = rf_locked.term;
                                rf_locked.become_follower(term);
                                break;
                            }
                        }
                    }
                };
            }
        })
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
    me: usize,
    peer_ids: Vec<usize>,

    state: RaftState,
    term: u64,
    vote: Option<usize>,
    index: u64, // todo: to be removed

    leader: Option<usize>,

    election_elapsed: u64,
    election_timeout: u64,
    randomized_election_timeout: u64,

    prs: ProgressTracker,
    // msgs: Vec<Msg>,
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
        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            peer_ids,
            state: RaftState::Follower,
            term: 0,
            index: 0,
            vote: None,
            leader: None,
            election_elapsed: 0,
            randomized_election_timeout: gen_randomized_timeout(),
            election_timeout: ELECTION_TIMEOUT,

            prs: ProgressTracker::new(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // crate::your_code_here((rf, apply_ch))
        rf
    }

    // todo : handle commit index
    pub fn handle_heartbeat(&mut self, id: usize, term: u64) {
        if term >= self.term {
            self.leader = Some(id);
            self.election_elapsed = 0;
        }
    }

    pub fn bcast_heatbeat(&mut self) {
        self.election_elapsed = 0;
        let arg = HeartbeatArgs {
            id: self.me as u64,
            term: self.term,
        };
        for &id in &self.peer_ids {
            if id == self.me {
                continue;
            }
            let peer = &self.peers[id];
            let peer_clone = peer.clone();
            let arg_clone = arg.clone();
            peer.spawn(async move {
                // todo: now, we don't need the result of hb
                let _ = peer_clone.heartbeat(&arg_clone).await.map_err(Error::Rpc);
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
    }

    fn become_leader(&mut self) {
        self.election_elapsed = 0;
        self.state = RaftState::Leader;

        if PRINT_ELECTION {
            println!("[{}] becomes leader at term {}", self.me, self.term);
        }
    }

    fn become_follower(&mut self, term: u64) {
        self.state = RaftState::Follower;
        self.term = term;
        self.election_elapsed = 0;
        self.prs.reset_vote_record();

        if PRINT_ELECTION {
            println!("[{}] becomes follower at term {}", self.me, term);
        }
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    fn poll(&mut self, resp: RequestVoteReply) -> VoteResult {
        if resp.term == self.term {
            if PRINT_ELECTION {
                println!(
                    "[{}] received vote from {} at term {}",
                    self.me, resp.id, self.term
                );
            }
            self.prs.record_vote(resp.id as usize, resp.grant);
        }
        self.prs.tally_vote()
    }

    pub fn handle_vote_req(&mut self, req: RequestVoteArgs) -> RequestVoteReply {
        if (req.term > self.term && self.log_up_to_date(req.log_term, req.log_index))
            || (self.term == req.term && self.vote == Some(req.id as usize))
        {
            if PRINT_ELECTION {
                println!(
                    "[{}] grant vote for {} at term {}",
                    self.me, req.id, self.term
                );
            }
            self.become_follower(req.term);

            RequestVoteReply {
                grant: true,
                id: self.me as u64,
                term: req.term,
            }
        } else {
            if PRINT_ELECTION {
                println!(
                    "[{}] reject vote for {} at term {}",
                    self.me, req.id, self.term
                );
            }
            RequestVoteReply {
                grant: false,
                id: self.me as u64,
                term: self.term,
            }
        }
    }

    fn log_up_to_date(&self, term: u64, index: u64) -> bool {
        if term > self.term || (term == self.term && index >= self.index) {
            true
        } else {
            false
        }
    }

    // fn request_vote(&self, tx: UnboundedSender<bool>, id: usize, vote_request: &RequestVoteArgs) {
    //     if let Ok(res) =
    //         futures::executor::block_on(async { self.peers[id].request_vote(&vote_request).await })
    //     {
    //         self.prs.record_vote(id, res.grant);
    //     } else {
    //         tx.send(false);
    //     }
    // }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
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

    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }

    pub fn is_leader(&self) -> bool {
        self.state == RaftState::Leader
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        let _ = self.snapshot(0, &[]);
        self.persist();
        let _ = &self.persister;
        let _ = &self.peers;
    }
}
