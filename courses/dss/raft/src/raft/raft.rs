use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::lock::MutexGuard;
use futures::select;
use futures::SinkExt;
use futures::StreamExt;
use rand::{self, Rng};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use crate::proto::raftpb::*;

use super::progress::*;
use super::persister::*;
use super::errors::*;

const ElectionTimeout: u64 = 300;
const SleepDuration: u64 = 30;

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
    rng.gen_range(0, ElectionTimeout)
}

pub fn may_compaign(rf: Arc<Mutex<Raft>>) {
    // sleep some ms below SleepDuration
    let mut rng = rand::thread_rng();
    loop {
        let sleep_time = rng.gen_range(0, SleepDuration);
        thread::sleep(Duration::from_millis(sleep_time));

        let mut rf_locked = rf.lock().unwrap();
        rf_locked.election_elapsed += sleep_time;
        if rf_locked.election_elapsed
            < rf_locked.randomized_election_timeout + rf_locked.election_timeout
        {
            continue;
        }
        let (mut tx_timeout, mut rx_timeout): (UnboundedSender<bool>, UnboundedReceiver<bool>) =
            unbounded();
        
        let term = rf_locked.term;
        rf_locked.become_candidate(term);
        let compaign_duration = rf_locked.election_timeout + rf_locked.randomized_election_timeout;

        // 超时提醒
        let _ = thread::spawn(move || {
            thread::sleep(Duration::from_millis(compaign_duration));
            tx_timeout.send(true);
        });

        let vote_request = RequestVoteArgs {
            id: rf_locked.me as u64,
            term: rf_locked.term as u64,
            index: 0,
        };
        let mut vote_reponses = Vec::new();

        for &id in &rf_locked.peer_ids {
            if id == rf_locked.me {
                continue;
            }
            let client = rf_locked.peers[id].clone();
            let req = vote_request.clone();
            let resp = async move { client.request_vote(&req).await };
            vote_reponses.push(resp);
        }
        drop(rf_locked);

        let (tx_vote, mut rx_vote): (UnboundedSender<bool>, UnboundedReceiver<bool>) =
            unbounded();
        for f in vote_reponses {
            let rf_clone = rf.clone();
            let mut tx_vote_clone = tx_vote.clone();
            thread::spawn(move || {
                if let Ok(res) = futures::executor::block_on(f) {
                    let mut rf_locked = rf_clone.lock().unwrap();
                    rf_locked.prs.record_vote(res.id as usize, res.grant);
                } else {
                    tx_vote_clone.send(false);
                }
            });
        }

        futures::executor::block_on(async {
            loop {
                select! {
                    _ = rx_timeout.next() => {
                        break;
                    }
                    resp = rx_vote.next() => {
                        {
                            let rf_locked = rf.lock().unwrap();
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

    leader: Option<u64>,

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
            vote: None,
            leader: None,
            election_elapsed: 0,
            randomized_election_timeout: gen_randomized_timeout(),
            election_timeout: ElectionTimeout,

            prs: ProgressTracker::new(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        // crate::your_code_here((rf, apply_ch))
        rf
    }

    fn become_candidate(&mut self, term: u64) {
        self.state = RaftState::Candidate;
        self.term = term + 1;
        self.leader = None;
        self.vote = Some(self.me)
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    fn propose<M>(&self, command: &M)
    where
        M: labcodec::Message,
    {
    }

    fn poll(&mut self, id: usize, grant: bool) -> VoteResult {
        self.prs.record_vote(id, grant);
        self.prs.tally_vote()
    }

    fn win_compaign(&self) {}

    fn send(&self, msg: Msg) {}

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
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
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
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        crate::your_code_here((server, args, tx, rx))
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
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.persister;
        let _ = &self.peers;
    }
}