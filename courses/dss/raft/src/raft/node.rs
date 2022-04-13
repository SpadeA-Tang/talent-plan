use crate::proto::raftpb::*;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use super::errors::*;
use super::raft::*;

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,

    sender: UnboundedSender<ApplyMsg>,

    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (tx, _rx): (UnboundedSender<ApplyMsg>, UnboundedReceiver<ApplyMsg>) = unbounded();

        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            sender: tx,
            workers: Arc::new(Mutex::new(Vec::new())),
        };
        let mut workers = node.workers.lock().unwrap();

        let mut rf_locked = node.raft.lock().unwrap();
        let rf2 = node.raft.clone();
        let rf3 = node.raft.clone();
        let id = rf_locked.me;
        let worker = thread::Builder::new()
            .name(format!("BGWorker-{}", id))
            .spawn(move || {
                background_worker(rf2);
            })
            .unwrap();
        workers.push(worker);

        let rx2 = rf_locked.append_entries_router.rx.take().unwrap();

        let _worker = thread::Builder::new()
            .name(format!("HandleaAEResp-{}", id))
            .spawn(move || {
                handle_resp_worker(rf3, rx2);
            })
            .unwrap();
        // workers.push(worker);

        let rf4 = node.raft.clone();
        let rx3 = rf_locked.apply_flag_router.rx.take().unwrap();
        let worker = thread::Builder::new()
            .name(format!("Applyworker-{}", id))
            .spawn(move || {
                apply_worker(rf4, rx3);
            })
            .unwrap();
        workers.push(worker);

        drop(workers);
        drop(rf_locked);
        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        let mut rf_locked = self.raft.lock().unwrap();
        if !rf_locked.is_leader() {
            return Err(Error::NotLeader);
        }

        let (index, term) = rf_locked.start(command).unwrap();

        Ok((index, term))
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().get_term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        let mut rf_locked = self.raft.lock().unwrap();
        rf_locked.shutdown();
        drop(rf_locked);

        for worker in self.workers.lock().unwrap().drain(..) {
            worker.join().expect("Cannot join the thread");
        }
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        Ok(self.raft.lock().unwrap().handle_vote_req(args))
    }

    async fn heartbeat(&self, args: HeartbeatArgs) -> labrpc::Result<HeartbeatReply> {
        Ok(self.raft.lock().unwrap().handle_heartbeat(args))
    }

    async fn append_entries(&self, args: AppendEntryArgs) -> labrpc::Result<AppendEntryReply> {
        Ok(self.raft.lock().unwrap().handle_append_entry(args))
    }
}
