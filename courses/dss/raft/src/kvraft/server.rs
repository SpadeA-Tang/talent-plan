use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
type CallBack<T> = UnboundedSender<T>;

use crate::proto::kvraftpb::*;
use crate::raft;
use crate::raft::node;
use crate::raft::raft::ApplyMsg;

use futures::executor::block_on;

pub struct KvServer {
    pub rf: node::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    pub kv_table: HashMap<String, String>,

    call_backs: Vec<ApplyCtx<RequestRes>>,

    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,

    shutdown: bool,
}

pub struct ApplyCtx<T> {
    cb: CallBack<T>,
    index: u64,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let rf = raft::raft::Raft::new(servers, me, persister, tx);

        KvServer {
            rf: node::Node::new(rf),
            me,
            maxraftstate,
            kv_table: HashMap::new(),
            call_backs: Vec::new(),
            apply_ch: Some(apply_ch),
            shutdown: false,
        }
    }

    pub fn take_apply_ch(&mut self) -> UnboundedReceiver<ApplyMsg> {
        if let Some(apply_ch) = self.apply_ch.take() {
            return apply_ch;
        }

        unreachable!();
    }

    pub fn push_apply_ctx(&mut self, apply_ctx: ApplyCtx<RequestRes>) {
        self.call_backs.push(apply_ctx);
    }

    pub fn get_cb(&mut self, index: u64) -> CallBack<RequestRes> {
        for i in 0..self.call_backs.len() {
            let apply_ctx = &self.call_backs[i];
            if apply_ctx.index == index {
                let apply_ctx = self.call_backs.remove(i);
                return apply_ctx.cb;
            }
        }

        panic!("Cannot find cb");
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }

    fn apply_worker(server: Arc<Mutex<KvServer>>, mut apply_ch: UnboundedReceiver<ApplyMsg>) {
        loop {
            block_on(async {
                while let Some(apply_msg) = apply_ch.next().await {
                    let mut server_locked = server.lock().await;
                    if server_locked.shutdown {
                        return;
                    }
                    if !server_locked.rf.is_leader() {
                        continue;
                    }
                    match apply_msg {
                        ApplyMsg::Command { data, index } => {
                            let command: RaftCommand =
                                labcodec::decode(&data).expect("committed command is not an entry");

                            // Same problem: why op_type is the type of i32
                            // Get
                            if command.op_type == 0 {
                                let request: GetRequest = labcodec::decode(&command.data)
                                    .expect("committed command is not an entry");
                                let mut cb = server_locked.get_cb(index);
                                cb.send(RequestRes::Success(
                                    server_locked.kv_table.get(&request.key).unwrap().clone(),
                                ))
                                .await
                                .unwrap();
                            // PutAppend
                            } else if command.op_type == 1 {
                                let request: PutAppendRequest = labcodec::decode(&command.data)
                                    .expect("committed command is not an entry");
                                let mut cb = server_locked.get_cb(index);
                                let kv_table = &mut server_locked.kv_table;
                                // Append
                                if request.op == 1 {
                                    let old_value = kv_table
                                        .get_mut(&request.key)
                                        .expect("It should have a value");
                                    old_value.push_str(&request.value);
                                // Put
                                } else if request.op == 2 {
                                    kv_table.insert(request.key, request.value);
                                } else {
                                    unreachable!();
                                }
                                cb.send(RequestRes::Success(String::new())).await.unwrap();
                            } else {
                                unreachable!();
                            }
                        }
                        ApplyMsg::Snapshot { .. } => {
                            unreachable!()
                        }
                        ApplyMsg::Shutdown => {
                            unreachable!()
                        }
                    }
                }
            })
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    server: Arc<Mutex<KvServer>>,

    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        let id = kv.me;
        let node = Node {
            server: Arc::new(Mutex::new(kv)),
            workers: Arc::new(Mutex::new(Vec::new())),
        };

        let server = node.server.clone();
        let apply_ch = block_on(async { node.server.lock().await }).take_apply_ch();
        let worker = thread::Builder::new()
            .name(format!("ApplyWorker-{}", id))
            .spawn(move || {
                KvServer::apply_worker(server, apply_ch);
            })
            .unwrap();
        block_on(async { node.workers.lock().await }).push(worker);

        node
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::raft::State {
        // Your code here.
        block_on(async { self.server.lock().await }).rf.get_state()
    }

    pub async fn put_append(&self, arg: PutAppendRequest) -> PutAppendReply {
        let mut reply = PutAppendReply::default();
        reply.wrong_leader = true;

        let (cb, mut rx): (UnboundedSender<RequestRes>, UnboundedReceiver<RequestRes>) =
            unbounded();
        let mut timeout_cb = cb.clone();
        // 超时提醒
        let _ = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            let _ = block_on(async { timeout_cb.send(RequestRes::Timeout).await });
        });

        let mut server = self.server.lock().await;
        let mut data = vec![];
        labcodec::encode(&arg, &mut data).unwrap();
        let raft_command = RaftCommand { op_type: 1, data };

        if let Ok((index, _)) = server.rf.start(&raft_command) {
            server.push_apply_ctx(ApplyCtx { cb, index });
            drop(server);

            let res = rx.next().await.unwrap();

            match res {
                RequestRes::Timeout => {
                    reply.wrong_leader = false;
                    reply.err = String::from("Timeout");
                }
                RequestRes::Success(_) => {
                    reply.wrong_leader = false;
                    // todo: duplicate detection
                }
                RequestRes::Other => {
                    unimplemented!();
                }
            }
        } else {
            reply.wrong_leader = true;
        }

        reply
    }

    pub async fn get(&self, arg: GetRequest) -> GetReply {
        let mut reply = GetReply::default();
        reply.wrong_leader = true;

        let (cb, mut rx): (UnboundedSender<RequestRes>, UnboundedReceiver<RequestRes>) =
            unbounded();

        let mut timeout_cb = cb.clone();
        // 超时提醒
        let _ = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            let _ = block_on(async { timeout_cb.send(RequestRes::Timeout).await });
        });
        let mut server = self.server.lock().await;
        let mut data = vec![];
        labcodec::encode(&arg, &mut data).unwrap();
        let raft_command = RaftCommand { op_type: 0, data };

        if let Ok((index, _)) = server.rf.start(&raft_command) {
            server.push_apply_ctx(ApplyCtx { cb, index });
            drop(server);
            let res = rx.next().await.unwrap();

            match res {
                RequestRes::Timeout => {
                    reply.err = String::from("Timeout");
                }
                RequestRes::Success(value) => {
                    reply.wrong_leader = false;
                    reply.value = value;
                }
                RequestRes::Other => {
                    unimplemented!();
                }
            }
        } else {
            reply.wrong_leader = true;
        }

        reply
    }
}

pub enum RequestRes {
    Timeout,
    Success(String),
    Other,
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.
        Ok(self.get(arg).await)
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    // todo: duplicate detections
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        Ok(self.put_append(arg).await)
    }
}
