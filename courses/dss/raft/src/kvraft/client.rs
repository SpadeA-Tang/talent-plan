use futures::executor::block_on;
use rand::Rng;
use std::{fmt, sync::Mutex};

use crate::proto::kvraftpb::*;

#[derive(Debug)]
enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    potential_leader: Mutex<Option<u64>>,
    id: u64,
    op_sequence: Mutex<u64>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        // Clerk { name, servers }
        Self {
            name,
            servers,
            potential_leader: Mutex::new(None),
            id: rand::thread_rng().gen_range(0, u64::MAX),
            op_sequence: Mutex::new(0),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.

        println!("[{}] get {}", self.id, &key);

        let mut op_sequence = self.op_sequence.lock().unwrap();
        let mut potential_leader = self.potential_leader.lock().unwrap();
        *op_sequence += 1;
        let get_arg = GetRequest { key };

        let mut leader = if let Some(leader) = *potential_leader {
            leader
        } else {
            0
        };

        loop {
            let kv_client = &self.servers[leader as usize];

            // todo: append timeout mechanism
            if let Ok(res) = block_on(async { kv_client.get(&get_arg).await }) {
                if !res.wrong_leader && res.err == String::new() {
                    *potential_leader = Some(leader);
                    return res.value;
                }
            }

            // try for another server
            leader = (leader + 1) % (self.servers.len() as u64);
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.

        println!("[{}] op {:?}", self.id, op);

        let mut op_sequence = self.op_sequence.lock().unwrap();
        let mut potential_leader = self.potential_leader.lock().unwrap();
        *op_sequence += 1;

        let mut put_append_arg = PutAppendRequest::default();
        put_append_arg.client_id = self.id;
        put_append_arg.op_sequence = *op_sequence;

        match op {
            Op::Append(key, value) => {
                put_append_arg.key = key;
                put_append_arg.value = value;
                put_append_arg.op = 1;
            }
            Op::Put(key, value) => {
                put_append_arg.key = key;
                put_append_arg.value = value;
                put_append_arg.op = 2;
            }
        }

        let mut leader = if let Some(leader) = *potential_leader {
            leader
        } else {
            0
        };

        loop {
            let kv_client = &self.servers[leader as usize];

            if let Ok(res) = block_on(async { kv_client.put_append(&put_append_arg).await }) {
                if !res.wrong_leader && res.err == String::new() {
                    *potential_leader = Some(leader);
                    return;
                }
            }

            // try for another server
            leader = (leader + 1) % (self.servers.len() as u64);
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
