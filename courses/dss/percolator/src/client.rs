use labrpc::*;

use crate::service::{TSOClient, TransactionClient};

// new added ---------------
use crate::msg::{CommitRequest, GetRequest, PrewriteRequest, TimestampRequest, TimestampResponse};
use futures::executor::block_on;
use core::panic;
use std::thread;
use std::time::Duration;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,

    start_ts_: u64,
    write_buf: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            start_ts_: 0,
            write_buf: Vec::new(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let mut wait_factor = 1;
        for i in 0..RETRY_TIMES {
            if let Ok(res) =
                block_on(async { self.tso_client.get_timestamp(&TimestampRequest {}).await })
            {
                return Ok(res.ts);
            }
            thread::sleep(Duration::from_millis(wait_factor * BACKOFF_TIME_MS));
            wait_factor <<= 1;
        }

        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        self.start_ts_ = self
            .get_timestamp()
            .expect("Error -- Get Timestamp Timeout");
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        if let Ok(res) = block_on(async {
            self.txn_client
                .get(&GetRequest {
                    start_ts: self.start_ts_,
                    key,
                })
                .await
        }) {
            return Ok(res.val);
        };

        Ok(Vec::new())
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.write_buf.push((key, value));
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        if self.write_buf.len() == 0 {
            return Ok(true);
        }
        let primary = PrewriteRequest {
            start_ts: self.start_ts_,
            key: self.write_buf[0].0.clone(),
            val: self.write_buf[0].1.clone(),
            prime: self.write_buf[0].0.clone(),
        };

        // 首先预写入primary
        if let Ok(res) = block_on(async { self.txn_client.prewrite(&primary).await }) {
            if !res.success {
                return Ok(false);
            }
        }

        // 然后写入所有的secondary
        for i in 1..self.write_buf.len() {
            let secondary = PrewriteRequest {
                start_ts: self.start_ts_,
                key: self.write_buf[i].0.clone(),
                val: self.write_buf[i].1.clone(),
                prime: self.write_buf[0].0.clone(),
            };
            if let Ok(res) = block_on(async { self.txn_client.prewrite(&secondary).await }) {
                if !res.success {
                    return Ok(false);
                }
            }
        }

        // Its time to commit
        // First, get the commit timestamp
        let commit_ts = self.get_timestamp()?;

        // First commit the primary
        let primary = CommitRequest {
            is_primary: true,
            start_ts: self.start_ts_,
            commit_ts,
            key: self.write_buf[0].0.clone(),
        };
        if let Ok(res) = block_on(async { self.txn_client.commit(&primary).await }) {
            if !res.success {
                return Ok(false);
            }
        };

        for i in 1..self.write_buf.len() {
            let secondary = CommitRequest {
                is_primary: false,
                start_ts: self.start_ts_,
                commit_ts,
                key: self.write_buf[i].0.clone(),
            };
            if let Ok(res) = block_on(async { self.txn_client.commit(&secondary).await }) {
                if !res.success {
                    panic!("It should be commit")
                }
            } else {
                panic!("It should be commit")
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_client_get() {}
}
