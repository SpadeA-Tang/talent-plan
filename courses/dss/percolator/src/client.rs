use labrpc::*;

use crate::service::{TSOClient, TransactionClient};

// new added ---------------
use crate::msg::{TimestampRequest, TimestampResponse,};
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
    txn_client: TransactionClient
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {tso_client, txn_client}
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        use futures::executor::block_on;
        let mut wait_factor = 1;
        for i in 0..RETRY_TIMES {
            if let Ok(res) = block_on(async {self.tso_client.get_timestamp(&TimestampRequest{}).await} ) {
                return Ok(res.ts)
            }
            thread::sleep(Duration::from_millis( wait_factor * BACKOFF_TIME_MS));
            wait_factor <<= 1;
        }

        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        unimplemented!()
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        unimplemented!()
    }
}
