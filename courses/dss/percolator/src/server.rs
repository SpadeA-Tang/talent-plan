use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

use std::time::{SystemTime, UNIX_EPOCH};

use std::ops::Bound::Included;
// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    current_timestamp: u64,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        Ok(TimestampResponse {
            ts: since_the_epoch.as_millis() as u64,
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let start_key: Key = (key.clone(), ts_start_inclusive.expect("Non in timestamp"));
        let end_key: Key = (key.clone(), ts_end_inclusive.expect("Non in timestamp"));
        let mut res = match column {
            Column::Data => {
                let range = self.data.range((Included(start_key), Included(end_key)));
                range.last()
            }
            Column::Write => {
                let range = self.write.range((Included(start_key), Included(end_key)));
                range.last()
            }
            Column::Lock => {
                let range = self.lock.range((Included(start_key), Included(end_key)));
                range.last()
            }
        };


        // 判断the latest key-value record是否代表删除
        if let Some(res_inner) = res {
            let val = res_inner.1;
            match val {
                Value::Timestamp(ts) => {
                    if *ts == 0 as u64 {
                        return None;
                    }
                },
                Value::Vector(vec) => {
                    if vec.len() == 0 {
                        return None;
                    }
                }
            }
        };

        res
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        match column {
            Column::Data => {
                self.data.insert((key, ts), value);
            }
            Column::Write => {
                self.write.insert((key, ts), value);
            }
            Column::Lock => {
                self.lock.insert((key, ts), value);
            }
        }
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        // Column::Write 列的Value可能是Value::Timestamp类型的, 对于该类型，用0代表删除了
        // 对于其他的，用空vec代表删除
        match column {
            Column::Write => {
                self.write(key, column, commit_ts, Value::Timestamp(0));
            },
            _ => {
                self.write(key, column, commit_ts, Value::Vector(Vec::new()));
            }
        }
        
    }

    pub fn new() -> Self {
        KvTable {
            data: BTreeMap::new(),
            write: BTreeMap::new(),
            lock: BTreeMap::new(),
        }
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        unimplemented!()
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        unimplemented!()
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        unimplemented!()
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str;

    // 封装read， 使得读出来更直接
    fn read_val_in_table(
        kv_table: &KvTable,
        key: &str,
        col: Column,
        start_ts: Option<u64>,
        end_ts: Option<u64>,
    ) -> Option<String> {
        match kv_table.read(key.as_bytes().to_owned(), Column::Data, start_ts, end_ts) {
            Some(val) => match val.1 {
                Value::Timestamp(ts) => Some(ts.to_string()),
                Value::Vector(us) => Some(str::from_utf8(us).expect("Something wrong").to_owned()),
            },
            None => None,
        }
    }

    #[test]
    fn test_kvtable_read_key_ts() {
        let mut kv_table = KvTable::new();
        kv_table.write(
            "key1".as_bytes().to_owned(),
            Column::Data,
            1,
            Value::Vector("val1".as_bytes().to_owned()),
        );

        kv_table.write(
            "key1".as_bytes().to_owned(),
            Column::Data,
            2,
            Value::Vector("val2".as_bytes().to_owned()),
        );

        kv_table.write(
            "key1".as_bytes().to_owned(),
            Column::Data,
            4,
            Value::Vector("val4".as_bytes().to_owned()),
        );

        let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(3));

        assert_eq!(Some("val2".to_owned()), res);
    }

    #[test]
    fn test_kvtable_nonexsit_key() {
        let mut kv_table = KvTable::new();
        let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(1));
        assert_eq!(None, res);
    }

    #[test]
    fn test_kvtable_removed_key() {
        let mut kv_table = KvTable::new();
        kv_table.write(
            "key1".as_bytes().to_owned(),
            Column::Data,
            1,
            Value::Vector("val1".as_bytes().to_owned()),
        );
        let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(2));
        assert_eq!(Some("val1".to_owned()), res);
        kv_table.erase("key1".as_bytes().to_owned(), Column::Data, 3);

        // we can still read it at previous timestamp
        let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(2));
        assert_eq!(Some("val1".to_owned()), res);

        // but we cannot read it after ts 3
        let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(3), Some(3));
        println!("{:?}", res);
        assert_eq!(None, res);
    }
}
