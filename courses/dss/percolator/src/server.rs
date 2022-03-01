use core::panic;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

use std::time::{SystemTime, UNIX_EPOCH};

// use env_logger::fmt::Timestamp;
use labcodec::DecodeError;
use labrpc::{Error, Result};
use prost::bytes::Buf;

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
            ts: since_the_epoch.as_micros() as u64,
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

use std::fmt;

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Timestamp(ts) => {
                write!(f, "Timestamp {}", ts)
            }
            Value::Vector(ve) => {
                write!(f, "Vector {}", std::str::from_utf8(ve).unwrap())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Column::Write => {
                write!(f, "Write")
            },
            Column::Data => {
                write!(f, "Data")
            },
            Column::Lock => {
                write!(f, "Lock")
            }
        }
    }
}

impl Column {
    pub(crate) fn decode(column: u32) -> Result<Column> {
        match column {
            1 => Ok(Column::Write),
            2 => Ok(Column::Data),
            3 => Ok(Column::Lock),
            _ => Err(Error::Decode(prost::DecodeError::new(
                "Decode Column error",
            ))),
        }
    }
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
                &Value::Timestamp(ts) => {
                    if ts == 0 as u64 {
                        return None;
                    }
                }
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
                log::info!(
                    "Write -- Data: key: ({}, {}) value: {}",
                    std::str::from_utf8(&key).unwrap(),
                    ts,
                    value
                );
                self.data.insert((key, ts), value);
            }
            Column::Write => {
                log::info!(
                    "Write -- Write: key: ({}, {}) value: {}",
                    std::str::from_utf8(&key).unwrap(),
                    ts,
                    value
                );

                self.write.insert((key, ts), value);
            }
            Column::Lock => {
                log::info!(
                    "Write -- Lock: key: ({}, {}) value: {}",
                    std::str::from_utf8(&key).unwrap(),
                    ts,
                    value
                );
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
        log::info!(
            "Erase: key: {} Column: {} commit_ts: {}",
            std::str::from_utf8(&key).unwrap(),
            column,
            commit_ts,
        );
        match column {
            Column::Write => {
                self.write(key, column, commit_ts, Value::Timestamp(0));
            }
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
        let mut kvtable = self.data.lock().unwrap();
        let mut backoff = 1;
        loop {
            // Check for locks that signal concurrent writes.
            if let Some(res) =
                kvtable.read(req.key.clone(), Column::Lock, Some(0), Some(req.start_ts))
            {
                // There is a pending lock; wait it;
                drop(kvtable);

                self.back_off_maybe_clean_up_lock(req.start_ts, req.key.clone(), backoff);
                kvtable = self.data.lock().unwrap();
                backoff <<= 1;
                continue;
            }

            // 先从write列中读取req.ts能读到的最新的data的timestamp
            match kvtable.read(req.key.clone(), Column::Write, Some(0), Some(req.start_ts)) {
                Some(res) => {
                    if let Value::Timestamp(ts) = res.1 {
                        // 再从data列读取与才在column列读到的ts相关联的数据
                        match kvtable.read(req.key, Column::Data, Some(*ts), Some(*ts)) {
                            Some((_, Value::Vector(res))) => {
                                return Ok(GetResponse {
                                    val: res.to_vec(),
                                });
                            }
                            _ => {
                                return panic!(
                                    "The data specified by write column should be exist"
                                );
                            }
                        }
                    } else {
                        panic!("Only timestamp type should be in write column")
                    }
                }
                None => {
                    return Ok(GetResponse {
                        val: Vec::new(),
                    });
                }
            }
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        let mut kvtable = self.data.lock().unwrap();

        // Abort on writes after our start timestamp . . .
        if let Some(res) = kvtable.read(
            req.key.clone(),
            Column::Write,
            Some(req.start_ts),
            Some(u64::MAX),
        ) {
            return Ok(PrewriteResponse { success: false });
        }
        // ... or locks at any timestamp.
        if let Some(res) = kvtable.read(req.key.clone(), Column::Lock, Some(0), Some(u64::MAX)) {
            return Ok(PrewriteResponse { success: false });
        }

        kvtable.write(
            req.key.clone(),
            Column::Data,
            req.start_ts,
            Value::Vector(req.val),
        );
        kvtable.write(
            req.key,
            Column::Lock,
            req.start_ts,
            Value::Vector(req.prime),
        );

        Ok(PrewriteResponse { success: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let mut kvtable = self.data.lock().unwrap();
        if req.is_primary {
            // 检查主锁是否还在
            match kvtable.read(
                req.key.clone(),
                Column::Lock,
                Some(req.start_ts),
                Some(req.start_ts),
            ) {
                Some(_) => {}
                None => {
                    return Ok(CommitResponse { success: false });
                }
            }
        };

        // 讲start_ts写入Column::Write后, start_ts处的data生效
        kvtable.write(req.key.clone(), Column::Write, req.commit_ts, Value::Timestamp(req.start_ts));
        kvtable.erase(req.key, Column::Lock, req.commit_ts);

        Ok(CommitResponse { success: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>, back_off: u64) {
        // Your code here.
        if back_off <= 64 {
            std::thread::sleep(Duration::from_millis(back_off * 5));
        } else {
            let mut kvtable = self.data.lock().unwrap();

            if let Some((&(_, old_start_ts), Value::Vector(primary))) = kvtable.read(key.clone(), Column::Lock, Some(0), Some(start_ts)) {
                // 确认primary lock是否还存在
                if let Some(_) = kvtable.read(primary.clone(), Column::Lock, Some(0), Some(start_ts)) {
                    // 存在，说明还没有提交；需要roll-back
                    let primary = primary.clone();
                    log::info!("");
                    log::info!("Roll-back: Erase the primary's key");
                    kvtable.erase(primary, Column::Lock, start_ts);
                    kvtable.erase(key.clone(), Column::Lock, start_ts);
                } else {
                    // 不存在，说明已经提交了；此时需要roll-forward； 此时在主key的Column列 能找到 值为 Value::Timestamp(old_start_ts)
                    // 或者主key已经被其他事物清理掉；此时需要roll-back；在主key的Column列 无法找到 值为 Value::Timestamp(old_start_ts)

                    // 此刻去寻找 primary的Column列，找到value为old_start_ts的那一行，该行key中的ts即为commit_ts
                    // 然后我们便可用该commit_ts来完成之前未完成的提交
                    let mut end_ts = start_ts;
                    loop {
                        log::info!("");
                        log::info!("Roll-forward: start...");
                        log::info!("Roll-forward: try timestamp {}", end_ts);
                        if let Some((&(_, commit_ts), Value::Timestamp(ts))) = kvtable.read(primary.clone(), Column::Write, Some(0), Some(end_ts)) {
                            log::info!("Roll-forward: timestamp {} is got", commit_ts);
                            if &old_start_ts == ts {
                                log::info!("Roll-forward: timestamp {} is what we want", commit_ts);
                                
                                kvtable.write(key.clone(), Column::Write, commit_ts, Value::Timestamp(old_start_ts));
                                kvtable.erase(key.clone(), Column::Lock, commit_ts);
                                log::info!("Roll-forward: end...\n");
                                
                                break;
                            } else {
                                end_ts = ts - 1;
                            }
                        } else {
                            
                            // 无法找到 值为 Value::Timestamp(old_start_ts)
                            // 执行清理工作
                            kvtable.erase(key.clone(), Column::Lock, start_ts);
                            break;
                        }
                    }
                }
            }
        }
    }
}

// #[cfg(test)]
// mod test_kvtable {
//     use super::*;
//     use std::str;

//     // 封装read， 使得读出来更直接
//     fn read_val_in_table(
//         kv_table: &KvTable,
//         key: &str,
//         col: Column,
//         start_ts: Option<u64>,
//         end_ts: Option<u64>,
//     ) -> Option<String> {
//         match kv_table.read(key.as_bytes().to_owned(), Column::Data, start_ts, end_ts) {
//             Some(val) => match val.1 {
//                 Value::Timestamp(ts) => Some(ts.to_string()),
//                 Value::Vector(us) => Some(str::from_utf8(us).expect("Something wrong").to_owned()),
//             },
//             None => None,
//         }
//     }

//     // 验证读不到比ts更新的值
//     #[test]
//     fn test_kvtable_read_key_ts() {
//         let mut kv_table = KvTable::new();
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Data,
//             1,
//             Value::Vector("val1".as_bytes().to_owned()),
//         );

//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Data,
//             2,
//             Value::Vector("val2".as_bytes().to_owned()),
//         );

//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Data,
//             4,
//             Value::Vector("val4".as_bytes().to_owned()),
//         );

//         let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(3));

//         assert_eq!(Some("val2".to_owned()), res);
//     }

//     #[test]
//     fn test_kvtable_nonexsit_key() {
//         let mut kv_table = KvTable::new();
//         let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(1));
//         assert_eq!(None, res);
//     }

//     #[test]
//     fn test_kvtable_removed_key() {
//         let mut kv_table = KvTable::new();
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Data,
//             1,
//             Value::Vector("val1".as_bytes().to_owned()),
//         );
//         let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(2));
//         assert_eq!(Some("val1".to_owned()), res);
//         kv_table.erase("key1".as_bytes().to_owned(), Column::Data, 3);

//         // we can still read it at previous timestamp
//         let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(0), Some(2));
//         assert_eq!(Some("val1".to_owned()), res);

//         // but we cannot read it after ts 3
//         let res = read_val_in_table(&kv_table, "key1", Column::Data, Some(3), Some(3));
//         println!("{:?}", res);
//         assert_eq!(None, res);
//     }
// }

// #[cfg(test)]
// mod test_memory_storage {
//     use crate::service::transaction::Service;

//     use super::*;
//     use futures::executor::block_on;

//     #[test]
//     fn test_get() {
//         let mut kv_table = KvTable::new();
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Data,
//             1,
//             Value::Vector("val1".as_bytes().to_owned()),
//         );
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Write,
//             2,
//             Value::Timestamp(1),
//         );
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Data,
//             3,
//             Value::Vector("val2".as_bytes().to_owned()),
//         );

//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Write,
//             4,
//             Value::Timestamp(3),
//         );

//         let mut mem = MemoryStorage {
//             data: Arc::new(Mutex::new(kv_table)),
//         };

//         let res = block_on(async {
//             mem.get(GetRequest {
//                 start_ts: 3,
//                 key: "key1".as_bytes().to_owned(),
//             })
//             .await
//         })
//         .unwrap();
//         assert_eq!("val1".as_bytes().to_owned(), res.val);

//         let res = block_on(async {
//             mem.get(GetRequest {
//                 start_ts: 4,
//                 key: "key1".as_bytes().to_owned(),
//             })
//             .await
//         })
//         .unwrap();
//         assert_eq!("val2".as_bytes().to_owned(), res.val);

//         let res = block_on(async {
//             mem.get(GetRequest {
//                 start_ts: 0,
//                 key: "key1".as_bytes().to_owned(),
//             })
//             .await
//         })
//         .unwrap();
//         assert_eq!(Vec::<u8>::new(), res.val)
//     }

//     #[test]
//     fn test_get_something_locked() {
//         let mut kv_table = KvTable::new();
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Data,
//             2,
//             Value::Vector("val1".as_bytes().to_owned()),
//         );
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Write,
//             3,
//             Value::Timestamp(2),
//         );
//         kv_table.write(
//             "key1".as_bytes().to_owned(),
//             Column::Lock,
//             1,
//             Value::Timestamp(1),
//         );

//         let mut mem = MemoryStorage {
//             data: Arc::new(Mutex::new(kv_table)),
//         };

//         let res = block_on(async {
//             mem.get(GetRequest {
//                 start_ts: 4,
//                 key: "key1".as_bytes().to_owned(),
//             })
//             .await
//         });
//         assert_eq!(Err(Error::Timeout), res);
//     }
// }
