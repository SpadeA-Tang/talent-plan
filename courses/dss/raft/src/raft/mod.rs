use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::lock::MutexGuard;
use futures::select;
use futures::SinkExt;
use futures::StreamExt;
use rand::{self, Rng};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod progress;
pub mod qurroum;
pub mod node;
pub mod raft;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

use progress::*;
use qurroum::*;
use node::*;

const ElectionTimeout: u64 = 300;
const SleepDuration: u64 = 30;