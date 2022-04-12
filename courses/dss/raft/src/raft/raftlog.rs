use super::raft::Entry;


pub struct RaftLog {
    pub commit_idx: u64,
    pub apply_idx: u64,
    pub entries: Vec<Entry>,

    // it records the last log index when become the leader
    pub prev_last_log_index: u64,
    last_index_in_snapshot: u64,
}

impl RaftLog {
    pub fn new() -> RaftLog {
        RaftLog {
            commit_idx: 0,
            apply_idx: 0,
            // 哨兵, 这样第一条log的index为1
            entries: vec![Entry{term: 0, index: 0, data: Vec::new()}],
            prev_last_log_index: 0,
            last_index_in_snapshot: 0,
        }
    }


    // todo: fix this
    pub fn term(&self, log_index: u64) -> u64 {
        if log_index <= self.last_index_in_snapshot || log_index > self.last_index() {
            0
        } else {
            self.get_entry(log_index).term
        }

    }

    pub fn match_term(&self, index: u64, term: u64) -> bool {
        term == self.term(index)
    }

    pub fn find_conflict_entry(&self, entries: &Vec<Entry>) -> u64 {
        for ent in entries {
            if !self.match_term(ent.index, ent.term) {
                return ent.index
            }
        }
        0
    }

    pub fn commit(&mut self, log_index: u64) -> bool {
        if log_index > self.last_index() {
            return false;
        }
        if log_index > self.commit_idx && log_index > self.prev_last_log_index {
            self.commit_idx = log_index;
            true
        } else {
            false
        }
        
    }

    pub fn last_index(&self) -> u64 {
        let len = self.entries.len();
        if len == 0 {
            // todo: consider the case of restart
            return self.last_index_in_snapshot;
        }

        self.entries[len - 1].index
    }

    pub fn trunct_append(&mut self, ents: Vec<Entry>) {
        if ents.len() == 0 {
            return;
        }

    }

    pub fn real_index(&self, log_index: u64) -> u64 {
        assert!(log_index >= self.last_index_in_snapshot);
        let real_index = log_index - self.last_index_in_snapshot;
        if real_index as usize >= self.entries.len() {
            println!("Realindex {} len {}", real_index, self.entries.len());
        }
        assert!(real_index < self.entries.len() as u64);
        real_index
    }

    pub fn append(&mut self, ent: Entry) {
        self.entries.push(ent);
    }

    pub fn append_entries(&mut self, ents: &[Entry]) {
        for ent in ents {
            self.append(ent.clone());
        }

    }

    pub fn trunct(&mut self, len: usize) {
        self.entries.truncate(len);
    }

    pub fn get_entry(&self, log_index: u64) -> &Entry {
        &self.entries[self.real_index(log_index) as usize]
    }
}
