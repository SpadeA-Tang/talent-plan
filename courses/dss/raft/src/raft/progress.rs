use std::collections::HashMap;

pub struct Progress {
    pub next_index: u64,
    pub matched_index: u64,
}

impl Progress {
    pub fn new() -> Progress {
        Progress {
            next_index: 0,
            matched_index: 0,
        }
    }
}

pub struct ProgressTracker {
    votes: HashMap<usize, bool>,
    pub progress_map: HashMap<usize, Progress>,
}

#[derive(PartialEq)]
pub enum VoteResult {
    VotePending,
    VoteLost,
    VoteWin,
}

impl ProgressTracker {
    pub fn new(peer_ids: &Vec<usize>) -> ProgressTracker {
        let mut progress_map = HashMap::new();
        for &id in peer_ids {
            progress_map.insert(id, Progress::new());
        }
        ProgressTracker {
            votes: HashMap::new(),
            progress_map,
        }
    }

    pub fn get_commit_index(&self) -> u64 {
        let mut indices = Vec::new();
        for (_, pr) in &self.progress_map {
            indices.push(pr.matched_index);
        }

        indices.sort();

        let n = self.progress_map.len();
        let qurroum_idx = n - (n / 2 + 1);

        indices[qurroum_idx]
    }

    pub fn reset_progress<F>(&mut self, mut f: F)
    where
        F: FnMut(usize, &mut Progress),
    {
        for (&id, pr) in &mut self.progress_map {
            f(id, pr);
        }
    }

    pub fn may_update(&mut self, id: usize, log_index: u64) -> bool {
        let mut pr = self.progress_map.get_mut(&id).unwrap();
        if pr.matched_index >= log_index {
            return false;
        }
        pr.matched_index = log_index;
        pr.next_index = std::cmp::max(pr.next_index, log_index + 1);
        true
    }

    pub fn reset_vote_record(&mut self) {
        self.votes = HashMap::new();
    }

    // The caller should check the term of the voter
    pub fn record_vote(&mut self, id: usize, grant: bool) {
        self.votes.insert(id, grant);
    }

    pub fn tally_vote(&self) -> VoteResult {
        // The length of progress_map reflects the number of peers
        // the length of votes only reflects the number of peers who has made decision
        let total = self.progress_map.len();
        let num_voters = self.votes.len();
        let quorum_num = super::qurroum::quorum_num(total);
        if num_voters < quorum_num {
            return VoteResult::VotePending;
        };

        let mut grant_num = 0;
        let mut reject_num = 0;
        for (&_, &grant) in &self.votes {
            if grant {
                grant_num += 1;
            } else {
                reject_num += 1;
            }
        }

        if grant_num >= quorum_num {
            VoteResult::VoteWin
        } else if reject_num >= quorum_num {
            VoteResult::VoteLost
        } else {
            VoteResult::VotePending
        }
    }
}
