use std::collections::HashMap;


pub struct Progress {

}

pub struct ProgressTracker {
    votes: HashMap<usize, bool>,
    progress_map: HashMap<usize, Progress>,
}



#[derive(PartialEq)]
pub enum VoteResult {
    VotePending,
    VoteLost,
    VoteWin
}

impl ProgressTracker {
    pub fn new() -> ProgressTracker {
        ProgressTracker {
            votes: HashMap::new(),
            progress_map: HashMap::new(),
        }
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
            return VoteResult::VotePending
        };

        let mut grant_num = 0;
        let mut reject_num = 0;
        for (&id, &grant) in &self.votes {
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