use crate::{ClientId, RaftCommand, SeqNum};
use bincode_next::{Decode, Encode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Decode, Encode)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
    pub client_id: ClientId,
    pub seq_num: SeqNum,
}

impl LogEntry {
    pub fn new(
        term: u64,
        index: u64,
        command: RaftCommand,
        client_id: ClientId,
        seq_num: SeqNum,
    ) -> Self {
        Self {
            term,
            index,
            command,
            client_id,
            seq_num,
        }
    }
}

/// In-memory log
#[derive(Debug, Default)]
pub struct Log {
    /// entries[0] is first entry after snapshot
    entries: Vec<LogEntry>, // index 0 = dummy (term=0, index=0)
    snapshot_index: u64,
    snapshot_term: u64,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: vec![LogEntry::new(
                0,
                0,
                RaftCommand::ClientCommand(vec![]),
                ClientId(0),
                SeqNum(0),
            )], // dummy entry
            snapshot_index: 0,
            snapshot_term: 0,
        }
    }

    pub fn set_snapshot(&mut self, index: u64, term: u64) {
        self.snapshot_index = index;
        self.snapshot_term = term;
        if index > 0 {
            // Delete dummy from snapshot.
            self.entries.clear();
        }
        // If index == 0, snapshot does not exist. Leave dummy for now.
    }

    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> u64 {
        self.snapshot_term
    }

    pub fn first_index(&self) -> u64 {
        if self.entries.is_empty() {
            self.snapshot_index + 1
        } else {
            let idx = if self.snapshot_index == 0 { 1 } else { 0 };
            self.entries
                .get(idx)
                .map(|e| e.index)
                .unwrap_or(self.snapshot_index + 1)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Length not including snapshots
    pub fn len(&self) -> usize {
        if self.snapshot_index == 0 {
            self.entries.len() - 1 // including dummy
        } else {
            self.entries.len()
        }
    }

    /// Last index in log.
    /// If log is empty, snapshot_index is returned.
    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_index)
    }

    /// Last term in log.
    /// If log is empty, snapshot_term is returned.
    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    /// Safe way to get log entry.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index < self.snapshot_index {
            // Entry is  included in shapshot.
            return None;
        }

        if index == self.snapshot_index {
            if self.snapshot_index == 0 {
                return self.entries.get(0); // dummy entry
            }
            return None;
        }

        let vec_idx = if self.snapshot_index == 0 {
            index as usize
        } else {
            (index - self.snapshot_index - 1) as usize
        };

        self.entries.get(vec_idx)
    }

    pub fn append(&mut self, entries: Vec<LogEntry>) {
        self.entries.extend(entries);
    }

    /// Delete entries starting from `index`
    pub fn truncate_from(&mut self, index: u64) {
        if index <= self.snapshot_index {
            return; // cant truncate log inside shapshot
        }

        let vec_idx = if self.snapshot_index == 0 {
            index as usize
        } else {
            (index - self.snapshot_index - 1) as usize
        };

        self.entries.truncate(vec_idx);
    }

    /// Get entries in range [`start`, `end`)
    pub fn get_range(&self, start: u64, end: u64) -> Vec<LogEntry> {
        if start <= self.snapshot_index {
            return vec![];
        }

        let vec_start = if self.snapshot_index == 0 {
            start as usize
        } else {
            (start - self.snapshot_index - 1) as usize
        };
        let vec_end = if self.snapshot_index == 0 {
            end as usize
        } else {
            (end - self.snapshot_index - 1) as usize
        };

        let vec_end = std::cmp::min(vec_end, self.entries.len());
        if vec_start >= vec_end {
            return vec![];
        }

        self.entries[vec_start..vec_end].to_vec()
    }

    /// Deletes all entries up to `index`
    /// Used when creating a snapshot.
    pub fn compact(&mut self, index: u64, term: u64) {
        if index <= self.snapshot_index || index > self.last_index() {
            return;
        }

        let vec_idx = if self.snapshot_index == 0 {
            index as usize
        } else {
            (index - self.snapshot_index - 1) as usize
        };

        if vec_idx < self.entries.len() {
            self.entries = self.entries.split_off(vec_idx + 1);
        } else {
            self.entries.clear();
        }

        self.snapshot_index = index;
        self.snapshot_term = term;

        // ====================== DEBUG ===============================
        println!(
            "💾 [CORE LOG] Compacted log up to index {}. Volatile memory cleared.",
            index
        );
    }
}
