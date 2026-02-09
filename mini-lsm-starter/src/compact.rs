// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::key::TS_ENABLED;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                if l0_sstables.is_empty() && l1_sstables.is_empty() {
                    return Ok(Vec::new());
                }
                let ssts = {
                    let state = self.state.read();
                    l0_sstables
                        .iter()
                        .chain(l1_sstables.iter())
                        .map(|sst_id| {
                            state
                                .sstables
                                .get(sst_id)
                                .cloned()
                                .with_context(|| format!("sst {sst_id} not found"))
                        })
                        .collect::<Result<Vec<_>>>()?
                };

                let mut sst_iters = Vec::with_capacity(ssts.len());
                for sst in &ssts {
                    let iter = SsTableIterator::create_and_seek_to_first(sst.clone())
                        .with_context(|| {
                            format!("failed to create iterator for sst {}", sst.sst_id())
                        })?;
                    sst_iters.push(Box::new(iter));
                }

                let mut merge_iter = MergeIterator::create(sst_iters);
                let mut sst_builder = SsTableBuilder::new(self.options.block_size);
                let mut new_ssts = Vec::<Arc<SsTable>>::new();
                let mut builder_has_entries = false;
                while merge_iter.is_valid() {
                    let key = merge_iter.key();
                    let value = merge_iter.value();

                    // When compacting to the bottom level, we can safely drop tombstones if
                    // timestamp is disabled (week 1 mode). With timestamp enabled, we must keep
                    // historical versions.
                    if !TS_ENABLED && _task.compact_to_bottom_level() && value.is_empty() {
                        merge_iter.next()?;
                        continue;
                    }

                    sst_builder.add(key, value);
                    builder_has_entries = true;
                    if sst_builder.estimated_size() >= self.options.target_sst_size {
                        let sst_id = self.next_sst_id();
                        let sst_path = self.path_of_sst(sst_id);
                        let new_sst = sst_builder
                            .build(sst_id, Some(self.block_cache.clone()), sst_path)
                            .with_context(|| format!("failed to build sst {sst_id}"))?;
                        new_ssts.push(Arc::new(new_sst));
                        sst_builder = SsTableBuilder::new(self.options.block_size);
                        builder_has_entries = false;
                    }
                    merge_iter.next()?;
                }
                if builder_has_entries {
                    let sst_id = self.next_sst_id();
                    let sst_path = self.path_of_sst(sst_id);
                    let new_sst = sst_builder
                        .build(sst_id, Some(self.block_cache.clone()), sst_path)
                        .with_context(|| format!("failed to build sst {sst_id}"))?;
                    new_ssts.push(Arc::new(new_sst));
                }
                Ok(new_ssts)
            }
            _ => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let ssts_to_compact = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: ssts_to_compact.0.clone(),
            l1_sstables: ssts_to_compact.1.clone(),
        })?;
        let ssts_to_compact = ssts_to_compact
            .0
            .iter()
            .chain(ssts_to_compact.1.iter())
            .copied()
            .collect::<Vec<_>>();
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = (**state).clone();
            new_state
                .l0_sstables
                .retain(|sst| !ssts_to_compact.contains(sst));
            new_state.levels[0] = (1, new_ssts.iter().map(|sst| sst.sst_id()).collect()); // new SSTs added to L1
            new_state
                .sstables
                .extend(new_ssts.iter().map(|sst| (sst.sst_id(), sst.clone())));
            *state = Arc::new(new_state);
        };
        for sst in ssts_to_compact {
            let path = self.path_of_sst(sst);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("failed to remove sst file {}", path.display()));
                }
            }
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let num_memtable = snapshot.imm_memtables.len() + 1;
        if num_memtable > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
