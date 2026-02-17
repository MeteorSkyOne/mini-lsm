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
use crate::manifest::ManifestRecord;
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
    /// Merge a compaction result into the given `state`.
    ///
    /// This is used when compaction is executed based on a snapshot, but we must apply its result
    /// onto the latest storage state (e.g. L0 may have new SSTs flushed while compaction is
    /// running). This function only mutates `l0_sstables` and `levels` and returns the list of SST
    /// ids that are removed from the state (and therefore safe to delete from disk).
    fn merge_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        state: &mut LsmStorageState,
        task: &CompactionTask,
        output_sst_ids: &[usize],
    ) -> Vec<usize> {
        // Always use compaction controller's apply logic to compute the intended new state.
        //
        // Note: some controllers (e.g. leveled) need to compare `first_key()` of *output* SSTs
        // when sorting the target level. The `snapshot` was taken before compaction runs, so it
        // does not contain output SST objects. We "augment" the snapshot with output SST objects
        // published into `state.sstables` to avoid panicking when looking them up.
        let mut augmented_snapshot = snapshot.clone();
        let mut missing_output_sst = false;
        for sst_id in output_sst_ids {
            if let Some(sst) = state.sstables.get(sst_id) {
                augmented_snapshot.sstables.insert(*sst_id, Arc::clone(sst));
            } else {
                missing_output_sst = true;
            }
        }

        let (applied_state, files_to_remove) = self.compaction_controller.apply_compaction_result(
            &augmented_snapshot,
            task,
            output_sst_ids,
            // Fallback: if output SSTs are missing, skip any sorting that requires SST objects.
            missing_output_sst,
        );

        // Merge into the latest state.
        // For tiered compaction the flush thread inserts new tiers at the front of
        // `levels` (not L0). Those tiers are absent from the snapshot so we must
        // preserve them; otherwise the newest data is silently dropped.
        if !self.compaction_controller.flush_to_l0() {
            let snapshot_tier_ids: Vec<usize> = snapshot.levels.iter().map(|(id, _)| *id).collect();
            let mut new_levels: Vec<(usize, Vec<usize>)> = state
                .levels
                .iter()
                .filter(|(id, _)| !snapshot_tier_ids.contains(id))
                .cloned()
                .collect();
            new_levels.extend(applied_state.levels);
            state.levels = new_levels;
        } else {
            state.levels = applied_state.levels;
        }

        // Apply L0 changes in an "incremental" way based on current state, so we don't drop
        // concurrently flushed SSTs.
        match task {
            CompactionTask::Simple(task) => {
                if task.upper_level.is_none() {
                    state
                        .l0_sstables
                        .retain(|id| !task.upper_level_sst_ids.contains(id));
                }
            }
            CompactionTask::Leveled(task) => {
                if task.upper_level.is_none() {
                    state
                        .l0_sstables
                        .retain(|id| !task.upper_level_sst_ids.contains(id));
                }
            }
            CompactionTask::ForceFullCompaction { l0_sstables, .. } => {
                state.l0_sstables.retain(|id| !l0_sstables.contains(id));
            }
            CompactionTask::Tiered(_) => {
                // Tiered compaction does not flush to L0 in this codebase.
            }
        }

        state
            .sstables
            .retain(|sst_id, _| !files_to_remove.contains(sst_id));

        files_to_remove
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let mut sst_ids = Vec::new();
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                if l0_sstables.is_empty() && l1_sstables.is_empty() {
                    return Ok(Vec::new());
                }
                sst_ids.extend(l0_sstables.iter().chain(l1_sstables.iter()).copied());
            }
            CompactionTask::Simple(task) => {
                sst_ids.extend(
                    task.upper_level_sst_ids
                        .iter()
                        .chain(task.lower_level_sst_ids.iter())
                        .copied(),
                );
            }
            CompactionTask::Tiered(task) => {
                sst_ids.extend(
                    task.tiers
                        .iter()
                        .flat_map(|(_, sst_ids)| sst_ids.iter())
                        .copied(),
                );
            }
            CompactionTask::Leveled(task) => {
                sst_ids.extend(
                    task.upper_level_sst_ids
                        .iter()
                        .chain(task.lower_level_sst_ids.iter())
                        .copied(),
                );
            }
        }
        let sstables = {
            let state = self.state.read();
            sst_ids
                .iter()
                .map(|sst_id| {
                    state
                        .sstables
                        .get(sst_id)
                        .cloned()
                        .with_context(|| format!("sst {sst_id} not found"))
                })
                .collect::<Result<Vec<_>>>()?
        };
        let mut sst_iters = Vec::with_capacity(sstables.len());
        for sst in &sstables {
            let iter = SsTableIterator::create_and_seek_to_first(sst.clone())
                .with_context(|| format!("failed to create iterator for sst {}", sst.sst_id()))?;
            sst_iters.push(Box::new(iter));
        }

        let mut merge_iter = MergeIterator::create(sst_iters);
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut new_ssts = Vec::<Arc<SsTable>>::new();
        let mut builder_has_entries = false;
        let compact_to_bottom_level = _task.compact_to_bottom_level();
        let watermark = TS_ENABLED.then(|| self.mvcc().watermark());
        // Avoid splitting entries with the same user key (ignoring ts) across different SSTs.
        // Internal key order is (user_key asc, ts desc), so all versions of a user key are
        // contiguous in the merge iterator.
        let mut split_pending_key: Option<Vec<u8>> = None;
        let mut current_user_key: Option<Vec<u8>> = None;
        let mut kept_version_at_or_below_watermark = false;
        while merge_iter.is_valid() {
            let key = merge_iter.key();
            let value = merge_iter.value();
            let user_key = key.key_ref();

            if current_user_key.as_deref() != Some(user_key) {
                current_user_key = Some(user_key.to_vec());
                kept_version_at_or_below_watermark = false;
            }

            // If we've exceeded the target size, only split when we reach a new user key.
            if let Some(pending) = &split_pending_key {
                if user_key != pending.as_slice() && builder_has_entries {
                    let sst_id = self.next_sst_id();
                    let sst_path = self.path_of_sst(sst_id);
                    let new_sst = sst_builder
                        .build(sst_id, Some(self.block_cache.clone()), sst_path)
                        .with_context(|| format!("failed to build sst {sst_id}"))?;
                    new_ssts.push(Arc::new(new_sst));
                    sst_builder = SsTableBuilder::new(self.options.block_size);
                    split_pending_key = None;
                }
            }

            // MVCC compaction rule:
            // - keep all versions with ts > watermark.
            // - for ts <= watermark, keep only the latest one per user key.
            // - if the kept latest (<= watermark) is a tombstone and we compact to the
            //   bottom level, drop the key entirely.
            let keep_entry = if let Some(watermark) = watermark {
                if key.ts() > watermark {
                    true
                } else if kept_version_at_or_below_watermark {
                    false
                } else {
                    kept_version_at_or_below_watermark = true;
                    !(compact_to_bottom_level && value.is_empty())
                }
            } else {
                // When timestamp is disabled, we can safely drop tombstones only if
                // compacting to the bottom level.
                !(compact_to_bottom_level && value.is_empty())
            };

            if !keep_entry {
                merge_iter.next()?;
                continue;
            }

            sst_builder.add(key, value);
            builder_has_entries = true;
            if split_pending_key.is_none()
                && sst_builder.estimated_size() >= self.options.target_sst_size
            {
                split_pending_key = Some(user_key.to_vec());
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

    fn remove_ssts(&self, sst_ids: &[usize]) -> Result<()> {
        for sst_id in sst_ids {
            let path = self.path_of_sst(*sst_id);
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

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;
        let ssts_to_compact = l0_sstables
            .iter()
            .chain(l1_sstables.iter())
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

        if let Some(manifest) = self.manifest.as_ref() {
            // Ensure newly created SSTs are durable before writing the manifest record that
            // references them.
            self.sync_dir()?;
            manifest.add_record(
                &self.state_lock.lock(),
                ManifestRecord::Compaction(
                    CompactionTask::ForceFullCompaction {
                        l0_sstables: l0_sstables.clone(),
                        l1_sstables: l1_sstables.clone(),
                    },
                    new_ssts.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>(),
                ),
            )?;
        }
        self.remove_ssts(&ssts_to_compact)?;
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        let sstables = self.compact(&task)?;
        let output_sst_ids = sstables.iter().map(|sst| sst.sst_id()).collect::<Vec<_>>();
        let files_to_remove;
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = (**state).clone();

            // Publish output SST objects early so apply-logic can read their first/last keys.
            // This is required by leveled compaction when it sorts SST ids by key range.
            new_state
                .sstables
                .extend(sstables.iter().map(|sst| (sst.sst_id(), Arc::clone(sst))));

            // 1) Apply compaction changes (on the latest state to avoid dropping concurrently
            // flushed L0 tables).
            files_to_remove =
                self.merge_compaction_result(&snapshot, &mut new_state, &task, &output_sst_ids);

            *state = Arc::new(new_state);
        }

        // Best-effort: delete old SST files after state update so readers won't observe missing
        // files for referenced tables.
        if let Some(manifest) = self.manifest.as_ref() {
            // Ensure newly created SSTs are durable before writing the manifest record that
            // references them.
            self.sync_dir()?;
            manifest.add_record(
                &self.state_lock.lock(),
                ManifestRecord::Compaction(task, output_sst_ids),
            )?;
        }
        self.remove_ssts(&files_to_remove)?;
        Ok(())
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
