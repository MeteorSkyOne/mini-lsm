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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        // Return **lower-level** SST ids that overlap with the *bounding range* of `sst_ids`.
        // This matches the reference behavior.
        if sst_ids.is_empty() {
            return Vec::new();
        }

        let mut begin_key = None;
        let mut end_key = None;
        for id in sst_ids {
            let sst = &snapshot.sstables[id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();

            let replace_begin = match begin_key.as_ref() {
                Some(k) => first_key < k,
                None => true,
            };
            if replace_begin {
                begin_key = Some(first_key.clone());
            }

            let replace_end = match end_key.as_ref() {
                Some(k) => last_key > k,
                None => true,
            };
            if replace_end {
                end_key = Some(last_key.clone());
            }
        }

        let (begin_key, end_key) = match (begin_key, end_key) {
            (Some(f), Some(l)) => (f, l),
            _ => return Vec::new(),
        };

        let level_sst_ids = &snapshot.levels[in_level - 1].1;
        let mut overlap_ssts = Vec::new();
        for lower_id in level_sst_ids {
            let lower_sst = &snapshot.sstables[lower_id];
            let first_key = lower_sst.first_key();
            let last_key = lower_sst.last_key();

            // Overlap test: [a,b] overlaps [c,d] iff !(b < c || a > d)
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*lower_id);
            }
        }
        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // Reference logic: compute base level and target level sizes, then:
        // 1) flush L0 to base level with top priority
        // 2) otherwise compact one SST from the level with highest (real/target) priority.
        let mut target_level_size = (0..self.options.max_levels)
            .map(|_| 0usize)
            .collect::<Vec<_>>(); // exclude L0
        let mut real_level_size = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;

        for i in 0..self.options.max_levels {
            let level_size = snapshot.levels[i]
                .1
                .iter()
                .map(|x| snapshot.sstables[x].table_size())
                .sum::<u64>() as usize;
            real_level_size.push(level_size);
        }

        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        // Select base level and compute target sizes.
        if self.options.max_levels > 0 {
            let last = self.options.max_levels - 1;
            target_level_size[last] = real_level_size[last].max(base_level_size_bytes);
            for i in (0..last).rev() {
                let next_level_size = target_level_size[i + 1];
                let this_level_size = next_level_size / self.options.level_size_multiplier;
                if next_level_size > base_level_size_bytes {
                    target_level_size[i] = this_level_size;
                }
                if target_level_size[i] > 0 {
                    base_level = i + 1;
                }
            }
        }

        // Flush L0 SSTs is the top priority.
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for level in 0..self.options.max_levels {
            let prio = if target_level_size[level] == 0 {
                if real_level_size[level] == 0 {
                    0.0
                } else {
                    f64::INFINITY
                }
            } else {
                real_level_size[level] as f64 / target_level_size[level] as f64
            };
            if prio > 1.0 {
                priorities.push((prio, level + 1));
            }
        }

        priorities.sort_by(|a, b| match a.0.partial_cmp(&b.0) {
            Some(ord) => ord.reverse(),
            None => std::cmp::Ordering::Equal,
        });

        if let Some((_, level)) = priorities.first().copied() {
            // Bottom level cannot be compacted further.
            if level >= self.options.max_levels {
                return None;
            }

            // Select the oldest SST (smallest id) in that level.
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied();
            if let Some(selected_sst) = selected_sst {
                return Some(LeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: vec![selected_sst],
                    lower_level: level + 1,
                    lower_level_sst_ids: self.find_overlapping_ssts(
                        snapshot,
                        &[selected_sst],
                        level + 1,
                    ),
                    is_lower_level_bottom_level: level + 1 == self.options.max_levels,
                });
            }
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();

        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        if let Some(upper_level) = task.upper_level {
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            debug_assert!(upper_level_sst_ids_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            debug_assert!(upper_level_sst_ids_set.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }

        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if lower_level_sst_ids_set.remove(x) {
                    return None;
                }
                Some(*x)
            })
            .collect::<Vec<_>>();
        debug_assert!(lower_level_sst_ids_set.is_empty());
        new_lower_level_ssts.extend(output);

        // Don't sort the SST IDs during recovery because actual SSTs may not be loaded.
        if !in_recovery {
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot.sstables[x]
                    .first_key()
                    .cmp(snapshot.sstables[y].first_key())
            });
        }

        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, files_to_remove)
    }
}
