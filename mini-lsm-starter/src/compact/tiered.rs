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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // triggered by space amplification ratio
        let last_level_size = snapshot.levels.last()?.1.len();
        // engine size is the sum of all levels except the last level
        let engine_size = snapshot
            .levels
            .iter()
            .take(snapshot.levels.len() - 1)
            .map(|(_, files)| files.len())
            .sum::<usize>();
        let space_amp_ratio = engine_size as f64 / last_level_size as f64 * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // triggered by size ratio
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0usize;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = next_level_size as f64 / size as f64;
            if current_size_ratio > size_ratio_trigger && id + 1 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {} > {}",
                    current_size_ratio * 100.0,
                    size_ratio_trigger * 100.0
                );
                return Some(TieredCompactionTask {
                    // Compact upper tiers to increase their total size. Size ratio trigger will
                    // never include the bottom level.
                    tiers: snapshot.levels.iter().take(id + 1).cloned().collect(),
                    bottom_tier_included: false,
                });
            }
        }

        // triggered by max merge width(first up to max_merge_width tiers)
        let num_tiers_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        println!("compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        assert!(
            !output.is_empty(),
            "tiered compaction output should not be empty"
        );

        let mut new_state = snapshot.clone();
        let mut tiers_to_remove = task
            .tiers
            .iter()
            .map(|(tier_id, sst_ids)| (*tier_id, sst_ids))
            .collect::<HashMap<_, _>>();
        let mut files_to_remove = Vec::new();
        let new_tier_id = output[0];
        let mut levels = Vec::<(usize, Vec<usize>)>::new();
        let mut new_tier_added = false;
        for (tier_id, files) in &new_state.levels {
            if let Some(sst_ids) = tiers_to_remove.remove(tier_id) {
                assert_eq!(sst_ids, files, "file changed after issuing compaction task");
                files_to_remove.extend(sst_ids.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()));
            }
            if !new_tier_added && tiers_to_remove.is_empty() {
                levels.push((new_tier_id, output.to_vec()));
                new_tier_added = true;
            }
        }
        if !tiers_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }
        new_state.levels = levels;
        (new_state, files_to_remove)
    }
}
