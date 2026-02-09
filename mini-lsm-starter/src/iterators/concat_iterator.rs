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


use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let mut sstables = sstables;
        sstables.sort_by_key(|sst| sst.first_key().clone());

        // Initialize `current` to the first valid table iterator (if any).
        let mut next_sst_idx = 0;
        let mut current = None;
        while next_sst_idx < sstables.len() {
            let iter =
                SsTableIterator::create_and_seek_to_first(Arc::clone(&sstables[next_sst_idx]))?;
            next_sst_idx += 1;
            if iter.is_valid() {
                current = Some(iter);
                break;
            }
        }

        Ok(Self {
            current,
            next_sst_idx,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let mut sstables = sstables;
        sstables.sort_by_key(|sst| sst.first_key().clone());

        // SSTables are non-overlapping and ordered by key range, so `last_key` is also increasing.
        // Find the first table whose `last_key` >= `key` (i.e. could contain `key`).
        let idx = sstables.partition_point(|sst| sst.last_key().as_key_slice() < key);

        // If `key` is larger than all tables, the iterator should be invalid.
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }

        // Try seeking to `key` in the selected table; if that yields an invalid iterator, fall back
        // to the next tables by seeking to first.
        let mut next_sst_idx = idx;
        let mut current = None;

        let iter =
            SsTableIterator::create_and_seek_to_key(Arc::clone(&sstables[next_sst_idx]), key)?;
        next_sst_idx += 1;
        if iter.is_valid() {
            current = Some(iter);
        }

        while current.is_none() && next_sst_idx < sstables.len() {
            let iter =
                SsTableIterator::create_and_seek_to_first(Arc::clone(&sstables[next_sst_idx]))?;
            next_sst_idx += 1;
            if iter.is_valid() {
                current = Some(iter);
                break;
            }
        }

        Ok(Self {
            current,
            next_sst_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current
            .as_ref()
            .map_or(KeySlice::from_slice(&[]), |current| current.key())
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().map_or(&[], |current| current.value())
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map_or(false, |current| current.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }

        if let Some(current) = self.current.as_mut() {
            current.next()?;
        }

        // If the current table is exhausted, move to the next table (if any).
        let exhausted = self
            .current
            .as_ref()
            .map_or(true, |current| !current.is_valid());
        if !exhausted {
            return Ok(());
        }

        while self.next_sst_idx < self.sstables.len() {
            let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(
                &self.sstables[self.next_sst_idx],
            ))?;
            self.next_sst_idx += 1;
            if iter.is_valid() {
                self.current = Some(iter);
                return Ok(());
            }
        }

        // No more tables.
        self.current = None;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        if let Some(current) = self.current.as_ref() {
            current.num_active_iterators()
        } else {
            0
        }
    }
}
