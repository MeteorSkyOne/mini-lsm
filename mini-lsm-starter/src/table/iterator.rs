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

use super::{BlockMeta, SsTable};
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Find the block index that may contain `key` using `first_key` metadata.
    ///
    /// Returns the last block whose `first_key` <= `key` (i.e. `upper_bound(first_key, key) - 1`),
    /// clamped to 0 when `key` is smaller than the first block.
    fn find_block_idx(blk_meta: &[BlockMeta], key: KeySlice) -> usize {
        if blk_meta.is_empty() {
            return 0;
        }

        let mut low: usize = 0;
        let mut high: usize = blk_meta.len(); // exclusive
        while low < high {
            let mid = low + (high - low) / 2;
            if blk_meta[mid].first_key.as_key_slice() <= key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if low == 0 { 0 } else { low - 1 }
    }

    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let iter = Self {
            table: table.clone(),
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
            blk_idx: 0,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0)?);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_meta = &table.block_meta;
        let blk_len = table.num_of_blocks();
        if blk_len == 0 {
            // Shouldn't happen for a valid SST, but keep it safe.
            return Ok(Self {
                table: table.clone(),
                blk_iter: BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
                blk_idx: 0,
            });
        }

        let mut blk_idx = Self::find_block_idx(blk_meta, key);
        let mut blk_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);

        // If the key is larger than all keys in this block, move to the next block (if any).
        if !blk_iter.is_valid() && blk_idx + 1 < blk_len {
            blk_idx += 1;
            blk_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(blk_idx)?);
        }

        Ok(Self {
            table: table.clone(),
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let blk_meta = &self.table.block_meta;
        let blk_len = self.table.num_of_blocks();
        if blk_len == 0 {
            return Ok(());
        }

        let mut blk_idx = Self::find_block_idx(blk_meta, key);
        self.blk_iter =
            BlockIterator::create_and_seek_to_key(self.table.read_block_cached(blk_idx)?, key);
        self.blk_idx = blk_idx;

        // If the key is larger than all keys in this block, move to the next block (if any).
        if !self.blk_iter.is_valid() && blk_idx + 1 < blk_len {
            blk_idx += 1;
            self.blk_iter =
                BlockIterator::create_and_seek_to_first(self.table.read_block_cached(blk_idx)?);
            self.blk_idx = blk_idx;
        }

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_idx < self.table.num_of_blocks() && self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
            }
        }
        Ok(())
    }
}
