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

use std::{cmp::Ordering, sync::Arc};

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_first();
        iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        if self.block.offsets.is_empty() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.first_key = KeyVec::new();
            return;
        }
        let key0_offset = self.block.offsets[0];
        let key_len = u16::from_le_bytes([
            self.block.data[key0_offset as usize],
            self.block.data[(key0_offset + 1) as usize],
        ]) as usize;

        let value0_offset = key0_offset + 2 + key_len as u16 + 8;
        self.key = KeyVec::from_vec_with_ts(
            self.block.data
                [(key0_offset + 2) as usize..(key0_offset + 2 + key_len as u16) as usize]
                .to_vec(),
            {
                let ts_offset = value0_offset as usize - 8;
                if ts_offset + 8 <= self.block.data.len() {
                    let mut ts_arr = [0u8; 8];
                    ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                    u64::from_le_bytes(ts_arr)
                } else {
                    0
                }
            },
        );
        let value_len = u16::from_le_bytes([
            self.block.data[value0_offset as usize],
            self.block.data[(value0_offset + 1) as usize],
        ]) as usize;
        self.value_range = (
            (value0_offset + 2) as usize,
            (value0_offset + 2 + value_len as u16) as usize,
        );
        self.first_key = self.key.clone();
    }

    fn decompress_key(&self, data: &[u8]) -> KeyVec {
        if data.len() < 4 {
            return KeyVec::new();
        }
        let prefix_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let rest_len = u16::from_le_bytes([data[2], data[3]]) as usize;
        if prefix_len > self.first_key.key_len() {
            return KeyVec::new();
        }
        if 4usize.saturating_add(rest_len) > data.len() {
            return KeyVec::new();
        }
        let prefix = self.first_key.key_ref()[..prefix_len].to_vec();
        let rest = data[4..4 + rest_len].to_vec();
        let mut key = KeyVec::new();
        key.append(&prefix);
        key.append(&rest);
        key
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }
        let key_offset = self.block.offsets[self.idx];
        let key_len = u16::from_le_bytes([
            self.block.data[key_offset as usize],
            self.block.data[(key_offset + 1) as usize],
        ]) as usize;
        let value_offset = key_offset + 2 + key_len as u16 + 8;
        let value_len = u16::from_le_bytes([
            self.block.data[value_offset as usize],
            self.block.data[(value_offset + 1) as usize],
        ]) as usize;
        self.value_range = (
            (value_offset + 2) as usize,
            (value_offset + 2 + value_len as u16) as usize,
        );
        let data = self.block.data
            [(key_offset + 2) as usize..(key_offset + 2 + key_len as u16) as usize]
            .to_vec();
        if self.idx > 0 {
            // key decompression
            self.key = self.decompress_key(&data);
            // Read timestamp for non-first keys
            let ts_offset = key_offset as usize + 2 + key_len;
            if ts_offset + 8 <= self.block.data.len() {
                let mut ts_arr = [0u8; 8];
                ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                self.key.set_ts(u64::from_le_bytes(ts_arr));
            }
        } else {
            self.key = KeyVec::from_vec_with_ts(data, {
                let ts_offset = value_offset as usize - 8;
                if ts_offset + 8 <= self.block.data.len() {
                    let mut ts_arr = [0u8; 8];
                    ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                    u64::from_le_bytes(ts_arr)
                } else {
                    0
                }
            });
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let len = self.block.offsets.len();
        if len == 0 {
            self.idx = 0;
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.first_key = KeyVec::new();
            return;
        }

        // Initialize `first_key` so we can correctly decompress keys during binary search.
        if self.first_key.is_empty() {
            let key0_offset = self.block.offsets[0] as usize;
            if key0_offset + 2 <= self.block.data.len() {
                let key0_len = u16::from_le_bytes([
                    self.block.data[key0_offset],
                    self.block.data[key0_offset + 1],
                ]) as usize;
                let start = key0_offset + 2;
                let end = start.saturating_add(key0_len);
                if end <= self.block.data.len() {
                    self.first_key =
                        KeyVec::from_vec_with_ts(self.block.data[start..end].to_vec(), {
                            let ts_offset = end as usize;
                            if ts_offset + 8 <= self.block.data.len() {
                                let mut ts_arr = [0u8; 8];
                                ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                                u64::from_le_bytes(ts_arr)
                            } else {
                                0
                            }
                        });
                }
            }
        }

        let target = key.to_key_vec();
        // lower_bound: find the first index i such that key[i] >= target.
        let mut low: usize = 0;
        let mut high: usize = len; // exclusive
        while low < high {
            let mid = low + (high - low) / 2;
            let key_offset = self.block.offsets[mid] as usize;
            let key_len =
                u16::from_le_bytes([self.block.data[key_offset], self.block.data[key_offset + 1]])
                    as usize;
            let mid_key_bytes = self.block.data[key_offset + 2..key_offset + 2 + key_len].to_vec();
            let mid_key = if mid > 0 {
                let mut k = self.decompress_key(&mid_key_bytes);
                let ts_offset = key_offset + 2 + key_len;
                if ts_offset + 8 <= self.block.data.len() {
                    let mut ts_arr = [0u8; 8];
                    ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                    k.set_ts(u64::from_le_bytes(ts_arr));
                }
                k
            } else {
                KeyVec::from_vec_with_ts(mid_key_bytes, {
                    let ts_offset = key_offset + 2 + key_len as usize;
                    if ts_offset + 8 <= self.block.data.len() {
                        let mut ts_arr = [0u8; 8];
                        ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                        u64::from_le_bytes(ts_arr)
                    } else {
                        0
                    }
                })
            };

            match mid_key.cmp(&target) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater | Ordering::Equal => high = mid,
            }
        }

        self.idx = low;
        if self.idx >= len {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        // Refresh iterator state to the entry at `self.idx`.
        let key_offset = self.block.offsets[self.idx] as usize;
        let key_len =
            u16::from_le_bytes([self.block.data[key_offset], self.block.data[key_offset + 1]])
                as usize;
        let data = self.block.data[key_offset + 2..key_offset + 2 + key_len].to_vec();
        if self.idx > 0 {
            self.key = self.decompress_key(&data);
            let ts_offset = key_offset + 2 + key_len;
            if ts_offset + 8 <= self.block.data.len() {
                let mut ts_arr = [0u8; 8];
                ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                self.key.set_ts(u64::from_le_bytes(ts_arr));
            }
        } else {
            self.key = KeyVec::from_vec_with_ts(data, {
                let ts_offset = key_offset + 2 + key_len as usize;
                if ts_offset + 8 <= self.block.data.len() {
                    let mut ts_arr = [0u8; 8];
                    ts_arr.copy_from_slice(&self.block.data[ts_offset..ts_offset + 8]);
                    u64::from_le_bytes(ts_arr)
                } else {
                    0
                }
            });
        }

        // Skip ts (8 bytes) after key bytes.
        let value_offset = key_offset + 2 + key_len + 8;
        let value_len = u16::from_le_bytes([
            self.block.data[value_offset],
            self.block.data[value_offset + 1],
        ]) as usize;
        self.value_range = (value_offset + 2, value_offset + 2 + value_len);
    }
}
