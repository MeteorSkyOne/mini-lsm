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

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len = key.raw_ref().len();
        let entry_size = 2 + key_len + 2 + value.len(); // key_len(u16) + key + value_len(u16) + value
        // footer len = 2(num_of_elements_len(u16))
        // Unless the first key-value pair exceeds the target block size, otherwise total size should be less than or equal to the target block size.
        if self.offsets.len() > 0 && self.data.len() + entry_size + 2 > self.block_size {
            return false;
        }
        if self.offsets.is_empty() {
            self.first_key = key.to_key_vec();
        }
        // append offset
        self.offsets.push(self.data.len() as u16);
        // append key_len
        self.data.extend_from_slice(&(key_len as u16).to_le_bytes());
        // append key
        self.data.extend_from_slice(key.raw_ref());
        // append value_len
        self.data
            .extend_from_slice(&(value.len() as u16).to_le_bytes());
        // append value
        self.data.extend_from_slice(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            offsets: self.offsets,
            data: self.data,
        }
    }
}
