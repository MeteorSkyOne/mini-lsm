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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, SsTable, bloom::Bloom};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    fn flush_current_block(&mut self) {
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let offset = self.data.len();
        let first_key = old_builder.first_key().raw_ref().to_vec();
        let last_key = self.last_key.clone();
        let block_data = old_builder.build().encode();
        let checksum = crc32fast::hash(&block_data);
        self.data.extend_from_slice(&block_data);
        self.data.put_u32(checksum);
        self.meta.push(BlockMeta {
            offset,
            first_key: KeyVec::from_vec(first_key).into_key_bytes(),
            last_key: KeyVec::from_vec(last_key).into_key_bytes(),
        });
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }

        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        if self.builder.add(key, value) {
            // successfully added to the current block
            self.last_key = key.raw_ref().to_vec();
        } else {
            // current block is full, need to flush the current block and create a new one
            self.flush_current_block();
            self.builder = BlockBuilder::new(self.block_size);
            if !self.builder.add(key, value) {
                panic!("split block failed: single entry too large");
            };
            self.last_key = key.raw_ref().to_vec();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    /// metablock layout:
    // | meta block data | meta checksum | meta block offset | bloom filter | bloom checksum | bloom filter offset |
    // |    varlen       |     u32       |        u32        |    varlen    |      u32       |        u32          |
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.flush_current_block();
        }

        if self.data.len() > u32::MAX as usize {
            return Err(anyhow::anyhow!(
                "sstable too large: data section {} bytes",
                self.data.len()
            ));
        }
        let mut meta_buf = Vec::new();
        BlockMeta::encode_block_meta(&self.meta, &mut meta_buf);
        let meta_checksum = crc32fast::hash(&meta_buf);
        let block_meta_offset = self.data.len() as u32;
        self.data.extend_from_slice(&meta_buf);
        self.data.put_u32(meta_checksum);
        self.data
            .extend_from_slice(&block_meta_offset.to_le_bytes());

        // Build bloom filter from all keys in this SSTable.
        let mut bloom = None;
        let bloom_offset = self.data.len() as u32;
        let mut bloom_buf = Vec::new();
        if !self.key_hashes.is_empty() {
            let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
            let built_bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
            built_bloom.encode(&mut bloom_buf);
            bloom = Some(built_bloom);
        }
        self.data.extend_from_slice(&bloom_buf);
        let bloom_checksum = crc32fast::hash(&bloom_buf);
        self.data.put_u32(bloom_checksum);
        self.data.extend_from_slice(&bloom_offset.to_le_bytes());

        if self.data.len() > u32::MAX as usize {
            return Err(anyhow::anyhow!(
                "sstable too large: total size {} bytes",
                self.data.len()
            ));
        }

        let file = super::FileObject::create(path.as_ref(), self.data)?;

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key: KeyVec::from_vec(self.first_key).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key).into_key_bytes(),
            bloom,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
