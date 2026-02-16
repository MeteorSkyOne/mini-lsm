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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // Encode the number of block metadata entries first so that
        // `decode_block_meta` knows how many records to read back.
        buf.put_u32(block_meta.len() as u32);

        for meta in block_meta {
            assert!(meta.offset <= u32::MAX as usize);
            // Encode offset of the block.
            buf.put_u32(meta.offset as u32);

            // Encode first key (length + bytes).
            let first_key = &meta.first_key;
            buf.put_u16(first_key.key_len() as u16);
            buf.extend_from_slice(first_key.key_ref());
            buf.put_u64(first_key.ts());

            // Encode last key (length + bytes).
            let last_key = &meta.last_key;
            buf.put_u16(last_key.key_len() as u16);
            buf.extend_from_slice(last_key.key_ref());
            buf.put_u64(last_key.ts());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: &mut impl Buf) -> Vec<BlockMeta> {
        let nums = buf.get_u32() as usize;
        let mut metas = Vec::with_capacity(nums);
        for _ in 0..nums {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let mut first_key = vec![0; first_key_len];
            buf.copy_to_slice(&mut first_key);
            let first_key_ts = buf.get_u64();
            let last_key_len = buf.get_u16() as usize;
            let mut last_key = vec![0; last_key_len];
            buf.copy_to_slice(&mut last_key);
            let last_key_ts = buf.get_u64();
            metas.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes_with_ts(Bytes::from(first_key), first_key_ts),
                last_key: KeyBytes::from_bytes_with_ts(Bytes::from(last_key), last_key_ts),
            });
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // Meta section layout:
        // | meta block data | meta checksum | meta block offset | bloom filter | bloom checksum | bloom filter offset |
        // |    varlen       |     u32       |        u32        |    varlen    |      u32       |        u32          |
        //
        // - Offsets are little-endian u32.
        // - Checksums are u32 written with `BufMut::put_u32` (big-endian).
        let file_size = file.size();
        if file_size < 20 {
            return Err(anyhow::anyhow!(
                "sstable file too small: {} bytes",
                file_size
            ));
        }

        // Footer: [bloom_checksum (4 bytes BE)] [bloom_offset (4 bytes LE)]
        let footer = file.read(file_size - 8, 8)?;
        let footer_bytes: [u8; 8] = footer
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid sstable footer length"))?;
        let bloom_checksum_bytes: [u8; 4] = footer_bytes[0..4]
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid bloom checksum encoding"))?;
        let bloom_offset_u32 = u32::from_le_bytes(
            footer_bytes[4..8]
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid bloom offset encoding"))?,
        ) as u64;
        if bloom_offset_u32 > file_size - 8 {
            bail!(
                "invalid bloom offset {} (file size {})",
                bloom_offset_u32,
                file_size
            );
        }
        let bloom_offset = bloom_offset_u32 as usize;
        if bloom_offset < 8 {
            bail!("invalid bloom offset {} (too small)", bloom_offset);
        }

        // Decode bloom filter if present and verify checksum.
        let bloom_len = file_size as usize - bloom_offset - 8;
        let bloom_buf = file.read(bloom_offset as u64, bloom_len as u64)?;
        let bloom_checksum_now = crc32fast::hash(&bloom_buf).to_be_bytes();
        if bloom_checksum_now != bloom_checksum_bytes {
            bail!("bloom checksum mismatched");
        }
        let bloom = if bloom_len > 0 {
            Some(Bloom::decode(&bloom_buf)?)
        } else {
            None
        };

        // Immediately before bloom filter data: [meta_checksum (4 bytes BE)] [meta_offset (4 bytes LE)]
        let meta_footer = file.read(bloom_offset as u64 - 8, 8)?;
        let meta_footer_bytes: [u8; 8] = meta_footer
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid meta footer length"))?;
        let meta_checksum_bytes: [u8; 4] = meta_footer_bytes[0..4]
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid meta checksum encoding"))?;
        let block_meta_offset_u32 = u32::from_le_bytes(
            meta_footer_bytes[4..8]
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid block meta offset encoding"))?,
        ) as u64;
        if block_meta_offset_u32 > (bloom_offset as u64).saturating_sub(8) {
            bail!(
                "invalid block meta offset {} (bloom offset {})",
                block_meta_offset_u32,
                bloom_offset
            );
        }
        let block_meta_offset = block_meta_offset_u32 as usize;
        let meta_len = (bloom_offset - 8)
            .checked_sub(block_meta_offset)
            .ok_or(anyhow::anyhow!("invalid meta section length"))?;
        if meta_len < 4 {
            bail!("meta section too small: {} bytes", meta_len);
        }
        let meta_buf = file.read(block_meta_offset as u64, meta_len as u64)?;
        let meta_checksum_now = crc32fast::hash(&meta_buf).to_be_bytes();
        if meta_checksum_now != meta_checksum_bytes {
            bail!("meta checksum mismatched");
        }

        let mut block_meta_buf = Bytes::from(meta_buf);
        let block_meta = BlockMeta::decode_block_meta(&mut block_meta_buf);
        let first_key = KeyBytes::from_bytes_with_ts(
            Bytes::from(
                block_meta
                    .first()
                    .ok_or(anyhow::anyhow!("empty block meta"))?
                    .first_key
                    .key_ref()
                    .to_vec(),
            ),
            block_meta
                .first()
                .ok_or(anyhow::anyhow!("empty block meta"))?
                .first_key
                .ts(),
        );
        let last_key = KeyBytes::from_bytes_with_ts(
            Bytes::from(
                block_meta
                    .last()
                    .ok_or(anyhow::anyhow!("empty block meta"))?
                    .last_key
                    .key_ref()
                    .to_vec(),
            ),
            block_meta
                .last()
                .ok_or(anyhow::anyhow!("empty block meta"))?
                .last_key
                .ts(),
        );

        Ok(Self {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let meta = &self.block_meta[block_idx];
        let data_len = if block_idx == self.block_meta.len() - 1 {
            self.block_meta_offset - meta.offset
        } else {
            self.block_meta[block_idx + 1].offset - meta.offset
        };
        let data = self.file.read(meta.offset as u64, data_len as u64 - 4)?;
        let real_checksum = self.file.read((meta.offset + data_len) as u64 - 4, 4)?;
        let now_checksum = crc32fast::hash(&data[..data_len - 4]).to_be_bytes();
        if real_checksum != now_checksum {
            bail!("block checksum mismatched");
        }
        Ok(Arc::new(Block::decode(&data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(blockcache) = self.block_cache.as_ref() {
            if let Some(block) = blockcache.get(&(self.id, block_idx)) {
                Ok(block.clone())
            } else {
                let block = self.read_block(block_idx)?;
                blockcache.insert((self.id, block_idx), block.clone());
                Ok(block)
            }
        } else {
            // block cache not enabled, read from disk directly
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        unimplemented!()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
