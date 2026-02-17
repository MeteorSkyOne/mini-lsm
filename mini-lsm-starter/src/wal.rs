// REMOVE THIS LINE after fully implementing this functionality
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

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        const U16_SIZE: usize = std::mem::size_of::<u16>();
        const U32_SIZE: usize = std::mem::size_of::<u32>();
        const U64_SIZE: usize = std::mem::size_of::<u64>();

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            if rbuf.remaining() < U32_SIZE {
                bail!("truncated WAL batch header");
            }
            let batch_size = rbuf.get_u32() as usize;
            let batch_and_checksum_len = match batch_size.checked_add(U32_SIZE) {
                Some(v) => v,
                None => bail!("invalid WAL batch size"),
            };
            if rbuf.remaining() < batch_and_checksum_len {
                bail!("truncated WAL batch");
            }

            let batch = &rbuf[..batch_size];
            rbuf.advance(batch_size);
            let expected_checksum = rbuf.get_u32();
            let actual_checksum = crc32fast::hash(batch);
            if expected_checksum != actual_checksum {
                bail!("checksum mismatch");
            }

            let mut batch_buf = batch;
            while batch_buf.has_remaining() {
                if batch_buf.remaining() < U16_SIZE {
                    bail!("truncated wal record (key_len)");
                }
                let key_len = batch_buf.get_u16() as usize;
                let key_and_meta_len = match key_len.checked_add(U64_SIZE + U16_SIZE) {
                    Some(v) => v,
                    None => bail!("invalid wal key length"),
                };
                if batch_buf.remaining() < key_and_meta_len {
                    bail!("truncated wal record (key/ts/value_len)");
                }

                let key_data = Bytes::copy_from_slice(&batch_buf[..key_len]);
                batch_buf.advance(key_len);
                let key_ts = batch_buf.get_u64();

                let value_len = batch_buf.get_u16() as usize;
                if batch_buf.remaining() < value_len {
                    bail!("truncated wal record (value)");
                }
                let value = Bytes::copy_from_slice(&batch_buf[..value_len]);
                batch_buf.advance(value_len);

                skiplist.insert(KeyBytes::from_bytes_with_ts(key_data, key_ts), value);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        for (key, value) in data {
            let key_len_u16 = match u16::try_from(key.key_len()) {
                Ok(v) => v,
                Err(_) => bail!("key too large"),
            };
            let value_len_u16 = match u16::try_from(value.len()) {
                Ok(v) => v,
                Err(_) => bail!("value too large"),
            };

            buf.put_u16(key_len_u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value_len_u16);
            buf.put_slice(value);
        }
        let batch_size = match u32::try_from(buf.len()) {
            Ok(v) => v,
            Err(_) => bail!("batch too large"),
        };
        let checksum = crc32fast::hash(&buf);
        file.write_all(&batch_size.to_be_bytes())?;
        file.write_all(&buf)?;
        file.write_all(&checksum.to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
