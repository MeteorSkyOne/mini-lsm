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
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            if rbuf.remaining() < 2 {
                bail!("truncated wal record (key_len)");
            }

            // Save the slice starting at the record boundary so we can checksum it
            // without reconstructing the record into a temporary Vec.
            let before = rbuf;

            let key_len = rbuf.get_u16() as usize;
            if rbuf.remaining() < key_len + 2 {
                bail!("truncated wal record (key)");
            }
            let key_data = Bytes::copy_from_slice(&rbuf[..key_len]);
            let key_ts = rbuf.get_u64();
            let key = KeyBytes::from_bytes_with_ts(key_data, key_ts);
            rbuf.advance(key_len);

            if rbuf.remaining() < 2 {
                bail!("truncated wal record (value_len)");
            }
            let value_len = rbuf.get_u16() as usize;
            if rbuf.remaining() < value_len + 4 {
                bail!("truncated wal record (value/checksum)");
            }
            let value = Bytes::copy_from_slice(&rbuf[..value_len]);
            rbuf.advance(value_len);

            let record_len = before.len() - rbuf.len(); // excludes checksum
            let checksum = rbuf.get_u32();
            if checksum != crc32fast::hash(&before[..record_len]) {
                bail!("checksum mismatch");
            }
            _skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        for (key, value) in _data {
            let key_len_u16 = match u16::try_from(key.key_len()) {
                Ok(v) => v,
                Err(_) => bail!("key too large"),
            };
            let value_len_u16 = match u16::try_from(value.len()) {
                Ok(v) => v,
                Err(_) => bail!("value too large"),
            };

            let start = buf.len();
            buf.put_u16(key_len_u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value_len_u16);
            buf.put_slice(value);
            let checksum = crc32fast::hash(&buf[start..]);
            buf.put_u32(checksum);
        }
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
