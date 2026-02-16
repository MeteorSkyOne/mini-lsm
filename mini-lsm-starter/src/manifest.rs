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

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create manifest")?,
            )),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(_path)
            .context("failed to recover manifest")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut offset = 0usize;
        let mut records = Vec::new();
        while offset < buf.len() {
            let len_end = offset
                .checked_add(8)
                .context("manifest offset overflow while reading length")?;
            let len_bytes = buf
                .get(offset..len_end)
                .context("manifest truncated while reading length")?;
            let len = u64::from_be_bytes(
                len_bytes
                    .try_into()
                    .context("manifest length must be 8 bytes")?,
            ) as usize;
            offset = len_end;

            let record_end = offset
                .checked_add(len)
                .context("manifest offset overflow while reading record")?;
            let record_bytes = buf
                .get(offset..record_end)
                .context("manifest truncated while reading record")?;
            offset = record_end;

            let checksum_end = offset
                .checked_add(4)
                .context("manifest offset overflow while reading checksum")?;
            let checksum_bytes = buf
                .get(offset..checksum_end)
                .context("manifest truncated while reading checksum")?;
            let expected_checksum = u32::from_be_bytes(
                checksum_bytes
                    .try_into()
                    .context("manifest checksum must be 4 bytes")?,
            );
            offset = checksum_end;

            let actual_checksum = crc32fast::hash(record_bytes);
            if actual_checksum != expected_checksum {
                return Err(anyhow::anyhow!(
                    "manifest checksum mismatch: expected {expected_checksum}, got {actual_checksum}"
                ));
            }

            let record: ManifestRecord = serde_json::from_slice(record_bytes)
                .context("failed to deserialize manifest record")?;
            records.push(record);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let mut lock = self.file.lock();
        let record = serde_json::to_vec(&_record)?;
        let len = record.len() as u64;
        let checksum = crc32fast::hash(&record);
        lock.write_all(&len.to_be_bytes())?;
        lock.write_all(&record)?;
        lock.write_all(&checksum.to_be_bytes())?;
        lock.sync_all()?;
        Ok(())
    }
}
