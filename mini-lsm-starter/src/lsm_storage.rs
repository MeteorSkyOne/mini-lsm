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

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mem_table::map_bound;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // Stop background threads first.
        //
        // Ignore send errors here: if the receiver side is already dropped (thread already exited),
        // close should still succeed.
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        // Wait for background threads to exit. `take()` makes `close()` idempotent.
        if let Some(handle) = self.compaction_thread.lock().take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("compaction thread panicked"))?;
        }
        if let Some(handle) = self.flush_thread.lock().take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("flush thread panicked"))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(),
                )))?;
        }
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        // `path` is treated as a directory throughout the codebase (e.g. `path/00001.sst`).
        // Ensure it exists before creating any SST/WAL/manifest files.
        if path.exists() {
            if !path.is_dir() {
                return Err(anyhow::anyhow!(
                    "storage path must be a directory, but got a non-directory path: {}",
                    path.display()
                ));
            }
        } else {
            std::fs::create_dir_all(path)?;
        }
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest_path = path.join("MANIFEST");
        let manifest;
        let mut next_sst_id = 1;
        if !manifest_path.exists() {
            manifest = Manifest::create(manifest_path)?;
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            // recover manifest
            let (m, records) = Manifest::recover(manifest_path)?;
            manifest = m;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(memtable_id) => {
                        memtables.insert(memtable_id);
                        next_sst_id = next_sst_id.max(memtable_id);
                    }
                    ManifestRecord::Compaction(task, sst_ids) => {
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &sst_ids, true);
                        state = new_state;
                        next_sst_id = next_sst_id.max(sst_ids.iter().max().copied().unwrap_or(0));
                    }
                }
            }

            next_sst_id += 1;

            if options.enable_wal {
                for memtable_id in memtables {
                    let memtable = MemTable::recover_from_wal(
                        memtable_id,
                        Self::path_of_wal_static(path, memtable_id),
                    )?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;

            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files.iter()))
            {
                let sst_path = Self::path_of_sst_static(path, *sst_id);
                if sst_path.exists() {
                    let sst = SsTable::open(
                        *sst_id,
                        Some(block_cache.clone()),
                        FileObject::open(&sst_path)?,
                    )?;
                    state.sstables.insert(*sst_id, Arc::new(sst));
                } else {
                    return Err(anyhow::anyhow!(
                        "SST file not found: {}",
                        sst_path.display()
                    ));
                }
            }

            // Sort SSTs on each level (only for leveled compaction)
            if let CompactionController::Leveled(_) = &compaction_controller {
                for (_id, ssts) in &mut state.levels {
                    ssts.sort_by(|x, y| {
                        state
                            .sstables
                            .get(x)
                            .unwrap()
                            .first_key()
                            .cmp(state.sstables.get(y).unwrap().first_key())
                    })
                }
            }
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let v = snapshot.memtable.get(_key);
        if let Some(v) = v {
            return if !v.is_empty() { Ok(Some(v)) } else { Ok(None) };
        }
        for imm_memtable in &snapshot.imm_memtables {
            let v = imm_memtable.get(_key);
            if let Some(v) = v {
                return if !v.is_empty() { Ok(Some(v)) } else { Ok(None) };
            }
        }

        // L0 SSTs (latest to earliest).
        if self.compaction_controller.flush_to_l0() {
            for id in &snapshot.l0_sstables {
                let Some(sst) = snapshot.sstables.get(id) else {
                    continue;
                };
                // Fast range check based on SST metadata.
                if sst.first_key().raw_ref() > _key || sst.last_key().raw_ref() < _key {
                    continue;
                }

                if let Some(bloom) = &sst.bloom
                    && !bloom.may_contain(farmhash::fingerprint32(_key))
                {
                    continue;
                }
                let iter = SsTableIterator::create_and_seek_to_key(
                    Arc::clone(sst),
                    KeySlice::from_slice(_key),
                )?;
                if iter.is_valid() && iter.key().raw_ref() == _key {
                    return if !iter.value().is_empty() {
                        Ok(Some(Bytes::copy_from_slice(iter.value())))
                    } else {
                        Ok(None)
                    };
                }
            }
        }

        // Leveled SSTs (L1..Lmax), newer levels first.
        for (_level, files) in &snapshot.levels {
            for id in files {
                let Some(sst) = snapshot.sstables.get(id) else {
                    continue;
                };
                if sst.first_key().raw_ref() > _key || sst.last_key().raw_ref() < _key {
                    continue;
                }
                let iter = SsTableIterator::create_and_seek_to_key(
                    Arc::clone(sst),
                    KeySlice::from_slice(_key),
                )?;
                if iter.is_valid() && iter.key().raw_ref() == _key {
                    return if !iter.value().is_empty() {
                        Ok(Some(Bytes::copy_from_slice(iter.value())))
                    } else {
                        Ok(None)
                    };
                }
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        self.state.read().memtable.put(_key, _value)?;
        if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
            let lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.state.read().memtable.put(_key, b"")?;
        if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
            let lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        // Best-effort directory fsync for durability (POSIX).
        // On Linux, directories can be opened and `sync_all()` will flush metadata (e.g., new files).
        std::fs::File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                new_memtable_id,
                self.path_of_wal(new_memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(new_memtable_id))
        };
        self.freeze_memtable_with_memtable(new_memtable)?;
        if self.options.enable_wal && self.manifest.is_some() {
            self.manifest.as_ref().unwrap().add_record(
                &_state_lock_observer,
                ManifestRecord::NewMemtable(new_memtable_id),
            )?;
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let memtable_to_flush = snapshot.imm_memtables.last();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush
            .ok_or(anyhow::anyhow!("no imm memtables!"))?
            .flush(&mut sst_builder)?;
        let sst_id = memtable_to_flush.unwrap().id();
        let sst_path = self.path_of_sst(sst_id);
        let sst = sst_builder.build(sst_id, Some(self.block_cache.clone()), sst_path)?;

        let mut state = self.state.write();
        let mut new_state = (**state).clone();
        new_state.imm_memtables.pop();
        if !self.compaction_controller.flush_to_l0() {
            new_state.levels.insert(0, (sst_id, vec![sst_id]));
        } else {
            new_state.l0_sstables.insert(0, sst_id);
        }
        new_state.sstables.insert(sst_id, Arc::new(sst));
        *state = Arc::new(new_state);

        // Only persist manifest records (and the corresponding directory entry metadata) when a
        // manifest is enabled.
        if let Some(manifest) = self.manifest.as_ref() {
            // Make sure the SST file creation is durable before writing the manifest record that
            // references it (POSIX crash consistency).
            self.sync_dir()?;
            manifest.add_record(&state_lock, ManifestRecord::Flush(sst_id))?;
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn sstable_overlaps_bounds(
        sst_first: &[u8],
        sst_last: &[u8],
        lower: &Bound<&[u8]>,
        upper: &Bound<&[u8]>,
    ) -> bool {
        // If scan lower bound is after the SST's last key, it cannot overlap.
        let no_overlap_lower = match lower {
            Bound::Unbounded => false,
            Bound::Included(k) => *k > sst_last,
            Bound::Excluded(k) => *k >= sst_last,
        };
        if no_overlap_lower {
            return false;
        }

        // If scan upper bound is before the SST's first key, it cannot overlap.
        let no_overlap_upper = match upper {
            Bound::Unbounded => false,
            Bound::Included(k) => *k < sst_first,
            Bound::Excluded(k) => *k <= sst_first,
        };
        !no_overlap_upper
    }

    fn create_sstable_iterator_for_scan(
        sst: Arc<SsTable>,
        lower: &Bound<&[u8]>,
    ) -> Result<SsTableIterator> {
        let iter = match lower {
            Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
            Bound::Included(k) => {
                SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(k))?
            }
            Bound::Excluded(k) => {
                let mut it = SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(k))?;
                if it.is_valid() && it.key().raw_ref() == *k {
                    it.next()?;
                }
                it
            }
        };
        Ok(iter)
    }

    fn build_l0_sstable_iters_for_scan(
        snapshot: &LsmStorageState,
        lower: &Bound<&[u8]>,
        upper: &Bound<&[u8]>,
    ) -> Result<Vec<Box<SsTableIterator>>> {
        let mut iters = Vec::new();
        for id in snapshot.l0_sstables.iter() {
            let sst = snapshot
                .sstables
                .get(id)
                .ok_or(anyhow::anyhow!("SST not found"))?;

            if !Self::sstable_overlaps_bounds(
                sst.first_key().raw_ref(),
                sst.last_key().raw_ref(),
                lower,
                upper,
            ) {
                continue;
            }

            let iter = Self::create_sstable_iterator_for_scan(Arc::clone(sst), lower)?;
            iters.push(Box::new(iter));
        }
        Ok(iters)
    }

    fn build_leveled_sstable_iters_for_scan(
        snapshot: &LsmStorageState,
        lower: &Bound<&[u8]>,
        upper: &Bound<&[u8]>,
    ) -> Result<Vec<Box<SstConcatIterator>>> {
        let mut iters = Vec::new();

        // L1..Lmax: within each level tables are non-overlapping, so each level can be represented
        // by a concat iterator. Across levels tables may overlap, so we merge these concat
        // iterators with precedence from smaller level number to larger.
        for (_level, files) in &snapshot.levels {
            let mut sstables = Vec::new();
            for id in files.iter() {
                let sst = snapshot
                    .sstables
                    .get(id)
                    .ok_or(anyhow::anyhow!("SST not found"))?;
                if !Self::sstable_overlaps_bounds(
                    sst.first_key().raw_ref(),
                    sst.last_key().raw_ref(),
                    lower,
                    upper,
                ) {
                    continue;
                }
                sstables.push(Arc::clone(sst));
            }

            if sstables.is_empty() {
                continue;
            }

            let iter = match lower {
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables)?,
                Bound::Included(k) => {
                    SstConcatIterator::create_and_seek_to_key(sstables, KeySlice::from_slice(k))?
                }
                Bound::Excluded(k) => {
                    let mut it = SstConcatIterator::create_and_seek_to_key(
                        sstables,
                        KeySlice::from_slice(k),
                    )?;
                    if it.is_valid() && it.key().raw_ref() == *k {
                        it.next()?;
                    }
                    it
                }
            };

            // `MergeIterator::create` will filter invalid iterators, but skipping them here avoids
            // building a heap wrapper unnecessarily.
            if iter.is_valid() {
                iters.push(Box::new(iter));
            }
        }

        Ok(iters)
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // Newest memtable first, then immutable memtables from latest to earliest.
        let mut iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));
        for mem in snapshot.imm_memtables.iter() {
            iters.push(Box::new(mem.scan(_lower, _upper)));
        }
        let mem_iter = MergeIterator::create(iters);

        // SST range filter: only create iterators for SSTs whose key ranges overlap with the scan
        // bounds. This is required so that `num_active_iterators()` reflects the effective fan-in
        // for scans that are known-empty from table metadata (week 1 day 6 task 3).
        let sst_iters = Self::build_l0_sstable_iters_for_scan(&snapshot, &_lower, &_upper)?;
        let sst_iter = MergeIterator::create(sst_iters);

        // L1..Lmax: each level uses concat (non-overlapping), then merge across levels.
        let leveled_iters =
            Self::build_leveled_sstable_iters_for_scan(&snapshot, &_lower, &_upper)?;
        let leveled_iter = MergeIterator::create(leveled_iters);

        let mem_and_l0 = TwoMergeIterator::create(mem_iter, sst_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(mem_and_l0, leveled_iter)?,
            map_bound(_upper),
        )?))
    }
}
