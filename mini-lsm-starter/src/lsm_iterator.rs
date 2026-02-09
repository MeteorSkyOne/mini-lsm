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

use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    SstConcatIterator,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    is_valid: bool,
    end_bound: Bound<Bytes>,
    reach_bound: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
            reach_bound: false,
        };
        iter.skip_tombstones()?;
        // Ensure the initial position respects `end_bound` as well, so callers observing the
        // iterator without calling `next()` (e.g., checking `is_valid()` immediately) still see an
        // empty iterator when the range is known to be out of bound.
        iter.check_end_bound();
        Ok(iter)
    }

    fn advance_inner(&mut self) -> Result<()> {
        self.check_end_bound();
        if self.reach_bound {
            return Ok(());
        }

        self.inner.next()?;
        self.is_valid = self.inner.is_valid();
        // `inner.next()` may move the iterator past `end_bound`. Check again so that the iterator
        // becomes invalid immediately after advancing beyond the bound.
        self.check_end_bound();
        Ok(())
    }

    /// Mark iterator as invalid if current key is out of `end_bound`.
    fn check_end_bound(&mut self) {
        if self.reach_bound {
            return;
        }
        if !self.inner.is_valid() {
            return;
        }

        match &self.end_bound {
            Bound::Unbounded => {}
            Bound::Included(end) => {
                if self.inner.key().raw_ref() > end.as_ref() {
                    self.reach_bound = true;
                }
            }
            Bound::Excluded(end) => {
                if self.inner.key().raw_ref() >= end.as_ref() {
                    self.reach_bound = true;
                }
            }
        }
    }

    /// Skip entries whose value is empty (tombstones).
    fn skip_tombstones(&mut self) -> Result<()> {
        while !self.reach_bound && self.is_valid && self.inner.value().is_empty() {
            self.advance_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        !self.reach_bound && self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.reach_bound || !self.is_valid {
            return Ok(());
        }
        self.advance_inner()?;
        self.skip_tombstones()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            Err(anyhow::anyhow!("Iterator has already errored"))
        } else {
            match self.iter.next() {
                Ok(()) => Ok(()),
                Err(e) => {
                    self.has_errored = true;
                    Err(e)
                }
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
