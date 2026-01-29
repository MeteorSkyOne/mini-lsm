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

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // Filter out invalid iterators to avoid calling `key()` on them when maintaining the heap.
        let mut iters = iters
            .into_iter()
            .enumerate()
            .filter_map(|(i, iter)| {
                if iter.is_valid() {
                    Some(HeapWrapper(i, iter))
                } else {
                    None
                }
            })
            .collect::<BinaryHeap<_>>();
        let current = iters.pop().map(|wrapper| HeapWrapper(wrapper.0, wrapper.1));
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&'_ self) -> KeySlice<'_> {
        self.current
            .as_ref()
            .map_or(KeySlice::from_slice(&[]), |current| current.1.key())
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map_or(&[], |current| current.1.value())
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map_or(false, |current| current.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let Some(mut current) = self.current.take() else {
            return Ok(());
        };

        // If `current` is already invalid, just select the next valid iterator from the heap.
        if !current.1.is_valid() {
            self.current = self.iters.pop();
            return Ok(());
        }

        // Own the key because advancing iterators may invalidate borrowed slices.
        let current_key = current.1.key().to_key_vec();
        let current_key_slice = current_key.as_key_slice();

        // Advance and reinsert all iterators whose key equals current key (they should be skipped).
        loop {
            let should_advance = match self.iters.peek() {
                Some(top) => top.1.key() == current_key_slice,
                None => false,
            };
            if !should_advance {
                break;
            }

            let mut dup = self.iters.pop().expect("peek() returned Some");
            dup.1.next()?;
            if dup.1.is_valid() {
                self.iters.push(dup);
            }
        }

        // Advance current iterator itself and reinsert if still valid.
        current.1.next()?;
        if current.1.is_valid() {
            self.iters.push(current);
        }

        // Select the new current iterator.
        self.current = self.iters.pop();
        Ok(())
    }
}
