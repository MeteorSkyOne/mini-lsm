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

use anyhow::Result;

use std::cmp;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    current: bool, // true if current iterator is a, false if current iterator is b
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    fn select_current(&mut self) {
        let a_valid = self.a.is_valid();
        let b_valid = self.b.is_valid();

        self.current = match (a_valid, b_valid) {
            (true, true) => self.a.key().cmp(&self.b.key()) != cmp::Ordering::Greater,
            (true, false) => true,
            (false, true) => false,
            (false, false) => self.current,
        };
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            current: true,
        };
        iter.select_current();
        Ok(iter)
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.current {
            if self.a.is_valid() {
                self.a.key()
            } else {
                self.b.key()
            }
        } else {
            if self.b.is_valid() {
                self.b.key()
            } else {
                self.a.key()
            }
        }
    }

    fn value(&self) -> &[u8] {
        if self.current {
            if self.a.is_valid() {
                self.a.value()
            } else {
                self.b.value()
            }
        } else {
            if self.b.is_valid() {
                self.b.value()
            } else {
                self.a.value()
            }
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        let a_valid = self.a.is_valid();
        let b_valid = self.b.is_valid();
        if !a_valid && !b_valid {
            return Ok(());
        }

        match (a_valid, b_valid) {
            (true, true) => {
                // Compare before advancing to avoid using borrowed keys after calling `next`.
                let ordering = self.a.key().cmp(&self.b.key());
                if ordering == cmp::Ordering::Greater {
                    // B has smaller key.
                    self.b.next()?;
                } else {
                    // A has smaller (or equal) key; when equal, prefer A but must skip B's duplicate.
                    self.a.next()?;
                    if ordering == cmp::Ordering::Equal && self.b.is_valid() {
                        self.b.next()?;
                    }
                }
            }
            (true, false) => {
                self.a.next()?;
            }
            (false, true) => {
                self.b.next()?;
            }
            (false, false) => {}
        }

        self.select_current();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        if self.current {
            self.a.num_active_iterators()
        } else {
            self.b.num_active_iterators()
        }
    }
}
