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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num = self.offsets.len() as u16;
        let mut encoded = self.data.clone();
        encoded.extend(self.offsets.iter().flat_map(|offset| offset.to_le_bytes()));
        encoded.extend(num.to_le_bytes());
        Bytes::from(encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let mut buf = &data[data_len - 2..];
        let num = buf.get_u16_le() as usize;
        let offsets_start = data_len - 2 - num * 2;
        let mut offsets = Vec::with_capacity(num);
        let mut offset_buf = &data[offsets_start..data_len - 2];
        for _ in 0..num {
            offsets.push(offset_buf.get_u16_le());
        }
        let block_data = data[..offsets_start].to_vec();
        Block {
            data: block_data,
            offsets,
        }
    }
}
