use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;

use bloom::BloomFilter;

use crate::config::Config;
use crate::sparse_index::SparseIndex;
use crate::util::Assignment;

pub struct Segment {
    pub file: File,
    pub path: PathBuf,
    bloom_filter: BloomFilter,
    sparse_index: SparseIndex,
}

impl Segment {
    pub fn new(mut file: File, path: PathBuf, config: &Config) -> Self {
        file.seek(SeekFrom::Start(0)).unwrap();
        let line_count = BufReader::new(&file).lines().count();

        let mut bloom_filter =
            BloomFilter::with_rate(config.bloom_filter_false_positive_rate, line_count as u32);

        let mut sparse_index = SparseIndex::new();
        let mut elapsed_bytes = 0;

        file.seek(SeekFrom::Start(0)).unwrap();
        for (i, line) in BufReader::new(&file).lines().enumerate() {
            if let Ok(line) = line {
                if let Ok(Assignment { key: k, .. }) = Assignment::try_from(line.as_str()) {
                    bloom_filter.insert(&k);
                    if i % config.sparse_index_range_size == 0 {
                        sparse_index.insert(&k, elapsed_bytes);
                    }
                }
                elapsed_bytes += line.as_bytes().len() as u64 + 1;
            }
        }

        Self {
            file,
            path,
            bloom_filter,
            sparse_index,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if !self.bloom_filter.contains(&key) {
            return None;
        }

        let range = self.sparse_index.get_byte_range(&key.into());
        let start = range.start.unwrap_or(0);
        self.file.seek(SeekFrom::Start(start)).unwrap();

        let mut elapsed_bytes = start;

        for line in BufReader::new(&self.file).lines() {
            if range.end.is_some() && elapsed_bytes >= range.end.unwrap() {
                break;
            }
            if let Ok(line) = line {
                if let Ok(Assignment { key: k, value: v }) = Assignment::try_from(line.as_str()) {
                    if k == key {
                        return Some(v.to_owned());
                    }
                }
                elapsed_bytes += line.as_bytes().len() as u64 + 1;
            }
        }

        None
    }
}
