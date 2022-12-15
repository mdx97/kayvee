use rbtree::RBTree;
use walkdir::WalkDir;

use crate::compaction::compactor;
use crate::config::Config;
use crate::segment::Segment;

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

pub struct Database {
    path: PathBuf,
    config: Config,
    segments: Arc<Mutex<Vec<Segment>>>,
    memtable: RBTree<String, String>,
    compaction_kill_flag: Arc<AtomicBool>,
    compaction_join_handle: Option<JoinHandle<()>>,
}

impl Database {
    pub fn new(path: PathBuf, config: Config) -> Self {
        // Initialize on-disk representation of the database by creating the
        // directory if it doesn't exist, or reading the segment files in the
        // given directory if it does exist.
        let segments = Arc::new(Mutex::new(if !path.exists() {
            fs::create_dir_all(path.clone()).unwrap();
            Vec::new()
        } else {
            WalkDir::new(path.clone())
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter_map(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("segment")
                        .then(|| {
                            let file = File::open(entry.path()).unwrap();
                            Segment::new(file, PathBuf::from(entry.path()), &config)
                        })
                })
                .collect()
        }));

        // Start a new thread which will handle compacting segment files in
        // the background.
        let compaction_kill_flag = Arc::new(AtomicBool::new(false));
        let compaction_join_handle = Some(thread::spawn({
            let path = path.clone();
            let config = config.clone();
            let segments = segments.clone();
            let flag = compaction_kill_flag.clone();
            move || compactor(&path, &config, segments, flag)
        }));

        Self {
            path,
            config,
            segments,
            memtable: RBTree::new(),
            compaction_kill_flag,
            compaction_join_handle,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.memtable.replace_or_insert(key.into(), value.into());

        // If the memtable has filled to capacity, we "flush" it and write its
        // contents to a new segment file.
        if self.memtable.len() >= self.config.memtable_capacity {
            let mut files = self.segments.lock().unwrap();
            let path = self.path.clone().join(
                // TODO: This should be based on the segment file with the highest number + 1, not the length.
                // This is because we compact files now so segment_files.len() won't always be equal to the highest
                // numbered segment file.
                format!("segment-{}.dat", files.len() + 1),
            );
            let mut file = File::create(path.clone()).unwrap();

            for (key, value) in self.memtable.iter() {
                file.write_all(format!("{}={}\n", key, value).as_bytes())
                    .unwrap();
            }

            files.push(Segment::new(
                File::open(path.clone()).unwrap(),
                path,
                &self.config,
            ));

            self.memtable = RBTree::new();
        }
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        // First, check to see if the key exists in the memtable.
        if let Some(value) = self.memtable.get(&key.into()) {
            return Some(value.to_owned());
        }

        // If it doesn't, check our segment files for the key.
        let mut segments = self.segments.lock().unwrap();
        for segment in segments.iter_mut().rev() {
            if let Some(value) = segment.get(key) {
                return Some(value);
            }
        }

        None
    }

    pub fn delete(&mut self, key: &str) {
        self.memtable.remove(&key.into());
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.compaction_kill_flag.swap(true, Ordering::Relaxed);
        self.compaction_join_handle
            .take()
            .map(|h| h.join().expect("failed to join on compaction_join_handle"));
    }
}
