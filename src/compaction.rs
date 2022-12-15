use std::cmp;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::segment::Segment;
use crate::util::Assignment;

const COMPACTOR_INTERVAL_SECONDS: u64 = 10;

pub fn compactor(
    path: PathBuf,
    segments: Arc<Mutex<Vec<Segment>>>,
    compaction_kill_flag: Arc<AtomicBool>,
) {
    let mut last_compaction = Instant::now();
    while !compaction_kill_flag.load(Ordering::Relaxed) {
        if last_compaction.elapsed().as_secs() >= COMPACTOR_INTERVAL_SECONDS {
            let mut segments = segments.lock().unwrap();
            if segments.len() >= 2 {
                let new_segment_file;
                let new_segment_path = path.clone().join("new-segment.dat");
                {
                    let (a, b) = segments.split_at_mut(1);
                    let first = &mut a[0].file;
                    let second = &mut b[0].file;

                    new_segment_file = Some({
                        let mut new_segment_file = File::create(path.clone()).unwrap();

                        first.seek(SeekFrom::Start(0)).unwrap();
                        second.seek(SeekFrom::Start(0)).unwrap();

                        let mut first_iter = BufReader::new(first).lines().into_iter().peekable();
                        let mut second_iter = BufReader::new(second).lines().into_iter().peekable();

                        while first_iter.peek().is_some() && second_iter.peek().is_some() {
                            let first_line: String =
                                first_iter.peek().unwrap().as_ref().unwrap().into();
                            let second_line: String =
                                second_iter.peek().unwrap().as_ref().unwrap().into();

                            let first_assignment: Assignment = first_line.parse().unwrap();
                            let second_assignment: Assignment = second_line.parse().unwrap();

                            match first_assignment.key.cmp(&second_assignment.key) {
                                cmp::Ordering::Less => {
                                    new_segment_file.write(first_line.as_bytes()).unwrap();
                                    first_iter.next();
                                }
                                cmp::Ordering::Greater => {
                                    new_segment_file.write(second_line.as_bytes()).unwrap();
                                    second_iter.next();
                                }
                                cmp::Ordering::Equal => {
                                    new_segment_file.write(second_line.as_bytes()).unwrap();
                                    first_iter.next();
                                    second_iter.next();
                                }
                            };

                            new_segment_file.write("\n".as_bytes()).unwrap();
                        }

                        for line in first_iter {
                            if let Ok(line) = line {
                                new_segment_file
                                    .write(format!("{}\n", line).as_bytes())
                                    .unwrap();
                            }
                        }

                        for line in second_iter {
                            if let Ok(line) = line {
                                new_segment_file
                                    .write(format!("{}\n", line).as_bytes())
                                    .unwrap();
                            }
                        }

                        Segment::new(new_segment_file, path.clone())
                    });
                }

                fs::remove_file(&segments[0].path).unwrap();
                fs::remove_file(&segments[1].path).unwrap();
                fs::rename(new_segment_path, segments[1].path.clone()).unwrap();

                segments.splice(0..2, [new_segment_file.unwrap()]);
            }
            last_compaction = Instant::now();
        }
        thread::sleep(Duration::from_secs(1));
    }
}
