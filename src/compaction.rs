use std::{
    cmp,
    fs::{self, File},
    io::{prelude::*, BufReader, SeekFrom},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{segment::Segment, util::parse_assignment};

pub struct CompactionParams {
    pub interval_seconds: u64,
    pub path: PathBuf,
    pub segments: Arc<Mutex<Vec<Segment>>>,
    pub compaction_kill_flag: Arc<AtomicBool>,
}

pub fn compaction_loop(
    CompactionParams {
        interval_seconds,
        path,
        segments,
        compaction_kill_flag,
    }: CompactionParams,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut last_compaction = Instant::now();
        while !compaction_kill_flag.load(Ordering::Relaxed) {
            if last_compaction.elapsed().as_secs() >= interval_seconds {
                let mut segments = segments.lock().unwrap();
                if segments.len() >= 2 {
                    let new_segment_file;
                    let new_segment_path = path.clone().join("new-segment.dat");
                    {
                        let (a, b) = segments.split_at_mut(1);
                        let first = &mut a[0];
                        let second = &mut b[0];

                        new_segment_file = Some(do_compaction(
                            &mut first.file,
                            &mut second.file,
                            new_segment_path.clone(),
                        ));
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
    })
}

fn do_compaction(first: &mut File, second: &mut File, path: PathBuf) -> Segment {
    let mut new_segment_file = File::create(path.clone()).unwrap();

    first.seek(SeekFrom::Start(0)).unwrap();
    second.seek(SeekFrom::Start(0)).unwrap();

    let mut first_iter = BufReader::new(first).lines().into_iter().peekable();
    let mut second_iter = BufReader::new(second).lines().into_iter().peekable();

    while first_iter.peek().is_some() && second_iter.peek().is_some() {
        let first_line: String = first_iter.peek().unwrap().as_ref().unwrap().into();
        let second_line: String = second_iter.peek().unwrap().as_ref().unwrap().into();

        let first_assignment = parse_assignment(first_line.as_str()).unwrap();
        let second_assignment = parse_assignment(second_line.as_str()).unwrap();

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

    Segment::new(new_segment_file, path)
}
