use std::cmp;
use std::fs::{self, File, OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::segment::Segment;
use crate::util::Assignment;

pub struct CompactionParams {
    pub interval_seconds: u64,
    pub path: PathBuf,
    pub segments: Arc<Mutex<Vec<Segment>>>,
    pub compaction_kill_flag: Arc<AtomicBool>,
}

pub fn compaction_loop(
    CompactionParams { interval_seconds, path, segments, compaction_kill_flag }: CompactionParams,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut last_compaction = Instant::now();
        while !compaction_kill_flag.load(Ordering::Relaxed) {
            if last_compaction.elapsed().as_secs() >= interval_seconds {
                // TODO: It's really bad that we lock the segment files here. This will make our
                // database completely unavailable to incoming read and write requests while a
                // compaction is taking place. Which is completely unnecessary since the
                // compaction is not modifying the existing segment files.
                let mut segments = segments.lock().unwrap();

                if segments.len() >= 2 {
                    log::debug!("starting compaction");
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
                    log::debug!("compaction finished");
                } else {
                    log::debug!("compaction loop ticked, but there was nothing to do");
                }
                last_compaction = Instant::now();
            }
            thread::sleep(Duration::from_secs(1));
        }
    })
}

fn do_compaction(first: &mut File, second: &mut File, path: PathBuf) -> Segment {
    let mut new_segment_file =
        OpenOptions::new().create_new(true).write(true).read(true).open(&path).unwrap();

    first.seek(SeekFrom::Start(0)).unwrap();
    second.seek(SeekFrom::Start(0)).unwrap();
    log::trace!("reset segment file offsets");

    let mut first_iter = BufReader::new(first).lines().into_iter().peekable();
    let mut second_iter = BufReader::new(second).lines().into_iter().peekable();

    while first_iter.peek().is_some() && second_iter.peek().is_some() {
        let first_line: String = first_iter.peek().unwrap().as_ref().unwrap().into();
        let second_line: String = second_iter.peek().unwrap().as_ref().unwrap().into();

        let first_assignment = Assignment::parse(first_line.as_str()).unwrap();
        let second_assignment = Assignment::parse(second_line.as_str()).unwrap();
        log::trace!("stitching {first_line} vs {second_line}");

        match first_assignment.key.cmp(&second_assignment.key) {
            cmp::Ordering::Less => {
                log::trace!("left ({first_line}) -> segment file");
                new_segment_file.write(first_line.as_bytes()).unwrap();
                first_iter.next();
            },
            cmp::Ordering::Greater => {
                log::trace!("right ({second_line}) -> segment file");
                new_segment_file.write(second_line.as_bytes()).unwrap();
                second_iter.next();
            },
            cmp::Ordering::Equal => {
                log::trace!("equivalent keys; deduplicating and writing to segment file");
                new_segment_file.write(second_line.as_bytes()).unwrap();
                first_iter.next();
                second_iter.next();
            },
        };

        new_segment_file.write("\n".as_bytes()).unwrap();
    }

    while let Some(Ok(line)) = first_iter.next() {
        log::trace!("left ({line}) -> segment file");
        new_segment_file.write(format!("{}\n", line).as_bytes()).unwrap();
    }

    while let Some(Ok(line)) = second_iter.next() {
        log::trace!("right ({line}) -> segment file");
        new_segment_file.write(format!("{}\n", line).as_bytes()).unwrap();
    }

    Segment::new(new_segment_file, path)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::StoreFixture;

    #[test]
    fn compaction() {
        env_logger::init();
        let mut fixture = StoreFixture::init("./test-db-compaction");
        let mut file1 = fixture.create_segment_file("a=1\nc=3\ne=5");
        let mut file2 = fixture.create_segment_file("b=2\nd=4\nf=6");
        let mut segment = do_compaction(&mut file1, &mut file2, fixture.allocate_segment_file());
        assert_eq!(segment.read_to_string(), "a=1\nb=2\nc=3\nd=4\ne=5\nf=6\n");
    }
}
