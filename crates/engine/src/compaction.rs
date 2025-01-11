use std::cmp;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::segment::EntryIter;

pub struct CompactionParams {
    pub interval_seconds: u64,
    pub path: PathBuf,
    pub segments: Arc<RwLock<Vec<PathBuf>>>,
    pub compaction_kill_flag: Arc<AtomicBool>,
}

pub fn compaction_loop(
    CompactionParams { interval_seconds, path, segments, compaction_kill_flag }: CompactionParams,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut last_compaction = Instant::now();
        while !compaction_kill_flag.load(Ordering::Relaxed) {
            if last_compaction.elapsed().as_secs() >= interval_seconds {
                let segments_read = segments.read().unwrap();
                if segments_read.len() >= 2 {
                    let (a, b) = segments_read.split_at(1);
                    let first = &a[0];
                    let second = &b[0];
                    log::debug!("starting compaction of {first:?} and {second:?}");
                    let new_segment_path = path.clone().join("new-segment.dat");
                    let mut first = File::open(&a[0]).unwrap();
                    let mut second = File::open(&b[0]).unwrap();
                    do_compaction(&mut first, &mut second, new_segment_path.clone());
                    drop(segments_read);

                    let mut segments_write = segments.write().unwrap();
                    fs::remove_file(&segments_write[0]).unwrap();
                    fs::remove_file(&segments_write[1]).unwrap();
                    fs::rename(&new_segment_path, &segments_write[1]).unwrap();
                    // TODO: This should be a VecDeque so we can do this in constant time.
                    segments_write.remove(0);

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

fn do_compaction(first: &mut File, second: &mut File, path: PathBuf) {
    let mut new_segment =
        OpenOptions::new().create_new(true).write(true).read(true).open(&path).unwrap();

    let mut first_iter = EntryIter::from_start(first).peekable();
    let mut second_iter = EntryIter::from_start(second).peekable();

    while let (Some(first_entry), Some(second_entry)) = (first_iter.peek(), second_iter.peek()) {
        // TODO: Don't unwrap these...
        match first_entry.key().cmp(&second_entry.key()) {
            cmp::Ordering::Less => {
                log::trace!("first ({first_entry:?}) -> {path:?}");
                first_entry.write(&mut new_segment).unwrap();
                first_iter.next();
            },
            cmp::Ordering::Greater => {
                log::trace!("second ({second_entry:?}) -> {path:?}");
                second_entry.write(&mut new_segment).unwrap();
                second_iter.next();
            },
            cmp::Ordering::Equal => {
                log::trace!("equal, dedupe ({second_entry:?}) -> {path:?}");
                second_entry.write(&mut new_segment).unwrap();
                first_iter.next();
                second_iter.next();
            },
        }
    }

    while let Some(entry) = first_iter.next() {
        log::trace!("first ({entry:?}) -> {path:?}");
        entry.write(&mut new_segment).unwrap();
    }

    while let Some(entry) = second_iter.next() {
        log::trace!("second ({entry:?}) -> {path:?}");
        entry.write(&mut new_segment).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::segment::Entry;
    use crate::test::StoreFixture;

    #[test]
    fn compaction() {
        _ = env_logger::try_init();
        let mut fixture = StoreFixture::init("./test-db-compaction");
        let mut file1 = fixture.create_segment_file([("a", "1"), ("c", "3"), ("e", "5")]);
        let mut file2 = fixture.create_segment_file([("b", "2"), ("d", "4"), ("f", "6")]);
        let mut file3 = fixture.create_segment_file([("a", "7"), ("d", "9"), ("e", "8")]);

        let new1 = fixture.allocate_segment_file();
        do_compaction(&mut file1, &mut file2, new1.clone());
        let mut new1 = File::open(new1).unwrap();

        let new2 = fixture.allocate_segment_file();
        do_compaction(&mut new1, &mut file3, new2.clone());
        let mut new2 = File::open(new2).unwrap();

        pretty_assertions::assert_eq!(
            EntryIter::new(&mut new2).collect::<Vec<_>>(),
            [("a", "7"), ("b", "2"), ("c", "3"), ("d", "9"), ("e", "8"), ("f", "6")]
                .into_iter()
                .map(|(key, value)| {
                    Entry::Assignment { key: key.to_owned(), value: value.to_owned() }
                })
                .collect::<Vec<_>>()
        );
    }
}
