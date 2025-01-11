use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::{cmp, thread};

use crate::segment::EntryIter;

pub fn compaction_loop(
    interval_seconds: u64,
    path: PathBuf,
    segments: Arc<RwLock<VecDeque<PathBuf>>>,
    compaction_kill_flag: Arc<AtomicBool>,
) {
    let mut last_compaction = Instant::now();
    while !compaction_kill_flag.load(Ordering::Relaxed) {
        if last_compaction.elapsed().as_secs() >= interval_seconds {
            let segments_read = segments.read().unwrap();
            if segments_read.len() >= 2 {
                let first = &segments_read[0];
                let second = &segments_read[1];
                log::debug!("starting compaction of {first:?} and {second:?}");
                let mut first = File::open(first).unwrap();
                let mut second = File::open(second).unwrap();
                let new_segment_path = path.clone().join("new-segment.dat");
                compact(&mut first, &mut second, new_segment_path.clone());

                // This explicit drop is pivotal to avoid deadlocks, otherwise the write lock
                // on the following line can not be acquired.
                drop(segments_read);

                // This separate swaperoo step is so that we only need to hold a *read* lock on
                // the segment buffer when doing the compaction, and those files can continue to
                // service read requests on the engine thread.
                //
                // TODO: Don't need to acquire a write lock over the whole buffer for this
                // section. We only need write locks on the two original segment files until the
                // new one is swapped in. We still need a write lock on the buffer for the final
                // `pop_front`, but the runtime of that is very short.
                let mut segments_write = segments.write().unwrap();
                fs::remove_file(&segments_write[0]).unwrap();
                fs::remove_file(&segments_write[1]).unwrap();
                fs::rename(&new_segment_path, &segments_write[1]).unwrap();
                segments_write.pop_front();

                log::debug!("compaction finished");
            } else {
                log::debug!("compaction loop ticked, but there was nothing to do");
            }
            last_compaction = Instant::now();
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn compact(first: &mut File, second: &mut File, path: PathBuf) {
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
        compact(&mut file1, &mut file2, new1.clone());
        let mut new1 = File::open(new1).unwrap();

        let new2 = fixture.allocate_segment_file();
        compact(&mut new1, &mut file3, new2.clone());
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
