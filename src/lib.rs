//! `tiny-lsm` is a dead-simple in-memory LSM for managing
//! fixed-size metadata in more complex systems.
//!
//! Uses crc32fast to checksum all key-value pairs in the log and
//! sstables. Uses zstd to compress all sstables. Performs sstable
//! compression in the background in a low-priority thread.
//!
//! Because the data is in-memory, there is no need to put bloom
//! filters on the sstables, and get / range cannot fail due
//! to IO issues. This is a bad choice for large data sets if you
//! require quick recovery time because it needs to read all of
//! the sstables and the write ahead log when starting up.
//!
//! The benefit to using tiered sstables at all, despite being
//! in-memory, is that they act as an effective log-deduplication
//! mechanism, keeping space amplification very low.
//!
//! Maximum throughput is not the goal of this project. Low space
//! amplification and very simple code is the goal, because this
//! is intended to maintain metadata in more complex systems.
//!
//! There is currently no compaction throttling. You can play
//! with the constants around compaction to change compaction
//! characteristics.
//!
//! Never change the constant size of keys or values for an existing
//! database.
//!
//! Basically untested, but on the flip side there are less
//! than 500 lines of bugs. Let it be a case study for how
//! many bugs can exist in a codebase with 0 usage of unsafe.
//!
//! # Examples
//!
//! ```
//! // open up the LSM
//! let mut lsm = tiny_lsm::Lsm::recover("path/to/base/dir").expect("recover lsm");
//!
//! // store some things
//! let key: [u8; 8] = 8_u64.to_le_bytes();
//! let value: [u8; 1] = 255_u8.to_le_bytes();
//! lsm.insert(key, value);
//!
//! assert_eq!(lsm.get(&key), Some(&value));
//!
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, prelude::*, BufReader, BufWriter, Result};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

const MAX_SPACE_AMP: u64 = 2;
const LOG_COMPACT_BYTES: usize = 32 * 1024 * 1024;
const MERGE_RATIO: u64 = 3;
const MERGE_WINDOW: usize = 10;
const SSTABLE_DIR: &str = "sstables";
const LOG_BUFFER: usize = 32 * 1024;

fn hash<const K: usize, const V: usize>(k: &[u8; K], v: &[u8; V]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&*k);
    hasher.update(&*v);

    // we XOR the hash to make sure it's something other than 0 when empty,
    // because 0 is an easy value to create accidentally or via corruption.
    hasher.finalize() ^ 0xFF
}

enum WorkerMessage {
    NewSST { id: u64, sst_sz: u64, db_sz: u64 },
}

struct Worker<const K: usize, const V: usize> {
    sstable_directory: BTreeMap<u64, u64>,
    inbox: mpsc::Receiver<WorkerMessage>,
    db_sz: u64,
    path: PathBuf,
}

impl<const K: usize, const V: usize> Worker<K, V> {
    fn run(mut self) {
        loop {
            let message = if let Ok(message) = self.inbox.recv() {
                message
            } else {
                log::info!("tiny-lsm compaction worker quitting");
                return;
            };

            let WorkerMessage::NewSST { id, sst_sz, db_sz } = message;

            self.db_sz = db_sz;
            self.sstable_directory.insert(id, sst_sz);

            loop {
                let res = self.sstable_maintenance();
                match res {
                    Ok(false) => break,
                    Ok(true) => continue,
                    Err(e) => {
                        log::error!("error while compacting sstables in the background: {:?}", e);
                        break;
                    }
                }
            }
        }
    }

    fn sstable_maintenance(&mut self) -> Result<bool> {
        let on_disk_size: u64 = self.sstable_directory.values().sum();

        log::debug!("disk size: {} mem size: {}", on_disk_size, self.db_sz);
        if on_disk_size / self.db_sz > MAX_SPACE_AMP {
            log::info!(
                "performing full compaction, decompressed on-disk \
                database size has grown beyond {}x the in-memory size",
                MAX_SPACE_AMP
            );
            let run_to_compact: Vec<u64> = self.sstable_directory.keys().copied().collect();

            self.compact_sstable_run(&run_to_compact)?;
            return Ok(true);
        }

        if self.sstable_directory.len() < MERGE_WINDOW {
            return Ok(false);
        }

        for window in self
            .sstable_directory
            .iter()
            .collect::<Vec<_>>()
            .windows(MERGE_WINDOW)
        {
            if window
                .iter()
                .skip(1)
                .all(|w| *w.1 * MERGE_RATIO > *window[0].1)
            {
                let run_to_compact: Vec<u64> = window.into_iter().map(|(id, _sum)| **id).collect();

                self.compact_sstable_run(&run_to_compact)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn compact_sstable_run(&mut self, sstable_ids: &[u64]) -> Result<()> {
        log::debug!(
            "trying to compact sstable_ids {:?}",
            sstable_ids
                .iter()
                .map(|id| id_format(*id))
                .collect::<Vec<_>>()
        );
        let mut sstable_data = vec![];

        for sstable_id in sstable_ids {
            let mut sstable = read_sstable::<K, V>(&self.path, *sstable_id)?;
            sstable_data.append(&mut sstable);
        }

        let sst_id = sstable_ids.iter().max().unwrap();

        let map = sstable_data.into_iter().collect();

        write_sstable(&self.path, *sst_id, &map, true)?;

        let sst_sz = map.len() as u64 * (4 + K + V) as u64;
        self.sstable_directory.insert(*sst_id, sst_sz);

        log::debug!("compacted range into sstable {}", id_format(*sst_id));

        for sstable_id in sstable_ids {
            if sstable_id == sst_id {
                continue;
            }
            fs::remove_file(self.path.join(SSTABLE_DIR).join(id_format(*sstable_id)))?;
            self.sstable_directory.remove(sstable_id).unwrap();
        }

        fs::File::open(self.path.join(SSTABLE_DIR))?.sync_all()?;

        Ok(())
    }
}

fn id_format(id: u64) -> String {
    format!("{:016x}", id)
}

fn list_sstables(path: &Path) -> Result<BTreeMap<u64, u64>> {
    let mut sstable_map = BTreeMap::new();

    for dir_entry_res in fs::read_dir(path.join(SSTABLE_DIR))? {
        let dir_entry = dir_entry_res?;
        let file_name = dir_entry.file_name().into_string().unwrap();
        if let Ok(id) = u64::from_str_radix(&file_name, 16) {
            let metadata = dir_entry.metadata()?;

            sstable_map.insert(id, metadata.len());
        } else {
            if file_name.ends_with("-tmp") {
                log::warn!("removing incomplete sstable rewrite {}", file_name);
                fs::remove_file(path.join(SSTABLE_DIR).join(file_name))?;
            }
        }
    }

    Ok(sstable_map)
}

fn write_sstable<const K: usize, const V: usize>(
    path: &Path,
    id: u64,
    items: &BTreeMap<[u8; K], [u8; V]>,
    tmp_mv: bool,
) -> Result<()> {
    let sst_dir_path = path.join(SSTABLE_DIR);
    let sst_path = if tmp_mv {
        sst_dir_path.join(format!("{}-tmp", id))
    } else {
        sst_dir_path.join(id_format(id))
    };

    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&sst_path)?;

    let mut bw = BufWriter::new(zstd::Encoder::new(file, 3).unwrap());

    bw.write(&(items.len() as u64).to_le_bytes())?;

    for (k, v) in items {
        let crc: u32 = hash(k, v);
        bw.write(&crc.to_le_bytes())?;
        bw.write(k)?;
        bw.write(v)?;
    }

    bw.flush()?;

    bw.get_mut().get_mut().sync_all()?;

    if tmp_mv {
        let new_path = sst_dir_path.join(id_format(id));
        fs::rename(sst_path, new_path)?;
    }

    Ok(())
}

fn read_sstable<const K: usize, const V: usize>(
    path: &Path,
    id: u64,
) -> Result<Vec<([u8; K], [u8; V])>> {
    let file = fs::OpenOptions::new()
        .read(true)
        .open(path.join(SSTABLE_DIR).join(id_format(id)))?;

    let mut reader = BufReader::with_capacity(128 * 1024 * 1024, zstd::Decoder::new(file).unwrap());

    let mut buf = vec![0; 4 + K + V];

    let len_buf = &mut [0; 8];

    reader.read_exact(len_buf)?;

    let expected_len: u64 = u64::from_le_bytes(*len_buf);
    let mut sstable = Vec::with_capacity(expected_len as usize);

    while let Ok(()) = reader.read_exact(&mut buf) {
        let crc_expected: u32 = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let k: [u8; K] = buf[4..K + 4].try_into().unwrap();
        let v: [u8; V] = buf[K + 4..4 + K + V].try_into().unwrap();
        let crc_actual: u32 = hash(&k, &v);

        if crc_expected != crc_actual {
            log::warn!("detected torn-write while reading sstable {:016x}", id);
            break;
        }

        sstable.push((k, v));
    }

    if sstable.len() as u64 != expected_len {
        log::warn!(
            "sstable {:016x} tear detected - process probably crashed \
                  before full sstable could be written out",
            id
        );
    }

    Ok(sstable)
}

pub struct Lsm<const K: usize, const V: usize> {
    log: BufWriter<fs::File>,
    memtable: BTreeMap<[u8; K], [u8; V]>,
    db: BTreeMap<[u8; K], [u8; V]>,
    worker_outbox: mpsc::Sender<WorkerMessage>,
    next_sstable_id: u64,
    dirty_bytes: usize,
    path: PathBuf,
}

impl<const K: usize, const V: usize> std::ops::Deref for Lsm<K, V> {
    type Target = BTreeMap<[u8; K], [u8; V]>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl<const K: usize, const V: usize> Lsm<K, V> {
    pub fn recover<P: AsRef<Path>>(p: P) -> Result<Lsm<K, V>> {
        let path = p.as_ref();
        if !path.exists() {
            fs::create_dir_all(path)?;
            fs::create_dir(path.join(SSTABLE_DIR))?;
        }

        let log = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.join("log"))?;

        let sstable_directory = list_sstables(path)?;
        let mut sstables: Vec<u64> = sstable_directory.iter().map(|(id, _sz)| *id).collect();

        let mut sstable_data = vec![];
        for sstable_id in &sstables {
            let mut sstable = read_sstable::<K, V>(path, *sstable_id)?;
            sstable_data.append(&mut sstable);
        }
        let mut db: BTreeMap<_, _> = sstable_data.into_iter().collect();

        let max_sstable_id = sstables.pop();

        let mut reader = BufReader::new(log);

        let mut buf = vec![0; 4 + K + V];

        let mut recovered = 0;
        while let Ok(()) = reader.read_exact(&mut buf) {
            let crc_expected: u32 = u32::from_le_bytes(buf[0..4].try_into().unwrap());
            let k: [u8; K] = buf[4..K + 4].try_into().unwrap();
            let v: [u8; V] = buf[K + 4..4 + K + V].try_into().unwrap();
            let crc_actual: u32 = hash(&k, &v);

            if crc_expected != crc_actual {
                // need to back up a few bytes to chop off the torn log
                let log_file = reader.get_mut();
                log_file.seek(io::SeekFrom::Start(recovered))?;
                log_file.set_len(recovered)?;
                log_file.sync_all()?;
                fs::File::open(path.join(SSTABLE_DIR))?.sync_all()?;
                break;
            }

            recovered += buf.len() as u64;

            db.insert(k, v);
        }

        let (tx, rx) = mpsc::channel();

        let worker: Worker<K, V> = Worker {
            path: path.clone().into(),
            sstable_directory,
            inbox: rx,
            db_sz: db.len() as u64 * (K + V) as u64,
        };

        std::thread::spawn(move || worker.run());

        let lsm = Lsm {
            log: BufWriter::with_capacity(LOG_BUFFER, reader.into_inner()),
            memtable: BTreeMap::new(),
            db,
            path: path.into(),
            next_sstable_id: max_sstable_id.unwrap_or(0) + 1,
            dirty_bytes: recovered as usize,
            worker_outbox: tx,
        };

        Ok(lsm)
    }

    pub fn insert(&mut self, k: [u8; K], v: [u8; V]) -> Result<Option<[u8; V]>> {
        let crc: u32 = hash(&k, &v);
        self.log.write(&crc.to_le_bytes())?;
        self.log.write(&k)?;
        self.log.write(&v)?;

        self.memtable.insert(k, v);

        self.dirty_bytes += 4 + K + V;

        if self.dirty_bytes > LOG_COMPACT_BYTES {
            self.flush()?;
        }

        Ok(self.db.insert(k, v))
    }

    pub fn flush(&mut self) -> Result<()> {
        self.log.flush()?;

        if self.dirty_bytes > LOG_COMPACT_BYTES {
            log::debug!("flushing log");
            self.log.get_mut().sync_all()?;
            let memtable = std::mem::take(&mut self.memtable);
            let sst_id = self.next_sstable_id;
            if let Err(e) = write_sstable(&self.path, sst_id, &memtable, false) {
                // put memtable back together before returning
                self.memtable = memtable;
                return Err(e.into());
            }

            let sst_sz = 8 + (memtable.len() as u64 * (4 + K + V) as u64);
            let db_sz = self.db.len() as u64 * (K + V) as u64;

            self.worker_outbox
                .send(WorkerMessage::NewSST {
                    id: sst_id,
                    sst_sz,
                    db_sz,
                })
                .expect("compactor must outlive db");

            self.next_sstable_id += 1;

            assert_eq!(self.log.buffer().len(), 0);

            let log_file: &mut fs::File = self.log.get_mut();
            log_file.seek(io::SeekFrom::Start(0))?;
            log_file.set_len(0)?;
            log_file.sync_all()?;
            fs::File::open(self.path.join(SSTABLE_DIR))?.sync_all()?;

            self.dirty_bytes = 0;
        }

        Ok(())
    }
}
