use std::collections::BTreeMap;
use std::io::Write;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, fuzzcheck::DefaultMutator)]
enum Operation {
    Insert(u8, u8),
    Remove(u8),
    Contains(u8),
    Batch(Vec<(u8, Option<u8>)>),
    TornBatch(Vec<(u8, Option<u8>)>, usize, bool),
    Restart,
}

fn compare_with_btree_map(operations: &[Operation]) {
    static NDB: AtomicUsize = AtomicUsize::new(0);

    let path = format!(
        "test_db/fuzzcheck-test-{}",
        NDB.fetch_add(1, Ordering::SeqCst)
    );
    let _ = std::fs::remove_dir_all(&path);

    let config = crate::Config {
        max_space_amp: 2,
        max_log_length: 6,
        merge_ratio: 5,
        merge_window: 3,
        log_bufwriter_size: 1024,
        zstd_sstable_compression_level: 1,
    };

    let _ = std::fs::remove_dir_all(&path);
    let mut lsm = crate::Lsm::<1, 1>::recover_with_config(&path, config).unwrap();
    let mut map = BTreeMap::<[u8; 1], [u8; 1]>::new();
    for op in operations {
        match op {
            Operation::Insert(key, value) => {
                let a = lsm.insert([*key], [*value]).unwrap();
                let b = map.insert([*key], [*value]);
                assert_eq!(a, b);
            }
            Operation::Remove(key) => {
                let a = lsm.remove(&[*key]).unwrap();
                let b = map.remove(&[*key]);
                assert_eq!(a, b);
            }
            Operation::Contains(key) => {
                let a = lsm.contains_key(&[*key]);
                let b = map.contains_key(&[*key]);
                assert_eq!(a, b);
            }
            Operation::Batch(batch) => {
                let mut wb = vec![];
                for (k, v) in batch {
                    if let Some(v) = v {
                        map.insert([*k], [*v]);
                        wb.push(([*k], Some([*v])));
                    } else {
                        map.remove(&[*k]);
                        wb.push(([*k], None));
                    }
                }

                lsm.write_batch(&wb).unwrap();
            }
            Operation::TornBatch(batch, tear_offset, corrupt) => {
                // this tests torn batches which
                // should not be present in the
                // db after recovering.

                lsm.log.begin_tear();

                let mut wb = vec![];
                for (k, v) in batch {
                    if let Some(v) = v {
                        wb.push(([*k], Some([*v])));
                    } else {
                        wb.push(([*k], None));
                    }
                }

                lsm.write_batch(&wb).unwrap();

                lsm.log.apply_tear(*tear_offset, *corrupt);

                lsm.log.flush().unwrap();

                drop(lsm);

                lsm = crate::Lsm::recover_with_config(&path, config).unwrap();

                // lsm should be the same as if the batch was never applied
            }
            Operation::Restart => {
                lsm.flush().unwrap();
                drop(lsm);
                lsm = crate::Lsm::recover_with_config(&path, config).unwrap();
            }
        }
        assert_eq!(
            lsm.deref(),
            &map,
            "lsm and map diverged after op {:?}:\nlsm: {:?}\nmap:{:?}",
            op,
            lsm.deref(),
            map
        );
    }
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn check() {
    env_logger::init();
    let _ = std::fs::remove_dir_all("test_db");
    let result = fuzzcheck::fuzz_test(compare_with_btree_map)
        .default_options()
        .stop_after_first_test_failure(true)
        .launch();
    let _ = std::fs::remove_dir_all("test_db");
    assert!(!result.found_test_failure);
}

/*
#[test]
fn trophy_00() {
    let json = std::fs::read_to_string("trophy_case/00.json").unwrap();
    let ops: Vec<Operation> = serde_json::from_str(&json).unwrap();
    compare_with_btree_map(&ops[..]);
}

#[test]
fn t1() {
    compare_with_btree_map(&[
        Operation::Insert(220, 53),
        Operation::Restart,
        Operation::Restart,
    ]);
}
*/
