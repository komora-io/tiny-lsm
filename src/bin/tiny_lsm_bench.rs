fn main() {
    let before_recovery = std::time::Instant::now();
    let mut lsm = tiny_lsm::Lsm::<8, 8>::recover("tiny_lsm_bench").unwrap();
    dbg!(before_recovery.elapsed());

    if let Some((k, _v)) = lsm.iter().next_back() {
        println!("max key recovered: {:?}", u64::from_le_bytes(*k));
    } else {
        println!("starting from scratch");
    }

    let before_writes = std::time::Instant::now();
    for i in 0_u64..1_000_000_000 {
        lsm.insert(i.to_le_bytes(), [0; 8]).unwrap();
    }
    lsm.flush().unwrap();
    dbg!(before_writes.elapsed());

    std::thread::sleep(std::time::Duration::from_secs(200));
}
