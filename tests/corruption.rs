#[test]
fn test_corruption() {
    static NDB: AtomicUsize = AtomicUsize::new(0);

    env_logger::init();

    let wb = [([0], None), ([1], Some([1])), ([2], None)];

    let _ = std::fs::remove_dir_all("test_corruption_db");
    for i in 0..100 {
        let path = format!(
            "test_corruption_db/corruption-test-{}",
            NDB.fetch_add(1, Ordering::SeqCst)
        );

        let _ = std::fs::remove_dir_all(&path);

        let mut lsm = crate::Lsm::<1, 1>::recover(&path).unwrap();
        lsm.flush().unwrap();

        lsm.log.begin_tear();
        lsm.write_batch(&wb).unwrap();

        lsm.log.apply_tear(i, true);

        drop(lsm);

        lsm = crate::Lsm::recover(&path).unwrap();

        assert!(lsm.is_empty(), "corruption test at slot {}", i);
    }
    let _ = std::fs::remove_dir_all("test_corruption_db");
}
