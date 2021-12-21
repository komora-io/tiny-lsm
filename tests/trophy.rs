#[test]
fn trophy_00() {
    env_logger::init();
    let json = std::fs::read_to_string("trophy_case/00.json").unwrap();
    let args: Args = serde_json::from_str(&json).unwrap();
    compare_with_btree_map(&args);
}
