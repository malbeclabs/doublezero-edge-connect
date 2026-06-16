mod common;

use common::replay;

#[test]
fn tob_golden_splits_into_valid_frames() {
    let bytes = std::fs::read("tests/fixtures/tob_marketdata.bin").expect("read tob_marketdata.bin");
    let frames = replay::split_frames(&bytes, replay::TOB_MAGIC);
    assert!(!frames.is_empty(), "expected at least one TOB frame");
    for f in &frames {
        assert!(f.len() >= 24);
        assert_eq!(u16::from_le_bytes([f[0], f[1]]), replay::TOB_MAGIC);
    }
}
