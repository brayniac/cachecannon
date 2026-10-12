//! `workload.read_values`: read each GET hit's value the way a client that
//! uses it would, so the generator's CPU per request includes that work.

use crate::metrics;
use crc_fast::CrcAlgorithm;

/// CRC-32c of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc_fast::checksum(CrcAlgorithm::Crc32Iscsi, bytes) as u32
}

/// Per-connection reader for received values.
///
/// [`read`](Self::read) copies the value into a buffer this connection owns,
/// computes a CRC-32c over the copy, and folds the CRC into
/// [`digest`](Self::digest). The fold makes each CRC an input to a value the
/// caller keeps, and the caller passes the digest to `black_box`, so neither the
/// copy nor the checksum can be removed by the compiler.
#[derive(Debug, Default)]
pub struct ValueReader {
    buf: Vec<u8>,
    digest: u32,
}

impl ValueReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Copy `value`, checksum the copy, and count its bytes in
    /// `values_read_bytes`.
    pub fn read(&mut self, value: &[u8]) {
        self.buf.clear();
        self.buf.extend_from_slice(value);
        self.digest = self.digest.rotate_left(1) ^ crc32c(&self.buf);
        metrics::VALUES_READ_BYTES.add(value.len() as u64);
    }

    /// Running fold of every CRC computed so far.
    pub fn digest(&self) -> u32 {
        self.digest
    }
}

#[cfg(test)]
mod tests {
    use super::{ValueReader, crc32c};

    #[test]
    fn crc32c_matches_known_vectors() {
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn read_checksums_the_value() {
        let mut reader = ValueReader::new();
        reader.read(b"123456789");
        assert_eq!(reader.digest(), 0xE306_9283);
        assert_eq!(reader.buf, b"123456789");
    }

    #[test]
    fn digest_depends_on_order() {
        let mut ab = ValueReader::new();
        ab.read(b"a");
        ab.read(b"b");
        let mut ba = ValueReader::new();
        ba.read(b"b");
        ba.read(b"a");
        assert_ne!(ab.digest(), ba.digest());
    }
}
