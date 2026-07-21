//! Dependency-free CRC32 used by persistent storage formats.

#[derive(Clone, Copy, Debug)]
pub(crate) struct Crc32 {
    state: u32,
}

impl Crc32 {
    pub(crate) fn new() -> Self {
        Self { state: u32::MAX }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= u32::from(byte);
            for _ in 0..8 {
                let mask = 0u32.wrapping_sub(self.state & 1);
                self.state = (self.state >> 1) ^ (0xedb8_8320 & mask);
            }
        }
    }

    pub(crate) fn finish(self) -> u32 {
        !self.state
    }
}

pub(crate) fn crc32(parts: &[&[u8]]) -> u32 {
    let mut checksum = Crc32::new();
    for part in parts {
        checksum.update(part);
    }
    checksum.finish()
}

#[cfg(test)]
mod tests {
    use super::crc32;

    #[test]
    fn crc32_matches_ieee_reference_vector() {
        assert_eq!(crc32(&[b"123456789"]), 0xcbf4_3926);
    }
}
