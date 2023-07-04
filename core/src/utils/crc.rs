//! crc provides utils to do crc checksum
use crc::{Crc, CRC_32_ISCSI};
/// CASTAGNOLI is for doing crc checksum
pub const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
