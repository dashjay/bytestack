//! utils provides utils for global use
pub mod path;
pub use path::*;

pub mod index_id;
pub use index_id::*;

pub mod time;
pub use time::*;

pub mod crc;
pub use self::crc::CASTAGNOLI;

mod log;
pub use self::log::init_logger;
