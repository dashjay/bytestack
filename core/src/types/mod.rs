//! types hold all types about data, index, meta
pub mod data;
pub use data::DataMagicHeader;
pub use data::DataRecordHeader;
pub use data::DataRecord;

pub mod index;
pub use index::IndexMagicHeader;
pub use index::IndexRecord;

pub mod meta;
pub use meta::MetaMagicHeader;
pub use meta::MetaRecord;

pub mod stack;
pub use stack::Stack;