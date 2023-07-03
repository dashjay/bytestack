//! sdk provides all tools for reading, writing, appending bytestacks.
pub mod bs_opendal_reader;
pub use bs_opendal_reader::BytestackOpendalReader;

pub mod bs_opendal_writer;
pub use bs_opendal_writer::BytestacOpendalkWriter;

pub mod err;
