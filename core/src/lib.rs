//! Bytestack is a way to operate billions of files.
//!
//! - Documentation: All docs are carried by self, visit [`docs`] for more.
//! - SDK: All things in sdk
//!
//! # Quick Start
//! ```
//! #[tokio::main]
//! async fn main() {
//!     let handler = bytestack::core::BytestackHandler::new();
//!     let mut bw = handler.open_writer("s3://test/dadadad.bs/").unwrap();
//!     let mut idx = 0;
//!     while idx < 100 {
//!         let content = vec![idx; 4096];
//!         let id = bw
//!             .put(content, format!("filename-{}", idx), None)
//!             .await
//!             .expect("put data file");
//!         println!("put {} success", id);
//!         idx += 1;
//!     }
//!     bw.close().await.unwrap();
//!
//!     let br = handler.open_reader("s3://test/dadadad.bs/").unwrap();
//!     let stack_list = br.list_all_stack().await.unwrap();
//!     for s in &stack_list {
//!         println!(
//!             "stack_id: {}, last_modified: {}",
//!             s.stack_id, s.last_modified
//!         )
//!     }
//!     for s in &stack_list {
//!         for id in br.list_stack(s.stack_id).await.unwrap() {
//!             println!("{}", id);
//!         }
//!     }
//! }
//! ```


#![warn(missing_docs)]

// Deny unused qualifications.
#![deny(unused_qualifications)]

mod types;
pub use types::*;

#[cfg(feature = "docs")]
pub mod docs;
pub mod sdk;