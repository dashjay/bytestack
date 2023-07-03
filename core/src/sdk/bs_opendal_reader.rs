//! bs_reader provides all tools for reading bytestacks

use super::err::{CustomError, ErrorKind};
use crate::types::{IndexMagicHeader, IndexRecord, MetaMagicHeader, MetaRecord, Stack};
use crate::utils;
use futures::AsyncReadExt;
use futures::TryStreamExt;
use opendal::EntryMode;
use opendal::Metakey;
use opendal::Operator;
use opendal::Reader;

/// BytestackReader is tool for reading the bytestack
pub struct BytestackOpendalReader {
    operator: Operator,
    prefix: String,
}

/// BytestackOpendalIterator is helper to iterator index items and meta items in opendal way;
pub struct BytestackOpendalIterator {
    irs: Vec<IndexRecord>,
    reader: Reader,
}

impl BytestackOpendalIterator {
    /// next work like iterator but async version
    /// return (ir, mr) if there is, and return None if there is not.
    pub async fn next(&mut self) -> Option<(IndexRecord, MetaRecord)> {
        if self.irs.len() == 0 {
            return None;
        }
        let ir = self.irs.remove(0);
        let mut buf = Vec::with_capacity(ir.size_meta as usize);
        buf.resize(ir.size_meta as usize, 0);
        let res = match self.reader.read(&mut buf).await {
            Ok(n) => {
                if n == 0 {
                    return None;
                }
                n
            }
            Err(e) => {
                eprintln!("next error {}", e);
                return None;
            }
        };
        println!("res={res}, buf={}", buf.len());
        assert!(res == buf.len());
        let mr = match serde_json::from_slice::<MetaRecord>(&buf) {
            Ok(mr) => mr,
            Err(e) => {
                eprintln!("deserialize error {}", e);
                return None;
            }
        };
        Some((ir, mr))
    }
}

/// Fetcher is return by batch_fetch api
pub struct OpendalFetcher {}

impl OpendalFetcher {
    /// do_fetch fetch the data for user
    pub async fn do_fetch() -> Result<Vec<u8>, opendal::Error> {
        todo!()
    }
}

impl BytestackOpendalReader {
    pub fn new(operator: Operator, prefix: String) -> Self {
        Self {
            operator: operator,
            prefix: prefix,
        }
    }

    /// list return all stack(stack_id only) under this path
    pub async fn list(&self) -> Result<Vec<u64>, opendal::Error> {
        let mut out = Vec::<u64>::new();
        let mut ds = self.operator.list_with(self.prefix.as_str()).await?;
        while let Some(de) = ds.try_next().await? {
            let meta = self
                .operator
                .metadata(&de, Metakey::Mode & Metakey::LastModified)
                .await
                .unwrap();
            match meta.mode() {
                EntryMode::FILE => {
                    if de.name().ends_with(".idx") {
                        let stack_id_str = de.name().strip_suffix(".idx").unwrap();
                        let stack_id_u64 = u64::from_str_radix(stack_id_str, 10).unwrap();
                        out.push(stack_id_u64);
                    }
                }
                EntryMode::DIR => {
                    println!("skip dir {}", de.path())
                }
                EntryMode::Unknown => continue,
            }
        }
        Ok(out)
    }

    /// list_al return all stack(full stack info) under this path
    pub async fn list_al(&self) -> Result<Vec<Stack>, opendal::Error> {
        let mut out = Vec::<Stack>::new();
        let mut ds = self.operator.list_with(self.prefix.as_str()).await?;
        while let Some(de) = ds.try_next().await? {
            let meta = self
                .operator
                .metadata(&de, Metakey::Mode & Metakey::LastModified)
                .await
                .unwrap();
            match meta.mode() {
                EntryMode::FILE => {
                    if de.name().ends_with(".idx") {
                        let stack_id_str = de.name().strip_suffix(".idx").unwrap();
                        let stack_id_u64 = u64::from_str_radix(stack_id_str, 10).unwrap();
                        // TODO: (full_size should be calculate by read all idx)
                        out.push(Stack {
                            stack_id: stack_id_u64,
                            last_modified: meta.last_modified().unwrap(),
                            full_size: 0,
                        });
                    }
                }
                EntryMode::DIR => {
                    println!("skip dir {}", de.path())
                }
                EntryMode::Unknown => continue,
            }
        }
        Ok(out)
    }

    /// list_stack return all record(index_id) in giving stack_id.
    /// return with list of format!({:x}{:08x}, data_offset, cookie) which can be used to fetch single or batch data.
    pub async fn list_stack(&self, stack_id: u64) -> Result<Vec<String>, ErrorKind> {
        let mut out = Vec::<String>::new();
        let index_file_path = utils::get_index_file_path(&self.prefix, stack_id);
        let imh = match self
            .operator
            .reader_with(index_file_path.as_str())
            .range(0..IndexMagicHeader::size() as u64)
            .await
        {
            Ok(mut reader) => {
                let mut buf = Vec::new();
                buf.resize(IndexMagicHeader::size(), 0);
                match reader.read(&mut buf).await {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                    }
                }
                let deserialized = match bincode::deserialize::<IndexMagicHeader>(&buf) {
                    Ok(h) => h,
                    Err(e) => {
                        return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                    }
                };
                deserialized
            }
            Err(e) => {
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
            }
        };

        assert_eq!(imh.valid(), true, "header magic mismatch");
        assert!(imh.stack_id == stack_id, "stack_id mismatch");

        let bs = match self
            .operator
            .range_read(&index_file_path, IndexMagicHeader::size() as u64..)
            .await
        {
            Ok(bs) => bs,
            Err(e) => {
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
            }
        };
        for chunk in bs.chunks(IndexRecord::size()) {
            let ir = match bincode::deserialize::<IndexRecord>(chunk) {
                Ok(ir) => ir,
                Err(e) => {
                    return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                }
            };
            out.push(format!("{},{}", stack_id, ir.index_id()))
        }
        Ok(out)
    }

    /// list_stack_al_iter return BytestackOpendalIterator which work like an iterator for IndexRecord and MetaRecord
    pub async fn list_stack_al_iter(
        &self,
        stack_id: u64,
    ) -> Result<BytestackOpendalIterator, ErrorKind> {
        let mut irs = Vec::<IndexRecord>::new();
        let index_file_path = utils::get_index_file_path(&self.prefix, stack_id);
        let imh = match self
            .operator
            .reader_with(index_file_path.as_str())
            .range(0..IndexMagicHeader::size() as u64)
            .await
        {
            Ok(mut reader) => {
                let mut buf = Vec::new();
                buf.resize(IndexMagicHeader::size(), 0);
                match reader.read(&mut buf).await {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                    }
                }
                let deserialized = match bincode::deserialize::<IndexMagicHeader>(&buf) {
                    Ok(h) => h,
                    Err(e) => {
                        return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                    }
                };
                assert!(deserialized.valid());
                deserialized
            }
            Err(e) => {
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
            }
        };

        assert_eq!(imh.valid(), true, "header magic mismatch");
        assert!(imh.stack_id == stack_id, "stack_id mismatch");

        let bs = match self
            .operator
            .range_read(&index_file_path, IndexMagicHeader::size() as u64..)
            .await
        {
            Ok(bs) => bs,
            Err(e) => {
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
            }
        };
        for chunk in bs.chunks(IndexRecord::size()) {
            let ir = match bincode::deserialize::<IndexRecord>(chunk) {
                Ok(ir) => ir,
                Err(e) => {
                    return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
                }
            };
            irs.push(ir);
        }
        let meta_file_path = utils::get_meta_file_path(&self.prefix, stack_id);
        let mgh = MetaMagicHeader::new(stack_id);
        let reader = match self
            .operator
            .reader_with(&meta_file_path)
            .range(mgh.size() as u64..)
            .await
        {
            Ok(reader) => reader,
            Err(e) => {
                return Err(ErrorKind::IOError(CustomError::new(e.to_string())));
            }
        };

        Ok(BytestackOpendalIterator { irs, reader })
    }

    /// fetch data by index_id
    pub async fn fetch(
        &self,
        index_id: String,
        check_crc: bool,
    ) -> Result<Vec<u8>, opendal::Error> {
        todo!()
    }

    /// batch_fetch can fetch data for giving a batch of index_id
    pub async fn batch_fetch(
        &self,
        index_ids: Vec<String>,
        check_crc: bool,
    ) -> Result<Vec<OpendalFetcher>, opendal::Error> {
        todo!()
    }
}
