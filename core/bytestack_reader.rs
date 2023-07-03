use super::err::{CustomError, ErrorKind};
use crate::types::{
    data::{DataRecordHeader, _DATA_RECORD_HEADER_MAGIC_END, _DATA_RECORD_HEADER_MAGIC_START},
    index::{IndexMagicHeader, IndexRecord, _INDEX_HEADER_MAGIC},
    MetaRecord, DataRecord,
};
use crate::utils;
use bincode;
use futures::TryStreamExt;
use opendal::EntryMode;
use opendal::Metakey;
use opendal::Operator;
use std::collections::HashMap;
use std::sync::RwLock;
use tokio::io::AsyncReadExt;
use futures::stream::Stream;
use futures::task::Context;
use futures::task::Poll;
use std::pin::Pin;

pub struct StackReader {
    operator: Operator,
    prefix: String,
    index_cache: RwLock<HashMap<String, IndexRecord>>,
}

pub struct StackIDWithTime {
    pub stack_id: u64,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

pub struct StackReaderIter{
    operator: Operator,
    prefix: String,
    stacks: Vec<StackIDWithTime>,
    stacks_idxs: Vec<IndexRecord>,
    cursor: i32
}

impl StackReaderIter{
    
}

impl Stream for StackReaderIter{
    type Item = (IndexRecord,DataRecord,MetaRecord);
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::option::Option<<Self as Stream>::Item>> {
        todo!()
    }
}

impl StackReader {
    pub fn new(operator: Operator, prefix: String) -> StackReader {
        StackReader {
            operator: operator,
            prefix: prefix,
            index_cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn list_all_stack(&self) -> Result<Vec<StackIDWithTime>, opendal::Error> {
        let mut out = Vec::<StackIDWithTime>::new();
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
                        out.push(StackIDWithTime {
                            stack_id: stack_id_u64,
                            last_modified: meta.last_modified().unwrap(),
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

        assert!(
            imh.index_header_magic == _INDEX_HEADER_MAGIC,
            "header magic mismatch"
        );
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

    // pub async fn get_data_by_index_id(&self, id: String) -> Result<Vec<u8>, ErrorKind> {
    //     let index_id = match utils::parse_stack_id(&id) {
    //         Some(index_id) => index_id,
    //         None => {
    //             return Err(ErrorKind::InvalidArgument(CustomError::new(format!(
    //                 "invalid index_id {}",
    //                 id
    //             ))))
    //         }
    //     };

    //     let data_file_path = utils::get_data_file_path(&self.prefix, index_id.stack_id());

    //     let mut dat = self
    //         .operator
    //         .range_read(
    //             &data_file_path,
    //             index_id.file_offset()..index_id.try_into() ,
    //         )
    //         .await
    //         .map_err(|e| ErrorKind::IOError(CustomError::new(e.to_string())))?;

    //     let drh = bincode::deserialize::<DataRecordHeader>(&dat[..DataRecordHeader::size()])
    //         .map_err(|e| ErrorKind::IOError(CustomError::new(e.to_string())))?;

    //     assert_eq!(drh.data_magic_record_start, _DATA_RECORD_HEADER_MAGIC_START);
    //     assert_eq!(drh.data_magic_record_end, _DATA_RECORD_HEADER_MAGIC_END);
    //     assert_eq!(drh.cookie(), index_id.cookie());
    //     if drh.data_size() < 4096 - (DataRecordHeader::size() as u32) {
    //         dat.drain(0..DataRecordHeader::size());
    //         return Ok(dat);
    //     }
    //     let mut full = dat.clone();

    //     let dat_after_first_4096 = self
    //         .operator
    //         .range_read(
    //             &data_file_path,
    //             index_id.file_offset() + 4096
    //                 ..index_id.file_offset() + drh.data_size() as u64 - 4096,
    //         )
    //         .await
    //         .map_err(|e| ErrorKind::IOError(CustomError::new(e.to_string())))?;
    //     full.extend_from_slice(&dat_after_first_4096);
    //     Ok(full)
    // }
}
