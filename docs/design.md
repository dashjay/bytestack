# Design of bytestack services

Services of bytestack contains: controller, index and bserve, 3 parts in total.
And we merge controller + index into one server and bserve provides high speed read.


## Controller + Index

|    |    |    |    |
| -- | -- | -- | -- |
| interface | args | returns | usage |
| next_stack_id | `NULL` | `u64` | manage the stack id |
| register_stack_source | `stack_id: u64, locations: Vec<String>` | `NULL` | register stack to source location |
| query_registered_source | `stack_id: u64` | `locations: Vec<String>` | find location source by stack_id |
| locate_stack | `stack_id: u64` | `data_addrs: Vec<String>` | locate where to get the bytestack |
| pre_load | `stack_id: u64` | `NULL` | tell controller to preload a bytestack |

## BServer(DataServer | ManageServer)

DataServer(grpc)

| -- | -- | -- | -- |
| interface | args | returns | usage |
| fetch_one | `index_id: String` | Vec<u8> | fetch one data |
| fetch_batch(Stream) | `index_ids: Vec<String>` | `Stream(Vec<u8>)` | batch fetch datas |
| range_from(Stream) | `start_index_id: String` | `Stream((index_id, Vec<u8>))` | range read from start_index_id |

ManageServer(grpc)

| -- | -- | -- | -- |
| interface | args | returns | usage |
| heartbeat | ... | NULL | controller heartbeat and assign task for bserver |
