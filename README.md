# Bytestack

<img src="docs/logo.png" style="width: 60%">

[中文](./docs/README_CN.md)

## Introduction 
Bytestack is an good way to store billions of small files. With the basis of the excellent open source tool [Opendal](https://github.com/apache/incubator-opendal), we build a cli tool, cache server, etc. which can help user upload a billions files to the backends like s3, fs or any other blobs.

Inspiration, of course, comes from the excellent papers by facebook: [Finding a needle in Haystack: Facebook’s photo storage](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf). In short, the meta information of each file is usually fixed size, and a large number of small files means that there will be a large number of meta information, which is a huge cost for any system.

## Who need this
In some scenarios, such as large model AI training scenarios, this technology is wanted, as far as I know, some of AI trainer bundle their files into one blob and indexes them into another file, which has not any method of verifying file correctness, and 'To a thousand readers, there are a thousand Hamlets', there will be many different format of binary and indexes which is not cool.

In addition to charging for storage capacity, common public clouds also charge more for a large number of write requests, so bundle thousands files into one is a better way which save could services costs.

## File structure

For an index file, we store 16 bytes as a magic header(2 uint64), after that index item line up behind.
```
| magic_number: u64 | stack_id: u64 | (16 bytes)
| cookie: u32 | offset_data: u64 | size_data: u32 | offset_meta: u64 | size_meta: u32 | (28 bytes)
| anoter index item | (28 bytes)
| ... | (28 bytes)

```

For a meta file, we store a json marshaled magic header and a '\n'(10), after that meta items line up behind, also marshaled with json.

```
| {"magic_number": u64, "stack_id": u64 }| '\n': u8 | (17 bytes)
| {"create_time": u64, "file_offset": u64, "cookie": u32, "file_size": u32, "filename": String, extra: Vec<u8>}| (n bytes)
| items ... | (n bytes)
```

For a data file, we store 4096 bytes which contains a macic header(2 uint64) and 4080 '0' reserved for other purpose. after that, data items line up behind.

Every data item contains data header, data and padding which make header + data padding to 4k.

```
| magic_number: u64 | stack_id: u64 | vec![4080; 0] | (4096 bytes)
| data_record_header: DATA_HEADER 20 bytes  | data: n bytes  | padding (padding to 4k) | ...
| ..... | 
```

DATA_HEADER struct is that:

```
| data_magic_record_start: u32 | cookie: u32 | size: u32 | crc: u32 | data_magic_record_end: u32 | (20 bytes)
```

**what is stack_id?** One stack_id corresponds to one stack, which is considered a bytestack(which contain a index file, a data file and a meta file).

## CLI tools

```
bst a tools for operating bytestack(developing)
```

## Contribution

It's not planned yet