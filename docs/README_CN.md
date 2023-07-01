# Bytestack

<img src="logo.png" style="width: 60%">

## 简介 
Bytestack 是存储数十亿小文件的好方法。它基于优秀的开源工具Opendal，构建了一系列 cli tools，cache server 等，可以帮助用户将数十亿个文件上传到s3, fs或任何其他blobs等后端。

当然，灵感来自 facebook 的优秀论文: ["大海捞针:facebook的照片存储"](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf)。总之，每个文件的元信息通常是固定大小的，大量的小文件意味着会有大量的元信息，这对任何系统来说都是巨大的成本。

## 谁需要这个
在一些场景中，比如大型模型AI训练场景，需要这种技术，据我所知，一些AI训练师将他们的文件打包成一个blob，并将它们索引到另一个文件中，没有任何方法来验证文件的正确性，并且“一千个读者，一千个哈姆雷特”，会有许多不同格式的二进制和索引，这并不酷。

除了对存储容量收费外，公共云还对大量的写请求收取更高的费用，因此将数千个文件捆绑成一个文件是节省服务成本的更好方法。

## 文件结构

对于索引(idx)文件，我们存储16字节作为一个 Magic 的头(2 uint64)，在索引项后面排列。
```
| magic_number: u64 | stack_id: u64 | (16 bytes)
| cookie: u32 | offset_data: u64 | size_data: u64 | offset_meta: u64 | size_meta: u32 | (30 bytes)
| anoter index item | (30 bytes)
| ... | (30 bytes)

```

对于元信息文件(meta)，我们存储17字节作为 Magic 头，分别是(2 uint64) 和 一个 ’\n’(10)，之后元信息项排在后面。

```
| magic_number: u64 | stack_id: u64 | '\n': u8 | (17 bytes)
| create_time: u64 | file_offset: u64 | cookie: u32 | file_size: u32 | filename: String | extra: Vec<u8> | (n bytes)
| items ... | (n bytes)
```

对于一个数据文件，我们在开始存储 4096 字节，其中包含一个 magic 头，由 (2 uint64) 和保留的4080 个‘0’，之后，数据项排在后面。

每个数据项包含数据头，数据和填充，使头+数据填充到4k。

```
| magic_number: u64 | stack_id: u64 | vec![4080; 0] | (4096 bytes)
| data_record_header: DATA_HEADER 20 bytes  | data: n bytes  | padding (padding to 4k) | ...
| ..... | 
```

DATA_HEADER 结构体是:

```
| data_magic_record_start: u32 | cookie: u32 | size: u32 | crc: u32 | data_magic_record_end: u32 | (20 bytes)
```

**what is stack_id?** One stack_id corresponds to one stack, which is considered a bytestack(which contain a index file, a data file and a meta file).

## CLI tools

```
bst a tools for operating bytestack(developing)
```

## 贡献

暂无计划