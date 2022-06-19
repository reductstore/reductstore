# 😯 How Does It Work?

## What Is Time Series Storage?

Reduct Storage is a storage engine, which aims to solve a problem of storing data in a case where you need to write some data intensively and read it accidentally by some time interval. The storage engine uses an [HTTP API ](http-api/)and stores the data as blobs. It means:

* Reduct Storage has no information about the format of the stored data. So, you can write whatever you want, but the storage can't aggregate your data. You have to read the same data, what you have written.
* [HTTP API](http-api/) provides a simple and portable way to communicate with the storage engine, but the engine doesn't provide any query language to gather the data

In comparison with other S3-like object storage, Reduct Storage does its job differently:

* **Access By Time.** Each record in an entry is written with a timestamp and to read it, you need to know the entry name and time.
* **Flat Storage Structure.** It has no tree-like structure for data. There are only buckets and entries with unique names in them.
* **Batching Data.** It doesn't store a record as a single file. It batches them into blocks of the fixed size so that it can store small objects more efficiently. You don't waste the disk space because of the file system blocks. Moreover, Reduct Storage pre-allocate blocks size to increase performance for write operations.&#x20;
* **Forward Writing.** The engine records data fastest if it needs only to append records to the current block. It means for better performance, you write data always with the newest timestamps.
* **Strong FIFO Quota.** When you have intensive write operations, you may run out of the disk space quickly. The engine removes the oldest block in a bucket at once when the amount of the data reaches a quota limit.

## Internal Structure

![](<.gitbook/assets/Untitled Diagram.svg>)

As was mentioned above, Reduct Storage has flat structure:

#### **Bucket**

A container for entries which provides the following storage settings:

* **Maximal block size.**  The storage engine creates a new block after when the size of the current reaches this limit. A block might be bigger than the size because the storage engine can write a belated record.
* **Maximum number of records.** When the current block has more records than this limit, the storage engine creates a new one.&#x20;
* **Quota type.** The storage supports two types:
  * No quota. A bucket will consume the whole free disk space.
  * FIFO. A bucket removes the oldest block of some entry, when it reaches the quota limit.
* **Quota size.**&#x20;

A user can manage bucket with[ Bucket API](http-api/bucket-api.md).

#### Entry

An entry represents a source of data. For example, It might be a camera or output of a model. The entry should have a unique name so that a user can address to it with [Entry API.](http-api/bucket-api.md)

**Block**

A block of records with unique ID. The storage engine batches the records by blocks for a few reasons:

* Reduce overhead of storing little records. If you store a little chunk of information as separate files, they always consume at least one block of the file system. It maybe 512 bytes or bigger. So if you have a blob with 5 bytes of data, it consumes 512 bytes as a file anyway.
* Search records quickly. The storage engine finds a requested block first, then the record.

#### Record

A blob with a timestamp
