# 😯 How Does It Work?

## What Is Time Series Storage?

Reduct Storage is a storage engine which aims to solve the problem of storing data when you need to write data intensively and read it occasionally. The storage engine uses an [HTTP API ](http-api/)and stores the data as blobs. This means:

* Reduct Storage has no information about the format of the stored data. So, you can write whatever you want, but the storage can't aggregate your data. You have to read the same data, what you have written.
* The [HTTP API](http-api/) provides a simple and portable way to communicate with the storage engine, but the engine doesn't provide any query language to access the data.

In comparison to other S3-like object storage, Reduct Storage works differently:


* **Access By Time.** Each record in an entry is written with a timestamp and to read it you need to know the entry name and time.
* **Flat Storage Structure.** It doesn't have a tree-like structure for data. There are only buckets and entries with unique names in them.
* **Batching Data.** It doesn't store each record as a single file. It batches them into blocks of a fixed size so that it can store small objects more efficiently. You don't waste disk space because of the minimum size of file system blocks. Moreover, Reduct Storage pre-allocate blocks space to increase performance for write operations.&#x20;
* **Forward Writing.** The engine records data fastest if it only needs to append records to the current block. It means that, for better performance, you should always write data with the newest timestamps.
* **Strong FIFO Quota.** When you have intensive write operations, you may run out of disk space quickly. The engine removes the oldest block in a bucket as soon as the amount of the data reaches a specified quota limit.

## Internal Structure

![](<.gitbook/assets/Untitled Diagram.svg>)

As was mentioned above, Reduct Storage has flat structure:

#### **Bucket**

A container for entries which provides the following storage settings:

* **Maximum block size.**  The storage engine creates a new block after the size of the current block reaches this limit. A block might be bigger than this size because the storage engine can write a belated record.
* **Maximum number of records.** When the current block has more records than this limit, the storage engine creates a new block.&#x20;
* **Quota type.** The storage supports two quota types:
  * No quota. The bucket will grow to consume as much free disk space as is available - relative to how much data is written.
  * FIFO. The bucket removes the oldest block of some entry when it reaches the quota limit.
* **Quota size.**&#x20;

A user can manage bucket with[ Bucket API](http-api/bucket-api.md).

#### Entry

An entry represents a source of data. For example, It might be an image from a camera or the output of an AI model. The entry should have a unique name so that a user can address it with the [Entry API.](http-api/entry-api.md)

**Block**

A block of records with a unique ID. The storage engine batches the records by blocks for a few reasons:

* Reduce overhead of storing short records. If you store a small chunk of information as separate files they always consume at least one block of the file system. Typically it is 4 kilobytes. So if you have a blob with only 5 bytes of data, it consumes 4 kilobytes as a file anyway.
* Search records quickly. The storage engine finds a requested block first, then the record.

#### Record

A blob with a timestamp
