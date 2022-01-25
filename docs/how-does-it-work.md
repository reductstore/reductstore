# 😯 How Does It Work?

## What Is Historian Object Storage?

Reduct Storage aims to solve the problem of storing data in a case where you need to write some data intensively and read it accidentally by some time interval. The storage uses [HTTP API ](http-api/) and stores the data as blobs. It means:

* Storage has no idea about the format of the stored data. So, you can write whatever you want, but the storage can't aggregate your data. You have to read completely what you have written.
* [HTTP API](http-api/) provides a simple and portable way to communicate with the storage, but storage doesn't provide any query language to gather the data

In comparison with other S3-like object storages, Reduct Storage does its job differently:

* **Access By Time.** Each record is written with a timestamp and to read it, you need to know the entry name and time.
* **Flat Storage Structure.** It has no tree-like structure for data. Only buckets and entries in them.
* **Batching Data.** It doesn't store each blob as one of few files in the file system. It batches them into blocks of the fixed size so that it can store small objects more efficiently. You don't waste the disk space because of the filesystem blocks.&#x20;
* **Forward Writing.** The storage records data fastest if it needs only to append records to the current block. It means for better performance, you write data always with the latest timestamps.&#x20;
* **Strong FIFO Quota.** When you have intensive write operations, you may run out of the disk space quickly. The storage removes the oldest block in a bucket at once when the amount of data reaches the quota limit.

## Internal Structure

![](<.gitbook/assets/Untitled Diagram.svg>)

As was mentioned above, Reduct Storage has flat structure:

#### **Bucket**

A container for entries which provides the storage settings:

* **Maximal block size.** The size of the block, when the storage creates a new one after it finishes the current write operation. A block might be bigger than this size because the storage can write a belated record. &#x20;
* **Quota type.** The storage supports two types:
  * No quota. A bucket will consume the whole free disk space.
  * FIFO. A bucket removes the oldest block of some entry, when it reaches the quota limit.
* **Quota size.**

A user can manage bucket with[ Bucket API](http-api/bucket-api.md).

#### Entry

The entry represents a source of data. For example,  It might be a camera or output of a model. The entry should have a unique name so that a user can address to it with [Entry API.](http-api/bucket-api.md)

**Block**

A block of records with unique ID. The storage batches the records by blocks for few reasons:

* Reduce overhead to store little blobs. If you store a little chunk of information as separate files, they always consume at least one block of the file system. It maybe 512 bytes or bigger. So if you have a blob with 5 bytes of data, it consumes 512 bytes as a file.
* Search records quickly. The storage finds the needed block first, then the record.&#x20;

#### Record

A blob with a timestamp



