# 😯 How Does It Work?

## What Historian Blob Storage Is?

Reduct Storage aims to solve the problem of storing data in a case where you need to write some data intensively and read it accidentally by some time interval. The storage uses [HTTP API ](http-api/)and stores the data as blobs. It means:

* Storage has no idea about the format of the stored data. So, you can write whatever you want, but the storage can't aggregate your data. You have to read completely what you have written.
* [HTTP API](http-api/) provides a simple and portable way to communicate with the storage, but storage doesn't provide any query language to gather the data

In comparison with other S3 like blob storage, Reduct Storage does its job differently:

* **Access By Time.** Each record is written with a timestamp and to read it, you need to know the entry name and time.
* **Flat Storage Structure.** It has no tree-like structure for data. Only buckets and entries in them.
* **Batching Data.** It doesn't store each blob as one of few files in the file system. It batches them into blocks of the fixed size so that it can store small objects more efficiently. You don't waste the disk space because of the filesystem blocks.&#x20;
* **Forward Writing.** The storage records data fastest if it needs only to append records to the current block. It means for better performance, you write data always with the latest timestamps.&#x20;
* **Strong FIFO Quota.** When you have intensive write operations, you may run out of the disk space quickly. The storage removes the oldest block in a bucket at once when the amount of data reaches the quota limit.

## Internal Structure

As was mentioned above, Reduct Storage has flat structure:

* **Bucket** - the collections of entries. It contains settings that a common for all the entries
* **Entry— TODO**
