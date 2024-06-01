# transactions


## design

Mari performs operations, both reads and writes, to the instance using transactions. If a transaction succeeds in full, it is written to the memory map, otherwise it is discarded and retried. This ensures that transactions are `ACID`. Read only transactions are also performed in isolation but can run while read-write operations are occuring. The API is meant to be simple and is inspired by the [bbolt](https://github.com/etcd-io/bbolt) API for transaction, since I appreciate the simplicity of creating and using transactions.


## mvcc

Mari implements a form of [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control), or multi-version concurrency control. The implementation allows for both multi-reader/multi-writer setups with parallel mixed workloads. When a transaction is invoked, it performs the operation on the highest known version in the file metadata. Both reads and writes are performed in isolation.

### reads

Reads first grab the latest serialized version from the metadata and then traverse the data structure from that particular version. This allows reads to operate and return results while writers are appending data to the memory mapped file. No retry mechanism is in place for reads so no data is being mutated.

### writes

Like Reads, write transactions get the latest serialized version from the metadata and then build the updates in place, before incrementing the version number and then serializing the paths. Before the serialized data can be written to the memory mapped file, the write first checks that the version of its update is 1 more than the version in the metadata and then attempts to perform a `compare-and-swap` operation. If both checks pass, the data is appended to the data in the memory map and the metadata is updated with the new version, the next start offset for subsequent writes, and the offset of the root of the trie for other operations to point to. If the operation fails, the transaction is discarded and retried from the start of the structure.


## occ

Mari also implements a strict [OCC](https://en.wikipedia.org/wiki/Optimistic_concurrency_control), or optimistic concurrency control, policy. It uses serialized `OCC`, which is the highest level of `OCC`. This ensures that transactions behave as if they are executed one after the other. This is achieved through the use of version stamped nodes, as explained above. If a transaction fails, then it is "rolled back" and retried from the start.


## batching

Batching writes into a single transaction can also significantly improve performance of writes, including any writes that occur after the batched write, since all paths for each write will be serialized together onto the memory map instead of writing paths one at a time. This also reduces the amount of node repeats on path copies so the overall footprint on the size of the memory mapped file will decrease. However, if batched writes are too large performance may degrade as the serialized path can take up a significant amount of memory.


## design

Transactions take in a transaction function with the following signature:
```go
tx := func(tx *mari.MariTx) error
```

the `mari.MariTx` object, when a transaction is initiated, includes:

  1. the mari instance
  2. the current root as a pointer
  3. a flag determining if the transaction is read or read-write

However, this is all hidden from the end user, as the actual transaction is handled in the background, for a level of inversion of control. The thought process is that transactions should be simple to create. On read-write transactions, a mix of reads and writes can be performed and the updated data is only serialized once all operations in the transaction have been completed. Transaction operations are as follows:

  1. tx.Get - get a key-value from the instance if it exists. Nil is returned if non-existant
  2. tx.Put - put a key-value pair into the instance
  3. tx.Delete - delete a key-value pair from the instance, if it exists
  4. tx.Iterate - generate an ordered iteration over a span of elements, from a start key up to a specified number of elements
  5. tx.Range - perform a range operation to find all elements between a start key and an end key

If a `Put` or `Delete` is attempted in a read only transaction, an error will be thrown indicating that the user should be using a read-write transaction

As mentioned above, there are two variants of transactions, on the `mari` instance itself:

  1. ReadTx - perform a read only transaction, which takes in a transaction function containing one or multiple transaction operations
  2. UpdateTx - perform a read-write transaction, which again takes in a transaction function


## transforms

Read transactions (`Get`, `Iterate`, `Range`) can take in a transform function, with the following signature:
```go
transform := func(kvPair *mari.KeyValuePair) *mari.KeyValuePair
```

Transforms are a way to pre-process data before returning results, allowing a user to mutate results to limit post processing. If a transform is not provided, then the operations will default to returning the key-value pair as is


## response on reads

All read transactions will return `*mari.KeyValuePair`, which has the following structure:
```
{
	Key <[]byte>,
	Value <[]byte>
}
```

`Get` returns a single key-value object, while `Iterate` and `Range` return a list of key-value objects, in ascending order.


## iterate/range options

`Iterate` and `Range` operations take in optional options, as follows:
```
{
	MinVersion *uint64
	Transform *MariOpTransform
}
```

The `MinVersion` is the minimum version to return from the operation. It will default to the earliest version in the data if not provided. The Transform is just a custom transform function, as explained above.


## usage

```go
package main

import "os"

import "github.com/sirgallo/mariv2"


const FILENAME = "<your-file-name>"


func main() {
  homedir, homedirErr := os.UserHomeDir()
  if homedirErr != nil { panic(homedirErr.Error()) }

  opts := mariv2.InitOpts{ Filepath: homedir, FileName: FILENAME }

  mariInst, openErr := mariv2.Open(opts)
  if openErr != nil { panic(openErr.Error()) }
  defer mariInst.Close()

  // put a value in mari, using an UpdateTx
  putErr := mariInst.UpdateTx(func(tx *mariv2.Tx) error {
    putTxErr := tx.Put([]byte("hello"), []byte("world"))
    if putTxErr != nil { return putTxErr }

    return nil
  })

  if putErr != nil { panic(putErr.Error()) }

  // get a value in mari, using a ReadTx
  // if transform is nil, kvPair is returned as is
  var kvPair *mariv2.KeyValuePair
  getErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
    var getTxErr error
    kvPair, getTxErr = tx.Get([]byte("hello"), nil)
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if getErr != nil { panic(getErr.Error()) }

  // get a set of ordered iterated key value pairs from a start key to the total result size, using a ReadTx
  // if opts is nil, version is set to the earliest version and transform will not be used
  var iteratedkvPairs []*mariv2.KeyValuePair
  iterErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
    var iterTxErr error
    iteratedkvPairs, iterTxErr = tx.Iterate([]("hello"), 10000, nil)
    if iterTxErr != nil { return iterTxErr }

    return nil
  })

  if iterErr != nil { panic(iterErr.Error()) }

  // get a range of key-value pairs from a minimum version, using a ReadTx
  // if opts is nil, version is set to the earliest version and transform will not be used
  var rangekvPairs []*mariv2.KeyValuePair
  rangeErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
    var rangeTxErr error
    rangekvPairs, rangeTxErr = tx.Range([]("hello"), []("world"), nil)
    if rangeTxErr != nil { return rangeTxErr }

    return nil
  })

  if rangeErr != nil { panic(rangeErr.Error()) }

  // create a transformer to process results before being returned
  transform := func(kvPair *mariv2.KeyValuePair) *mariv2.KeyValuePair {
    kvPair.Value = append(kvPair.Value, kvPair.Value...)
    return kvPair
  }

  // opts for range + iteration functions
  // can also contain MinVersion for the minimum version
  rangeOpts := &mariv2.RangeOpts{ Transform: &transform }

  // get a transformed key-value in mari
  var transformedKvPair *mariv2.KeyValuePair
  getTransformedErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
    var getTxErr error
    transformedKvPair, getTxErr = tx.Get([]byte("hello"), transform)
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if getErr != nil { panic(getErr.Error()) }

  // get a range of key value pairs with transformed values
  var transformedRangePairs []*mariv2.KeyValuePair
  transformedRangeErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
    var rangeTxErr error
    transformedRangePairs, rangeTxErr = tx.Range([]("hello"), []("world"), rangeOpts)
    if rangeTxErr != nil { return rangeTxErr }

    return nil
  })

  if rangeErr != nil { panic(rangeErr.Error()) }

  // get a set of ordered iterated key value pairs with transformed values
  var transformedIterPairs []*mariv2.KeyValuePair
  transformedIterErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
    var iterTxErr error
    transformedIterPairs, iterTxErr = tx.Iterate([]("hello"), 10000, rangeOpts)
    if iterTxErr != nil { return iterTxErr }

    return nil
  })

  if transformedIterErr != nil { panic(iterErr.Error()) }

  // perform a mixed read-write transaction
  var mixedKvPair *mariv2.KeyValuePair
  mixedErr := mariInst.UpdateTx(func(tx *mariv2.Tx) error {
    putTxErr := tx.Put([]byte("key1"), []byte("value1"))
    if putTxErr != nil { return putTxErr }

    putTxErr = tx.Put([]byte("key2"), []byte("value2"))
    if putTxErr != nil { return putTxErr }

    var getTxErr error
    mixedKvPair, getTxErr = tx.Get([]byte("hello"))
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if mixedErr != nil { panic(mixedErr.Error()) }

  // delete a value in mari
  delErr := mariInst.UpdateTx(func(tx *mariv2.Tx) error {
    delTxErr := tx.Delete([]byte("hello"))
    if delTxErr != nil { return delTxErr }

    return nil
  })

  if delErr != nil { panic(delErr.Error()) }
}
```