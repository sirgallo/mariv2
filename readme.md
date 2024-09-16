# mariv2

## a high performance, embedded database written in golang


## mari?

`mari` is named after the ancient Library of Mari, which was located in Mari, Syria around `1900 BC`. The library contained over 22,000 documents at its peak. 


## overview 

`mariv2` is an embedded key-value store that utilzes a memory mapped file to back the contents of the data, implemented purely in `go`.

Unlike other data storage engines that utilize B+ or LSM trees for storing/indexing data, data is stored in an Ordered Array Mapped Trie, which is form of the Hash Array Mapped Trie that allows for ordered iterations and range queries. Data utilizes versioning and is serialized to an append-only data structure containing all versions within the store. Concurrent operations are lock free, so multiple writers and readers can operate on the data in parallel, utilizing a form of `mvcc`.

`mariv2` looks to take the exploratory findings from `mari` and optimize shortcomings, including reducing write amplification and reduced footprint of serialized data.

**currently only tested on unix systems**

The `mmap` function utilizes `golang.org/x/sys/unix`, so the mmap functionality will only work on unix based systems. Builds for other operating systems can be done but have not been explored or implemented yet.


## transactions

Every operation on `mari` is a transaction. Transactions can be either read only (`ReadTx`) or read-write (`UpdateTx`). Write operations will only modify the current version supplied in the transaction and will be isolated from updates to the data. Transforms can be created for read operations to mutate results before being returned to the user. This can be useful for situations where data pre-processing is required. For more information on transactions, check out [transactions](./docs/transactions.md).

Since the trie is ordered, range operations and ordered iterations are supported, which are also concurrent and lock free. Ordered iterations and range operations will also perform better than sequential lookups of singular keys as entire paths do not need to be traversed for each, while a singular key lookup requires full path traversal. If a range of values is required for a lookup, consider using `tx.Iterate` or `tx.Range`.

A compaction strategy can also be implemented as well, which is passed in the instance options using the `CompactTrigger` option. [compaction](./docs/compaction.md) is explained further in depth here.

To alleviate pressure on the `Go` garbage collector, a node pool is also utilized, which is explained here [pool](./docs/pool.md).


## usage

```go
package main

import (
  "os"
  "github.com/sirgallo/mariv2"
)


const FILENAME = "<your-file-name>"

func main() {
  homedir, homedirErr := os.UserHomeDir()
  if homedirErr != nil { panic(homedirErr.Error()) }
  
  opts := mariv2.InitOpts{ Filepath: homedir, FileName: FILENAME }

  mariInst, openErr := mariv2.Open(opts)
  if openErr != nil { panic(openErr.Error()) }
  defer mariInst.Close()

  putErr := mariInst.UpdateTx(func(tx *mariv2.Tx) error {
    putTxErr := tx.Put([]byte("hello"), []byte("world"))
    if putTxErr != nil { return putTxErr }

    return nil
  })

  if putErr != nil { panic(putErr.Error()) }

  var kvPair *mariv2.KeyValuePair
  getErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
    var getTxErr error
    kvPair, getTxErr = tx.Get([]byte("hello"), nil)
    if getTxErr != nil { return getTxErr }

    return nil
  })

  if getErr != nil { panic(getErr.Error()) }

  fSize, sizeErr := mariInst.FileSize()
  if sizeErr != nil { panic(sizeErr.Error()) }

  closeErr := mariInst.Close()
  if closeErr != nil { panic(closeErr.Error()) }

  removeErr := mariInst.Remove()
  if removeErr != nil { panic(removeErr.Error()) }
}
```


## tests

`mariv2`
```bash
go test -v ./tests

# with timeout (s, m, h)
go test -v ./tests -timeout <timeout>

# memory test
go test -v ./tests -bench=. -benchmem
```

Tests are explained further in depth here [test](./docs/tests.md)


## godoc

For in depth definitions of types and functions, `godoc` can generate documentation from the formatted function comments. If `godoc` is not installed, it can be installed with the following:
```bash
go install golang.org/x/tools/cmd/godoc
```

To run the `godoc` server and view definitions for the package:
```bash
godoc -http=:6060
```

Then, in your browser, navigate to:
```
http://localhost:6060/pkg/github.com/sirgallo/mari/
```


## sources

[comap](./docs/comap.md)

[compaction](./docs/compaction.md)

[concepts](./docs/concepts.md)

[pool](./docs/pool.md)

[test](./docs/test.md)

[transactions](./docs/transactions.md)

[vendor](./docs/vendor.md)