# compaction


## issue

Due to the design decision to use an append only memory map, there are a couple of downsides. While the append only nature is great for concurrency, the file will infinitely grow and the elements in the concurrent ordered array mapped trie will be spread across the memory map. Not only this, but as the file grows, the amount of unused nodes increases, resulting in wasted space.


## solution

A compaction strategy is implemented, where when triggered, will create a snapshot of the current state of the `mari` instance. 

A separate go routine runs compaction, and on signal, acquires the write lock, blocking all subsequent reads and writes. A tempory memory mapped file is created and dynamically resized as new elements are appended.

The overall time complexity is essentially `O(n * m)`, where `n` is the number of nodes that are being copied in the snapshot and `m` is the number of levels to a key. However, since the structure utilizes compact paths and hence is relatively shallow, this can be amortized to roughly `O(n)`. The operation, which is essentially a cursor, begins at the root of the trie and for each child in the node's child array, scans from least ordered to greatest ordered. As the cursor traverses each level, the next start offset for each node is computed. Once no child nodes are found, the offset is applied and the child is serialized and placed directly at the computed offset in the new memory mapped based off of the new root offset. Then, the operation travels back up the branch, serializing each node as it passes back up to the root, with the end offset of each child being serialized as a pointer into the parent node. The serialized, flattened structure will look as the following:
```
                                  root
level 1                 node 0   node 1    node 2
level 2           node 0    node 1
level 3      node 0 node 1 node 2 node 3

root | level 1 node 0 | level 2 node 0 | level 3 node 0 | level 3 node 1 | level 2 node 1 | level 3 node 2 | level 3 node 3 | level 1 node 1 | level 1 node 2
```

A benefit of compaction is that there will no longer be duplicated paths for different version, reducing overall size of the structure and reducing the space that an operation may need to travel along the memory map to find a node. For iterators and range operations, nodes will be more localized as well reducing the need to load and evict data from the system cache.


## custom triggers

A custom compact trigger can be passed in the mari options when first initializing the instance. The function has the following signature:
```go
compactTrigger := func(metaData *mari.MariMetaData) bool
```

When truthy value is returned, the compact trigger will attempt to signal the compaction go routine. If a process is already compacting the instance, the operation skips. Once the signal has been sent to the channel, the operation returns without writing to the instance and waits until the compaction process is complete to continue.

If a compaction strategy is not defined, then a default is used, where the instance will compact based on when a certain number of versions has been written.


## usage

```go
package main

import "os"
import "path/filepath"

import "github.com/sirgallo/mariv2"


const FILENAME = "<your-file-name>"


func main() {
  homedir, homedirErr := os.UserHomeDir()
  if homedirErr != nil { panic(homedirErr.Error()) }
  
  compactTrigger := func(metaData *mariv2.MetaData) bool {
    return metaData.version >= 1000000
  }

  opts := mariv2.InitOpts{ 
    Filepath: homedir,
    FileName: FILENAME,
    CompactTrigger: &compactTrigger,
  }

  mariInst, openErr := mariv2.Open(opts)
  if openErr != nil { panic(openErr.Error()) }
  defer mariInst.Close()
}
```


## what about batched writes?

When writes are batched in transactions, not just a single path is copied and serialized, but the structure for the entire insert set is built in memory, where all paths are copied onto the same version. When serialized, these batched writes mimic the same above structure. Due to this, batch writes are much more space efficient than single writes and reduce duplicate path copies with different versions in the memory map, so it is suggested that writes should be batched as transactions over single point inserts.


## NOTES

The compaction process can be avoided all together if required, and an optional field can be passed in the options when initializing the instance. This will become a truly append only data structure, and all versions will exist, creating a truly immuatable data structure. This can be done with the following:
```go
package main

import "os"
import "path/filepath"

import "github.com/sirgallo/mariv2"


const FILENAME = "<your-file-name>"


func main() {
  homedir, homedirErr := os.UserHomeDir()
  if homedirErr != nil { panic(homedirErr.Error()) }

  appendOnly := true
  opts := mariv2.InitOpts{ 
    Filepath: homedir,
    FileName: FILENAME,
    AppendOnly: &appendOnly
  }

  mariInst, openErr := mariv2.Open(opts)
  if openErr != nil { panic(openErr.Error()) }
  defer mariInst.Close()
}
```