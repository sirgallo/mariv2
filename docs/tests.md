# tests


## overview

The test suite consists of a few different tests, as follows:

  1. Mari_test - basic test to check operations
  2. Concurrent_test - test the concurrent performance for individual operations
  3. Parallel_test - test the parallel performance of a mixed read/write workload
  4. Transaction_test - test the performance of concurrent, batched transactions
  5. SingleThread_test - test the single threaded performance for individual operations
  6. MMap_test - test the mmap function behind memory mapping the mari file

Constant values can be modified in `Shared` to check performance characteristics of different ratios of readers/writers, total input sizes, etc.

Tests have been run using both `go test` and `go test -race`, to try and catch any race conditions, especially in the concurrent/parallel tests. Use of `-race` will cause tests to run significantly slower since the flag makes the go test tool run a memory profiling tool while the tests execute.


## results

Tests use key value pairs with randomly generated keys and values with `32 byte` length each.

Tested on:
```
Macbook Pro 14-inch, 2023
CPU: M2Pro
Ram: 16GB
HD: 512 GB SSD
```

### concurrent_test
```
compaction at version 1,000,000 
nodepool size of 1,000,000
iteration size of 500,000
2 writers, 10 readers

2,000,000 kv pairs:
  PASS: TestMariConcurrentOperations/Test_Write_Operations (34.47s)
  PASS: TestMariConcurrentOperations/Test_Read_Operations (8.72s)
  PASS: TestMariConcurrentOperations/Test_Read_Operations_After_Reopen (8.57s)
  PASS: TestMariConcurrentOperations/Test_Iterate_Operation (0.50s)
  PASS: TestMariConcurrentOperations/Test_Range_Operation (1.15s)
  PASS: TestMariConcurrentOperations/Test_Delete_Operations (33.53s)

4,000,000 kv pairs:
  PASS: TestMariConcurrentOperations/Test_Write_Operations (74.81s)
  PASS: TestMariConcurrentOperations/Test_Read_Operations (17.99s)
  PASS: TestMariConcurrentOperations/Test_Read_Operations_After_Reopen (17.73s)
  PASS: TestMariConcurrentOperations/Test_Iterate_Operation (0.49s)
  PASS: TestMariConcurrentOperations/Test_Range_Operation (1.53s)
  PASS: TestMariConcurrentOperations/Test_Delete_Operations (76.80s)

10,000,000 kv pairs:
  PASS: TestMariConcurrentOperations/Test_Write_Operations (221.65s)
  PASS: TestMariConcurrentOperations/Test_Read_Operations (47.10s)
  PASS: TestMariConcurrentOperations/Test_Read_Operations_After_Reopen (48.85s)
  PASS: TestMariConcurrentOperations/Test_Iterate_Operation (0.44s)
  PASS: TestMariConcurrentOperations/Test_Range_Operation (4.01s)
  PASS: TestMariConcurrentOperations/Test_Delete_Operations (225.69s)
```


### parallel_test
```
compaction at version 1,000,000 
nodepool size of 1,000,000
2 writers, 10 readers

2,000,000 kv pairs initial seed, 2,000,000 kv pairs read, 400,000 new kv pairs written:
  PASS: TestMariParallelReadWrites/Test_Read_Init_Key_Vals_In_MMap (8.04s)
  PASS: TestMariParallelReadWrites/Test_Write_New_Key_Vals_In_MMap (13.07s)

4,000,000 kv pairs initial seed, 4,000,000 kv pairs read, 800,000 new kv pairs written:
  PASS: TestMariParallelReadWrites/Test_Read_Init_Key_Vals_In_MMap (16.98s)
  PASS: TestMariParallelReadWrites/Test_Write_New_Key_Vals_In_MMap (27.51s)
```


### transaction_test
```
transaction size of 10,000 kv pair entries
compaction at version 1,000,000 
nodepool size of 1,000,000
iteration size of 500,000
2 writers, 10 readers

2,000,000 kv pairs:
  PASS: TestMariTransactionOperations/Test_Write_Operations (12.52s)
  PASS: TestMariTransactionOperations/Test_Read_Operations (8.66s)
  PASS: TestMariTransactionOperations/Test_Read_Operations_After_Reopen (8.99s)
  PASS: TestMariTransactionOperations/Test_Batched_Read_Operations (6.09s)
  PASS: TestMariTransactionOperations/Test_Iterate_Operation (0.58s)
  PASS: TestMariTransactionOperations/Test_Delete_Operations (11.21s)

4,000,000 kv pairs:
  PASS: TestMariTransactionOperations/Test_Write_Operations (27.24s)
  PASS: TestMariTransactionOperations/Test_Read_Operations (18.73s)
  PASS: TestMariTransactionOperations/Test_Read_Operations_After_Reopen (18.61s)
  PASS: TestMariTransactionOperations/Test_Batched_Read_Operations (12.71s)
  PASS: TestMariTransactionOperations/Test_Iterate_Operation (0.52s)
  PASS: TestMariTransactionOperations/Test_Delete_Operations (22.16s)

10,000,000 kv pairs:
  PASS: TestMariTransactionOperations/Test_Write_Operations (76.21s)
  PASS: TestMariTransactionOperations/Test_Read_Operations (46.60s)
  PASS: TestMariTransactionOperations/Test_Read_Operations_After_Reopen (48.14s)
  PASS: TestMariTransactionOperations/Test_Batched_Read_Operations (33.85s)
  PASS: TestMariTransactionOperations/Test_Iterate_Operation (0.54s)
  PASS: TestMariTransactionOperations/Test_Delete_Operations (78.78s)

30,000,000 kv pairs:
  PASS: TestMariTransactionOperations/Test_Write_Operations (834.34s)
  PASS: TestMariTransactionOperations/Test_Read_Operations (562.32s) 
  PASS: TestMariTransactionOperations/Test_Read_Operations_After_Reopen (578.96s)
  PASS: TestMariTransactionOperations/Test_Batched_Read_Operations (567.77s)
  PASS: TestMariTransactionOperations/Test_Iterate_Operation (4.79s)
  PASS: TestMariTransactionOperations/Test_Delete_Operations (1817.45s)
```

### singlethread_test
```
compaction at version 1,000,000 
nodepool size of 1,000,000
iteration size of 500,000
1 writer, 1 reader

2,000,000 kv pairs:
  PASS: TestMariSingleThreadOperations/Test_Write_Operations (28.60s)
  PASS: TestMariSingleThreadOperations/Test_Read_Operations (15.56s)
  PASS: TestMariSingleThreadOperations/Test_Read_Operations_After_Reopen (15.69s)
  PASS: TestMariSingleThreadOperations/Test_Iterate_Operation (1.46s)
  PASS: TestMariSingleThreadOperations/Test_Range_Operation (1.75s)
  PASS: TestMariSingleThreadOperations/Test_Delete_Operations (29.88s)

4,000,000 kv pairs:
  PASS: TestMariSingleThreadOperations/Test_Write_Operations (61.23s)
  PASS: TestMariSingleThreadOperations/Test_Read_Operations (31.36s)
  PASS: TestMariSingleThreadOperations/Test_Read_Operations_After_Reopen (31.92s)
  PASS: TestMariSingleThreadOperations/Test_Iterate_Operation (1.36s)
  PASS: TestMariSingleThreadOperations/Test_Range_Operation (4.35s)
  PASS: TestMariSingleThreadOperations/Test_Delete_Operations (63.34s)

10,000,000 kv pairs:
  PASS: TestMariSingleThreadOperations/Test_Write_Operations (180.41s)
  PASS: TestMariSingleThreadOperations/Test_Read_Operations (81.20s)
  PASS: TestMariSingleThreadOperations/Test_Read_Operations_After_Reopen (82.14s)
  PASS: TestMariSingleThreadOperations/Test_Iterate_Operation (1.25s)
  PASS: TestMariSingleThreadOperations/Test_Range_Operation (6.67s)
  PASS: TestMariSingleThreadOperations/Test_Delete_Operations (197.02s)
```


## afterthoughts

While having multiple writers will not cause race conditions or blocking of operations, adding more writers appears to have a dimishing return on performance. This is most likely due to the nature of retries with atomic operations. If contention is high and multiple threads are competing to append to the instance, the retry rate will increase. The data structure is already highly efficient for a single writer since the depth of branches will be relatively low and inserts are not very costly, where each path will be around 3-10 levels depending on the data size and structure of key-value pairs. Optimizations can be made for inserts. More compact serialization is being explored and lazily expanding the root node to be more than 256 children can also make the size and depth of the structure even smaller. The goal is to make the structure as flat as possible to limit traversing long paths. Compact paths have already been implemented which saw a significant uplift in performance. Tuning the compaction strategy can also play a major role in operating on the data, as well as batching writes together. The increase in performace for writes with batching can be seen in the `Transaction_test`, especially. This results in less "write ampliflication". For more info, look at [Compaction](./Compaction.md).

Multiple readers does see a major uplift, with performance gains per go routine added being almost logarithmic in nature. Purely adding more go routines will have diminishing returns due to the increase in context switching as more threads compete for system resources.

The usage of `MVCC` shows that readers and writers in mixed workloads do not cause performance issues for either since both types of operations are not competing for the same system resources.

The `Iterator` and `Range` operations are also highly efficient, getting much higher throughput than sequential individual `Get` operations, so it is recommended that if a scan of data is required to use either operation instead of sequential `Get`.

Note, these tests have not been performed on a traditional HDD, only on SSD. The performance can be expected to degrade significantly on HDD, so it is recommended to run using a machine with a fast SSD.