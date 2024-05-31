package maritests

import (
	"bytes"
	"fmt"
	"path/filepath"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirgallo/mariv2"
)


var txMariInst *mariv2.Mari
var txKeyValPairs []KeyVal
var txInitMariErr error
var txDelWG, txInsertWG, txIterWG, txRetrieveWG sync.WaitGroup


func init() {
	os.Remove(filepath.Join(os.TempDir(), "testtransaction"))
	os.Remove(filepath.Join(os.TempDir(), "testtransactiontemp"))

	opts := mariv2.MariOpts{ Filepath: os.TempDir(), FileName: "testtransaction" }

	txMariInst, txInitMariErr = mariv2.Open(opts)
	if txInitMariErr != nil {
		txMariInst.Remove()
		panic(txInitMariErr.Error())
	}

	fmt.Println("transaction test mari initialized")

	txKeyValPairs = make([]KeyVal, INPUT_SIZE)

	for idx := range txKeyValPairs {
		randomBytes, _ := GenerateRandomBytes(32)
		txKeyValPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
	}
}


func TestMariTransactionOperations(t *testing.T) {
	defer txMariInst.Remove()

	t.Run("Test Write Operations", func(t *testing.T) {
		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			kvPairsForWriter := txKeyValPairs[i * WRITE_CHUNK_SIZE:(i + 1) * WRITE_CHUNK_SIZE]

			chunks, chunkErr := Chunk(kvPairsForWriter, TRANSACTION_CHUNK_SIZE)
			if chunkErr != nil { t.Errorf("error chunking kvPairs sub slice: %s", chunkErr.Error()) }

			txInsertWG.Add(1)
			go func () {
				defer txInsertWG.Done()
				for _, chunk := range chunks {
					putErr := txMariInst.UpdateTx(func(tx *mariv2.MariTx) error {
						for _, kvPair := range chunk {
							putTxErr := tx.Put(kvPair.Key, kvPair.Value)
							if putTxErr != nil { return putTxErr }
						}

						return nil
					})
					
					if putErr != nil { t.Errorf("error on mari put: %s", putErr.Error()) }
				}
			}()
		}

		txInsertWG.Wait()
	})

	t.Run("Test Read Operations", func(t *testing.T) {
		defer txMariInst.Close()

		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := txKeyValPairs[i * READ_CHUNK_SIZE:(i + 1) * READ_CHUNK_SIZE]
			
			txRetrieveWG.Add(1)
			go func() {
				defer txRetrieveWG.Done()
				for _, val := range chunk {
					var kvPair *mariv2.KeyValuePair
					getErr := txMariInst.ReadTx(func(tx *mariv2.MariTx) error {
						var getTxErr error
						kvPair, getTxErr = tx.Get(val.Key, nil)
						if getTxErr != nil { return getTxErr }

						return nil
					})
					
					if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }

					if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
					}
				}
			}()
		}

		txRetrieveWG.Wait()
	})

	t.Run("Test Read Operations After Reopen", func(t *testing.T) {
		opts := mariv2.MariOpts{ Filepath: os.TempDir(), FileName: "testtransaction" }
		
		txMariInst, txInitMariErr = mariv2.Open(opts)
		if txInitMariErr != nil {
			txMariInst.Remove()
			t.Error("unable to open file")
		}

		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := txKeyValPairs[i * READ_CHUNK_SIZE:(i + 1) * READ_CHUNK_SIZE]

			txRetrieveWG.Add(1)
			go func() {
				defer txRetrieveWG.Done()
				for _, val := range chunk {
					var kvPair *mariv2.KeyValuePair
					getErr := txMariInst.ReadTx(func(tx *mariv2.MariTx) error {
						var getTxErr error
						kvPair, getTxErr = tx.Get(val.Key, nil)
						if getTxErr != nil { return getTxErr }

						return nil
					})
					
					if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }

					if ! bytes.Equal(kvPair.Key, val.Key) || ! bytes.Equal(kvPair.Value, val.Value) {
						t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
					}
				}
			}()
		}

		txRetrieveWG.Wait()
	})

	t.Run("Test Batched Read Operations", func(t *testing.T) {
		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			kvPairsForReader := txKeyValPairs[i * READ_CHUNK_SIZE:(i + 1) * READ_CHUNK_SIZE]
			
			chunks, chunkErr := Chunk(kvPairsForReader, TRANSACTION_CHUNK_SIZE)
			if chunkErr != nil { t.Errorf("error chunking kvPairs sub slice: %s", chunkErr.Error()) }

			txRetrieveWG.Add(1)
			go func() {
				defer txRetrieveWG.Done()
				for _, chunk := range chunks {
					getErr := txMariInst.ReadTx(func(tx *mariv2.MariTx) error {
						for _, kv := range chunk {
							kvPair, getTxErr := tx.Get(kv.Key, nil)
							if getTxErr != nil { return getTxErr }

							if ! bytes.Equal(kvPair.Key, kv.Key) || ! bytes.Equal(kvPair.Value, kv.Value) {
								t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, kv)
							}
						}

						return nil
					})
					
					if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }
				}
			}()
		}

		txRetrieveWG.Wait()
	})

	t.Run("Test Iterate Operation", func(t *testing.T) {
		totalElements := uint64(0)

		for range make([]int, NUM_READER_GO_ROUTINES) {
			first, _, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
			if randomErr != nil { t.Error("error generating random min max") }
	
			start := txKeyValPairs[first].Key

			txIterWG.Add(1)
			go func() {
				defer txIterWG.Done()

				var kvPairs []*mariv2.KeyValuePair
				iterErr := txMariInst.ReadTx(func(tx *mariv2.MariTx) error {
					var iterTxErr error
					kvPairs, iterTxErr = tx.Iterate(start, ITERATE_SIZE, nil)
					if iterTxErr != nil { return iterTxErr }

					return nil
				})

				if iterErr != nil { t.Errorf("error on mari get: %s", iterErr.Error()) }
				
				atomic.AddUint64(&totalElements, uint64(len(kvPairs)))

				isSorted := IsSorted(kvPairs)
				if ! isSorted { t.Errorf("key value pairs are not in sorted order: %t", isSorted) }
			}()
		}

		txIterWG.Wait()

		t.Log("total elements returned on iterate:", totalElements)
	})

	t.Run("Test Mixed Operation", func(t *testing.T) {
		txMixedKvPairs := make([]KeyVal, TRANSACTION_CHUNK_SIZE)

		first, _, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
		if randomErr != nil { t.Error("error generating random min max") }

		start := txKeyValPairs[first].Key

		for idx := range txMixedKvPairs {
			randomBytes, _ := GenerateRandomBytes(32)
			txMixedKvPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
		}

		var kvPairs []*mariv2.KeyValuePair
		var kvPair *mariv2.KeyValuePair

		mixedErr := txMariInst.UpdateTx(func(tx *mariv2.MariTx) error {
			for _, newKV := range txMixedKvPairs {
				putTxErr := tx.Put(newKV.Key, newKV.Value)
				if putTxErr != nil { return putTxErr }
			}

			var getTxErr error
			kvPair, getTxErr = tx.Get(start, nil)
			if getTxErr != nil { return getTxErr }

			var iterTxErr error
			kvPairs, iterTxErr = tx.Iterate(start, 10000, nil)
			if iterTxErr != nil { return iterTxErr }

			return nil
		})

		if mixedErr != nil { t.Errorf("error on mari tx mixed: %s", mixedErr.Error()) }
		
		isSorted := IsSorted(kvPairs)
		if ! isSorted { t.Errorf("key value pairs are not in sorted order: %t", isSorted) }

		if ! bytes.Equal(kvPair.Key, start) {
			t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, start)
		}
	})

	t.Run("Test Delete Operations", func(t *testing.T) {
		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			kvPairsForWriter := txKeyValPairs[i * WRITE_CHUNK_SIZE:(i + 1) * WRITE_CHUNK_SIZE]

			chunks, chunkErr := Chunk(kvPairsForWriter, TRANSACTION_CHUNK_SIZE)
			if chunkErr != nil { t.Errorf("error chunking kvPairs sub slice: %s", chunkErr.Error()) }

			txDelWG.Add(1)
			go func() {
				defer txDelWG.Done()
				for _, chunk := range chunks {
					delErr := txMariInst.UpdateTx(func(tx *mariv2.MariTx) error {
						for _, kvPair := range chunk {
							delTxErr := tx.Delete(kvPair.Key)
							if delTxErr != nil { return delTxErr }
						}

						return nil
					})

					if delErr != nil { t.Errorf("error on mari delete: %s", delErr.Error()) }
				}
			}()
		}

		txDelWG.Wait()
	})

	t.Run("Mari File Size", func(t *testing.T) {
		fSize, sizeErr := txMariInst.FileSize()
		if sizeErr != nil { t.Errorf("error getting file size: %s", sizeErr.Error()) }

		t.Log("File Size In Bytes:", fSize)
	})

	txKeyValPairs = nil
	t.Log("Done")
}