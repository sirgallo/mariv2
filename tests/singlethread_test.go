package maritests

import (
	"bytes"
	"fmt"
	"path/filepath"
	"os"
	"sync/atomic"
	"testing"

	"github.com/sirgallo/mariv2"
)


var singleThreadTestMap *mariv2.Mari
var stkeyValPairs []KeyVal
var stInitMariErr error


func init() {	
	os.Remove(filepath.Join(os.TempDir(), "testst"))
	os.Remove(filepath.Join(os.TempDir(), "teststtemp"))

	opts := mariv2.InitOpts{ Filepath: os.TempDir(), FileName: "testst" }
	singleThreadTestMap, stInitMariErr = mariv2.Open(opts)
	if stInitMariErr != nil {
		singleThreadTestMap.Remove()
		panic(stInitMariErr.Error())
	}

	fmt.Println("single thread test mari initialized")
	stkeyValPairs = make([]KeyVal, INPUT_SIZE)

	for idx := range stkeyValPairs {
		randomBytes, _ := GenerateRandomBytes(32)
		stkeyValPairs[idx] = KeyVal{ Key: randomBytes, Value: randomBytes }
	}
}


func TestMariSingleThreadOperations(t *testing.T) {
	defer singleThreadTestMap.Remove()

	t.Run("Test Write Operations", func(t *testing.T) {
		for _, val := range stkeyValPairs {
			putErr := singleThreadTestMap.UpdateTx(func(tx *mariv2.Tx) error {
				putTxErr := tx.Put(val.Key, val.Value)
				if putTxErr != nil { return putTxErr }
				return nil
			})

			if putErr != nil { t.Errorf("error on mari put: %s", putErr.Error()) }
		}
	})

	t.Run("Test Read Operations", func(t *testing.T) {
		defer singleThreadTestMap.Close()
		
		for _, val := range stkeyValPairs {
			var kvPair *mariv2.KeyValuePair
			getErr := singleThreadTestMap.ReadTx(func(tx *mariv2.Tx) error {
				var getTxErr error
				kvPair, getTxErr = tx.Get(val.Key, nil)
				if getTxErr != nil { return getTxErr }
				return nil
			})

			if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }
			
			if ! bytes.Equal(kvPair.Value, val.Value) {
				t.Errorf("actual value not equal to expected: actual(%v), expected(%v)", kvPair, val)
			}
		}
	})

	t.Run("Test Read Operations After Reopen", func(t *testing.T) {
		opts := mariv2.InitOpts{ Filepath: os.TempDir(), FileName: "testst" }
		
		singleThreadTestMap, stInitMariErr = mariv2.Open(opts)
		if stInitMariErr != nil {
			singleThreadTestMap.Remove()
			t.Error("unable to open file")
		}

		for _, val := range stkeyValPairs {
			var kvPair *mariv2.KeyValuePair
			getErr := singleThreadTestMap.ReadTx(func(tx *mariv2.Tx) error {
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
	})

	t.Run("Test Iterate Operation", func(t *testing.T) {
		totalElements := uint64(0)

		for range make([]int, NUM_READER_GO_ROUTINES) {
			first, _, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
			if randomErr != nil { t.Error("error generating random min max") }
	
			start := stkeyValPairs[first].Key
	
			var kvPairs []*mariv2.KeyValuePair
			iterErr := singleThreadTestMap.ReadTx(func(tx *mariv2.Tx) error {
				var iterTxErr error
				kvPairs, iterTxErr = tx.Iterate(start, ITERATE_SIZE, nil)
				if iterTxErr != nil { return iterTxErr }
	
				return nil
			})
	
			if iterErr != nil { t.Errorf("error on mari get: %s", iterErr.Error()) }
			
			atomic.AddUint64(&totalElements, uint64(len(kvPairs)))
			isSorted := IsSorted(kvPairs)
			if ! isSorted { t.Errorf("key value pairs are not in sorted order: %t", isSorted) }
		}

		t.Log("total elements returned on iterate:", totalElements)
	})

	t.Run("Test Range Operation", func(t *testing.T) {
		totalElements := uint64(0)

		for range make([]int, NUM_RANGE_GO_ROUTINES) {
			first, second, randomErr := TwoRandomDistinctValues(0, INPUT_SIZE)
			if randomErr != nil { t.Error("error generating random min max") }

			var start, end []byte
			switch {
				case bytes.Compare(stkeyValPairs[first].Key, stkeyValPairs[second].Key) == 1:
					start = stkeyValPairs[second].Key
					end = stkeyValPairs[first].Key
				default:
					start = stkeyValPairs[first].Key
					end = stkeyValPairs[second].Key
			}

			var kvPairs []*mariv2.KeyValuePair
			rangeErr := singleThreadTestMap.ReadTx(func(tx *mariv2.Tx) error {
				var rangeTxErr error
				kvPairs, rangeTxErr = tx.Range(start, end, nil)
				if rangeTxErr != nil { return rangeTxErr }
				return nil
			})

			if rangeErr != nil { t.Errorf("error on mari get: %s", rangeErr.Error()) }
			
			atomic.AddUint64(&totalElements, uint64(len(kvPairs)))
			isSorted := IsSorted(kvPairs)
			if ! isSorted { t.Errorf("key value pairs are not in sorted order: %t", isSorted) }
		}

		t.Log("total elements returned on range:", totalElements)
	})

	t.Run("Test Delete Operations", func(t *testing.T) {
		for _, val := range stkeyValPairs {
			delErr := singleThreadTestMap.UpdateTx(func(tx *mariv2.Tx) error {
				delTxErr := tx.Delete(val.Key)
				if delTxErr != nil { return delTxErr }
				return nil
			})

			if delErr != nil { t.Errorf("error on mari delete: %s", delErr.Error()) }
		}
	})

	t.Run("Mari File Size", func(t *testing.T) {
		fSize, sizeErr := singleThreadTestMap.FileSize()
		if sizeErr != nil { t.Errorf("error getting file size: %s", sizeErr.Error()) }
		t.Log("File Size In Bytes:", fSize)
	})

	stkeyValPairs = nil
	t.Log("Done")
}