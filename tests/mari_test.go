package maritests

import (
	"fmt"
	"path/filepath"
	"os"
	"testing"

	"github.com/sirgallo/mariv2"
)

var mariInst *mariv2.Mari


func init() {
	os.Remove(filepath.Join(os.TempDir(), "testmari"))
	os.Remove(filepath.Join(os.TempDir(), "testmaritemp"))

	var initPCMapErr error
	
	opts := mariv2.InitOpts{ Filepath: os.TempDir(), FileName: "testmari" }
	mariInst, initPCMapErr = mariv2.Open(opts)
	if initPCMapErr != nil { panic(initPCMapErr.Error()) }

	fmt.Println("op test mari initialized")
}


func TestMari(t *testing.T) {
	defer mariInst.Remove()

	var delErr, getErr, putErr error
	var val1, val2, val3, val4, val5 *mariv2.KeyValuePair

	t.Run("Test Mari Put", func(t *testing.T) {
		putErr = mariInst.UpdateTx(func(tx *mariv2.Tx) error {
			putErr = tx.Put([]byte("hello"), []byte("world"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("new"), []byte("wow!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("again"), []byte("test!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("woah"), []byte("random entry"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("key"), []byte("Saturday!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("sup"), []byte("6"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("final"), []byte("the!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("asdfasdf"), []byte("add 10"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("asdfasdf"), []byte("123123"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("asd"), []byte("queue!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("fasdf"), []byte("interesting"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("yup"), []byte("random again!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("asdf"), []byte("hello"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("asdffasd"), []byte("uh oh!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("fasdfasdfasdfasdf"), []byte("error message"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("fasdfasdf"), []byte("info!"))
			if putErr != nil { return putErr }
	
			putErr = tx.Put([]byte("woah"), []byte("done"))
			if putErr != nil { return putErr }

			putErr = tx.Put([]byte("Woah"), []byte("done"))
			if putErr != nil { return putErr }
			
			return nil
		})

		if putErr != nil { t.Errorf("error on udpate tx: %s\n", putErr.Error())}

		t.Logf("mariInst after inserts")
		mariInst.PrintChildren()
	})

	t.Run("Test Mari Get", func(t *testing.T) {
		getErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
			expVal1 := "world"
			val1, getErr = tx.Get([]byte("hello"), nil)
			if getErr != nil { return getErr }
			if val1 == nil { t.Error("val actually nil") }
	
			t.Logf("actual: %s, expected: %s", string(val1.Value), expVal1)
			if string(val1.Value) != expVal1 { t.Errorf("val 1 does not match expected val 1: actual(%s), expected(%s)\n", val1.Value, expVal1) }
	
			expVal2 := "wow!"
			val2, getErr = tx.Get([]byte("new"), nil)
			if getErr != nil { return getErr }
			if val2 == nil { t.Error("val actually nil") }
	
			t.Logf("actual: %s, expected: %s", val2.Value, expVal2)
			if string(val2.Value) != expVal2 { t.Errorf("val 2 does not match expected val 2: actual(%s), expected(%s)\n", val2.Value, expVal2) }
	
			expVal3 := "hello"
			val3, getErr = tx.Get([]byte("asdf"), nil)
			if getErr != nil { return getErr }
			if val3 == nil { t.Error("val actually nil") }
			
			t.Logf("actual: %s, expected: %s", val3.Value, expVal3)
			if string(val3.Value) != expVal3 { t.Errorf("val 3 does not match expected val 3: actual(%s), expected(%s)", val3.Value, expVal3) }
	
			expVal4 := "123123"
			val4, getErr = tx.Get([]byte("asdfasdf"), nil)
			if getErr != nil { return getErr }
			if val4 == nil { t.Error("val actually nil") }
	
			t.Logf("actual: %s, expected: %s", val4.Value, expVal4)
			if string(val4.Value) != expVal4 { t.Errorf("val 4 does not match expected val 4: actual(%s), expected(%s)", val4.Value, expVal4) }

			expVal5 := "done"
			val5, getErr = tx.Get([]byte("Woah"), nil)
			if getErr != nil { return getErr }
			if val5 == nil { t.Error("val actually nil") }
	
			t.Logf("actual: %s, expected: %s", val5.Value, expVal5)
			if string(val5.Value) != expVal5 { t.Errorf("val 4 does not match expected val 4: actual(%s), expected(%s)", val5.Value, expVal5) }
			
			return nil
		})

		if getErr != nil { t.Errorf("error getting val: %s", getErr.Error()) }
	})

	t.Run("Test Iterate Operation", func(t *testing.T) {
		var kvPairs []*mariv2.KeyValuePair

		iterErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
			var txIterErr error
			kvPairs, txIterErr = tx.Iterate([]byte("hello"), 3, nil)
			if txIterErr != nil { return txIterErr }

			return nil
		})

		if iterErr != nil { t.Errorf("error on mari range: %s", iterErr.Error()) }

		t.Log("keys in kv pairs", func() []string{
			var keys []string
			for _, kv := range kvPairs { 
				keys = append(keys, string(kv.Key))
			}

			return keys
		}())

		isSorted := IsSorted(kvPairs)
		t.Logf("is sorted: %t", isSorted)

		if ! isSorted {
			t.Errorf("key value pairs are not in sorted order: %t", isSorted)
		}
	})

	t.Run("Test Range Operation", func(t *testing.T) {
		var kvPairs []*mariv2.KeyValuePair

		rangeErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
			var txRangeErr error
			kvPairs, txRangeErr = tx.Range([]byte("hello"), []byte("yup"), nil)
			if txRangeErr != nil { return txRangeErr }
			return nil
		})

		if rangeErr != nil { t.Errorf("error on mari range: %s", rangeErr.Error()) }

		t.Log("keys in kv pairs", func() []string{
			var keys []string
			for _, kv := range kvPairs { 
				keys = append(keys, string(kv.Key))
			}

			return keys
		}())

		isSorted := IsSorted(kvPairs)
		t.Logf("is sorted: %t", isSorted)

		if ! isSorted {
			t.Errorf("key value pairs are not in sorted order: %t", isSorted)
		}
	})

	t.Run("Test Transform on Iterate Operation", func(t *testing.T) {
		var kvPairs []*mariv2.KeyValuePair

		transform := func(kvPair *mariv2.KeyValuePair) *mariv2.KeyValuePair {
			kvPair.Value = append(kvPair.Value, kvPair.Value...)
			return kvPair
		}

		opts := &mariv2.RangeOpts{ Transform: &transform }
		iterErr := mariInst.ReadTx(func(tx *mariv2.Tx) error {
			var txIterErr error
			kvPairs, txIterErr = tx.Iterate([]byte("hello"), 3, opts)
			if txIterErr != nil { return txIterErr }
			return nil
		})

		if iterErr != nil { t.Errorf("error on mari range: %s", iterErr.Error()) }

		t.Log("keys in kv pairs:", func() []string{
			var keys []string
			for _, kv := range kvPairs { 
				keys = append(keys, string(kv.Key))
			}

			return keys
		}(), "transformed values in kv pairs:", func() []string{
			var values []string
			for _, kv := range kvPairs { 
				values = append(values, string(kv.Value))
			}

			return values
		}())

		isSorted := IsSorted(kvPairs)
		t.Logf("is sorted: %t", isSorted)

		if ! isSorted {
			t.Errorf("key value pairs are not in sorted order: %t", isSorted)
		}
	})

	t.Run("Test Mari Delete", func(t *testing.T) {
		delErr = mariInst.UpdateTx(func(tx *mariv2.Tx) error {
			delTxErr := tx.Delete([]byte("hello"))
			if delTxErr != nil { return delTxErr }
	
			delTxErr = tx.Delete([]byte("yup"))
			if delTxErr != nil { return delTxErr }
	
			delTxErr = tx.Delete([]byte("asdf"))
			if delTxErr != nil { return delTxErr }
	
			delTxErr = tx.Delete([]byte("asdfasdf"))
			if delTxErr != nil { return delTxErr }
	
			delTxErr = tx.Delete([]byte("new"))
			if delTxErr != nil { return delTxErr }
			
			return nil
		})

		if delErr != nil { t.Errorf("error deleting key from mari: %s", delErr.Error()) }

		t.Log("mari after deletes")
		mariInst.PrintChildren()
	})

	t.Log("Done")
}