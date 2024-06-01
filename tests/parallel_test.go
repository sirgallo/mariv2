package maritests

import (
	"bytes"
	"fmt"
	"path/filepath"
	"os"
	"sync"
	"testing"

	"github.com/sirgallo/mariv2"
)


var parallelMariInst *mariv2.Mari
var initKeyValPairs []KeyVal
var pKeyValPairs []KeyVal
var pInitMariErr error
var pInitWG, pInsertWG, pRetrieveWG sync.WaitGroup


func setup() {
	os.Remove(filepath.Join(os.TempDir(), "testparallel"))
	os.Remove(filepath.Join(os.TempDir(), "testparalleltemp"))

	opts := mariv2.InitOpts{ Filepath: os.TempDir(), FileName: "testparallel" }
	parallelMariInst, pInitMariErr = mariv2.Open(opts)
	if pInitMariErr != nil {
		parallelMariInst.Remove()
		panic(pInitMariErr.Error())
	}

	fmt.Println("parallel test mari initialized")

	initKeyValPairs = make([]KeyVal, INPUT_SIZE)
	pKeyValPairs = make([]KeyVal, PWRITE_INPUT_SIZE)

	for idx := range pKeyValPairs {
		pRandomBytes, _ := GenerateRandomBytes(32)
		pKeyValPairs[idx] = KeyVal{ Key: pRandomBytes, Value: pRandomBytes }
	}

	for idx := range initKeyValPairs {
		iRandomBytes, _ := GenerateRandomBytes(32)
		initKeyValPairs[idx] = KeyVal{ Key: iRandomBytes, Value: iRandomBytes }
	}

	fmt.Println("seeding parallel test mari")

	for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
		kvPairsForWriter := initKeyValPairs[i * WRITE_CHUNK_SIZE:(i + 1) * WRITE_CHUNK_SIZE]

		chunks, chunkErr := Chunk(kvPairsForWriter, TRANSACTION_CHUNK_SIZE)
		if chunkErr != nil { panic("error chunking kvPairs sub slice") }

		pInitWG.Add(1)
		go func () {
			defer pInitWG.Done()
			for _, chunk := range chunks {
				putErr := parallelMariInst.UpdateTx(func(tx *mariv2.Tx) error {
					for _, kvPair := range chunk {
						putTxErr := tx.Put(kvPair.Key, kvPair.Value)
						if putTxErr != nil { return putTxErr }
					}

					return nil
				})
				
				if putErr != nil { panic(putErr.Error()) }
			}
		}()
	}

	pInitWG.Wait()
	fmt.Println("finished seeding parallel test mari")
}

func cleanup() {
	parallelMariInst.Remove()
}


func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	cleanup()

	os.Exit(code)
}

func TestMariParallelReadWrites(t *testing.T) {
	t.Run("Test Read Init Key Vals In MMap", func(t *testing.T) {
		t.Parallel()

		readData := initKeyValPairs[:len(initKeyValPairs) - PWRITE_INPUT_SIZE]

		for i := range make([]int, NUM_READER_GO_ROUTINES) {
			chunk := readData[i * PCHUNK_SIZE_READ:(i + 1) * PCHUNK_SIZE_READ]

			pRetrieveWG.Add(1)
			go func() {
				defer pRetrieveWG.Done()
				for _, kv := range chunk {
					var kvPair *mariv2.KeyValuePair
					getErr := parallelMariInst.ReadTx(func(tx *mariv2.Tx) error {
						var getTxErr error
						kvPair, getTxErr = tx.Get(kv.Key, nil)
						if getTxErr != nil { return getTxErr }

						return nil
					})

					if getErr != nil { t.Errorf("error on mari get: %s", getErr.Error()) }
					if ! bytes.Equal(kvPair.Value, kv.Value) {
						t.Errorf("actual value not equal to expected: actual(%s), expected(%s)", kvPair.Value, kv.Value)
					}
				}
			}()
		}

		pRetrieveWG.Wait()
	})

	t.Run("Test Write New Key Vals In MMap", func(t *testing.T) {
		t.Parallel()

		for i := range make([]int, NUM_WRITER_GO_ROUTINES) {
			chunk := pKeyValPairs[i * PCHUNK_SIZE_WRITE:(i + 1) * PCHUNK_SIZE_WRITE]

			pInsertWG.Add(1)
			go func() {
				defer pInsertWG.Done()
				for _, kv := range chunk {
					putErr := parallelMariInst.UpdateTx(func(tx *mariv2.Tx) error {
						putTxErr := tx.Put(kv.Key, kv.Value)
						if putTxErr != nil { return putTxErr }

						return nil
					})

					if putErr != nil { t.Errorf("error on mari put: %s", putErr.Error()) }
				}
			}()
		}

		pInsertWG.Wait()
		pKeyValPairs = nil
	})
}