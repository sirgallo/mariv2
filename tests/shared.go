package maritests

import (
	"bytes"
	"crypto/rand"
	"errors"
	mrand "math/rand"

	"github.com/sirgallo/mariv2"
)


const NUM_WRITER_GO_ROUTINES = 2
const NUM_READER_GO_ROUTINES = 10
const NUM_RANGE_GO_ROUTINES = 10
const INPUT_SIZE = 4000000
const ITERATE_SIZE = 500000
const TRANSACTION_CHUNK_SIZE = 10000
const PWRITE_INPUT_SIZE = INPUT_SIZE / 5
const WRITE_CHUNK_SIZE = INPUT_SIZE / NUM_WRITER_GO_ROUTINES
const READ_CHUNK_SIZE = INPUT_SIZE / NUM_READER_GO_ROUTINES
const PCHUNK_SIZE_READ = (INPUT_SIZE - PWRITE_INPUT_SIZE) / NUM_READER_GO_ROUTINES
const PCHUNK_SIZE_WRITE = PWRITE_INPUT_SIZE / NUM_WRITER_GO_ROUTINES


type KeyVal struct {
	Key   []byte
	Value []byte
}


func GenerateRandomBytes(length int) ([]byte, error) {
 	byteMapping := func(b byte) byte {
		const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		return charset[b%62]
	}

	if length <= 0 { return nil, errors.New("length must be greater than 0") }

	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil { return nil, err }

	for i := 0; i < length; i++ {
		randomBytes[i] = byteMapping(randomBytes[i])
	}

	return randomBytes, nil
}

func TwoRandomDistinctValues(min, max int) (int, int, error) {
	if min >= max { return 0, 0, errors.New("min cannot be greater than max") }

	first := mrand.Intn(max - min) + min
	var second int
	for {
		second = mrand.Intn(max - min) + min
		if second != first { break }
	}

	return first, second, nil
}

func IsSorted(s []*mariv2.KeyValuePair) bool {
	for i := 1; i < len(s); i++ {
		if bytes.Compare(s[i - 1].Key, s[i].Key) > 0 { return false }
	}

	return true
}

func Chunk (array[]KeyVal, chunkSize int) ([][]KeyVal, error) {
	if chunkSize <= 0 { return nil, errors.New("chunk size needs to be greater than 0") }
	
	var chunks [][]KeyVal
	
	if (len(array) <= chunkSize) { 
		return append(chunks, array), nil
	} else {
		totalChunks := (len(array) / chunkSize) + 1

		for idx := range make([]int, totalChunks - 1) {
			start := idx * chunkSize
			end := (idx + 1) * chunkSize
			chunks = append(chunks, array[start:end])
		}

		startOfRemainder := (totalChunks - 1) * chunkSize
		
		if startOfRemainder < len(array) { return append(chunks, array[startOfRemainder:]), nil }
		return chunks, nil
	} 
}