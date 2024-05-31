package maritests

import (
	"bytes"
	"io"
	"path/filepath"
	"os"
	"testing"

	"github.com/sirgallo/mariv2"
)


var TestMMapData = []byte("0123456789ABCDEF")
var TestMMapPath = filepath.Join(os.TempDir(), "testfile")


func init() {
	testFile := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	testFile.Write(TestMMapData)
	testFile.Close()
}

func openFile(flags int) *os.File {
	file, openErr := os.OpenFile(TestMMapPath, flags, 0644)
	if openErr != nil { panic(openErr.Error()) }

	return file
}


func TestMMap(t *testing.T) {
	t.Run("Test Unmap", func(t *testing.T) {
		testFile := openFile(os.O_RDONLY)
		defer testFile.Close()
	
		mMap, mmapErr := mariv2.Map(testFile, mariv2.RDONLY, 0)
		if mmapErr != nil { t.Errorf("error mapping: %s", mmapErr) }
	
		unmapErr := mMap.Unmap() 
		if unmapErr != nil { t.Errorf("mmap != testData: %q, %q", mMap, TestMMapData) }
	})

	t.Run("Test Read Write", func(t *testing.T) {
		testFile := openFile(os.O_RDWR)
		defer testFile.Close()
		
		mMap, mmapErr := mariv2.Map(testFile, mariv2.RDWR, 0)
		if mmapErr != nil { t.Errorf("error mapping: %s", mmapErr) }
	
		defer mMap.Unmap()
		
		if ! bytes.Equal(TestMMapData, mMap) { t.Errorf("mmap != testData: %q, %q", mMap, TestMMapData) }
	
		mMap[9] = 'X'
		mMap.Flush()
	
		fileData, err := io.ReadAll(testFile)
		if err != nil { t.Errorf("error reading file: %s", err) }
		if ! bytes.Equal(fileData, []byte("012345678XABCDEF")) { t.Errorf("file wasn't modified") }
	
		mMap[9] = '9'
		mMap.Flush()
	})
}