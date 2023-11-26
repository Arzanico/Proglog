package log

import (
	"errors"
	"fmt"
	api "github.com/Arzanico/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(apnd)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, apnd.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	var apiErr api.ErrOffsetOutOfRange
	errors.As(err, &apiErr)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, o *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := o.Append(apnd)
		require.NoError(t, err)
	}
	require.NoError(t, o.Close())

	off, err := o.LowestOffSet()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = o.HighestOffSet()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := NewLog(o.Dir, o.Config)
	require.NoError(t, err)

	off, err = n.LowestOffSet()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = n.HighestOffSet()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testTruncate(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(apnd)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}

func testReader(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(apnd)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	fmt.Println(read)
	require.NoError(t, err)
	require.Equal(t, apnd.Value, read.Value)
}
