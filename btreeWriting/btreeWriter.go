// Package spdb: fast, buffered, length-prefixed binary writer for btree.Item.
//
// Best practice here = encode the record into a reusable []byte buffer,
// then write: [u32 len][record-bytes] in one shot (no "patching" needed).
//
// Record format (little-endian):
//
//	[u32 recordLen]
//	[u32 pk]
//	[u16 fieldCount]
//	repeated fieldCount times:
//	  [u8 nameLen][name bytes]
//	  [u8 tag][value bytes...]
//
// Tags:
//
//	0 nil
//	1 bool        -> [u8 0|1]
//	2 int64       -> [i64]
//	3 float64     -> [f64]
//	4 string      -> [u32 n][n bytes]
//	5 bytes       -> [u32 n][n bytes]
//	6 json        -> [u32 n][n bytes] (fallback)
//
// NOTE: Row is a map => field order is NOT deterministic. If you want deterministic
// field order you must sort keys (costs extra time+memory).
package btreeWriting

import (
	"SpeedyDb/btree"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"
)

const (
	tagNil    = 0
	tagBool   = 1
	tagInt64  = 2
	tagFloat  = 3
	tagString = 4
	tagBytes  = 5
	tagJSON   = 6
)

type Writer struct {
	f  *os.File
	bw *bufio.Writer

	BytesWritten uint64
	Records      uint64

	pool sync.Pool
}

// NewWriter wraps an existing *bufio.Writer and uses an internal buffer pool.
// Use a large bufio.Writer size (e.g. 8–32 MiB) around your file for max throughput.
func NewWriter(f *os.File) *Writer {
	bw := bufio.NewWriterSize(f, 16<<20)
	w := &Writer{f: f, bw: bw}

	w.pool.New = func() any {
		b := make([]byte, 0, 64*1024)
		return &b
	}
	return w
}

// WriteItem encodes and writes one Item as a length-prefixed record.
// This is the "best practice" fast path: encode into pooled buffer -> write once.
func (w *Writer) WriteItem(it btree.Item) error {
	bufp := w.pool.Get().(*[]byte)
	buf := (*bufp)[:0]

	var err error
	buf, err = encodeItemInto(buf, it)
	if err != nil {
		*bufp = buf
		w.pool.Put(bufp)
		return err
	}

	// bytes for this record = 4 (len prefix) + len(buf)
	recBytes := uint64(4 + len(buf))

	if err := writeU32ToWriter(w.bw, uint32(len(buf))); err != nil {
		*bufp = buf
		w.pool.Put(bufp)
		return err
	}
	if _, err := w.bw.Write(buf); err != nil {
		*bufp = buf
		w.pool.Put(bufp)
		return err
	}

	w.BytesWritten += recBytes
	w.Records++

	*bufp = buf
	w.pool.Put(bufp)
	return nil
}

func encodeItemInto(dst []byte, it btree.Item) ([]byte, error) {
	// pk
	dst = appendU32(dst, uint32(it.PK))

	// field count
	if len(it.Row) > math.MaxUint16 {
		return dst, fmt.Errorf("too many fields: %d", len(it.Row))
	}
	dst = appendU16(dst, uint16(len(it.Row)))

	// fields (map iteration order is random)
	for k, v := range it.Row {
		if len(k) > 255 {
			return dst, fmt.Errorf("field name too long (%d): %q", len(k), k)
		}
		dst = append(dst, byte(len(k)))
		dst = append(dst, k...)
		var err error
		dst, err = appendAny(dst, v)
		if err != nil {
			return dst, fmt.Errorf("field %q: %w", k, err)
		}
	}

	return dst, nil
}

func (w *Writer) Flush() error {
	return w.bw.Flush()
}

func (w *Writer) Close() error {
	// flush buffered bytes, then close file
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

func appendAny(dst []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		dst = append(dst, tagNil)
		return dst, nil

	case bool:
		dst = append(dst, tagBool)
		if x {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
		return dst, nil

	// ints
	case int:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case int8:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case int16:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case int32:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case int64:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, x)
		return dst, nil

	// uints
	case uint:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case uint8:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case uint16:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case uint32:
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil
	case uint64:
		// If you care about overflow, clamp or error; here we error if too large.
		if x > math.MaxInt64 {
			return dst, fmt.Errorf("uint64 too large for int64: %d", x)
		}
		dst = append(dst, tagInt64)
		dst = appendI64(dst, int64(x))
		return dst, nil

	case float32:
		dst = append(dst, tagFloat)
		dst = appendF64(dst, float64(x))
		return dst, nil
	case float64:
		dst = append(dst, tagFloat)
		dst = appendF64(dst, x)
		return dst, nil

	case json.Number:
		// Try int first
		if i64, err := x.Int64(); err == nil {
			dst = append(dst, tagInt64)
			dst = appendI64(dst, i64)
			return dst, nil
		}
		// Otherwise store as string bytes
		s := x.String()
		dst = append(dst, tagString)
		dst = appendU32(dst, uint32(len(s)))
		dst = append(dst, s...)
		return dst, nil

	case string:
		dst = append(dst, tagString)
		dst = appendU32(dst, uint32(len(x)))
		dst = append(dst, x...)
		return dst, nil

	case []byte:
		dst = append(dst, tagBytes)
		dst = appendU32(dst, uint32(len(x)))
		dst = append(dst, x...)
		return dst, nil

	default:
		// Fallback: store JSON bytes (remove for max speed if you can constrain types)
		b, err := json.Marshal(x)
		if err != nil {
			return dst, fmt.Errorf("unsupported type %T (json marshal failed): %w", v, err)
		}
		dst = append(dst, tagJSON)
		dst = appendU32(dst, uint32(len(b)))
		dst = append(dst, b...)
		return dst, nil
	}
}

// ----- append helpers (fast, no reflection) -----

func appendU16(dst []byte, v uint16) []byte {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	return append(dst, b[:]...)
}

func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendI64(dst []byte, v int64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	return append(dst, b[:]...)
}

func appendF64(dst []byte, v float64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	return append(dst, b[:]...)
}

func writeU32ToWriter(w *bufio.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}
