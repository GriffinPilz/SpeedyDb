package main

import (
	"SpeedyDb/btree"
	"SpeedyDb/btreeWriting"
	"SpeedyDb/structuredDB"
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var filePaths []string

var currentMapSize uint64
var minKey, maxKey int
var setMinMaxKey = false
var tr = btree.New(32)

type Pair struct {
	Key string
	Val any
}

type SeekPoints struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

type Manifest struct {
	BytesPerRow uint64                `json:"BytesPerRow"`
	RowOrder    []string              `json:"RowOrder"`
	SeekMap     map[string]SeekPoints `json:"SeekPoints"`
}

func createBtree(FilerFolderPath string) {
	files, err := os.ReadDir(FilerFolderPath)
	if err != nil {
		slog.Error("operation failed", "err", err)
		os.Exit(1)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".spdb") {
			filePaths = append(filePaths, path.Join(FilerFolderPath, file.Name()))
		}
	}
}

func printDecodeContext(dec *json.Decoder, data []byte, msg string) {
	off := int(dec.InputOffset())
	start := off - 80
	if start < 0 {
		start = 0
	}
	end := off + 80
	if end > len(data) {
		end = len(data)
	}
	fmt.Printf("JSON decode error (%s) near byte %d:\n%s\n\n", msg, off, data[start:end])
}

func readOrderedObject(dec *json.Decoder, data []byte) ([]Pair, error) {
	dec.UseNumber()

	// Expect '{'
	t, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("decode token (object start) at %d: %w", dec.InputOffset(), err)
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("expected object start '{', got %v", t)
	}

	var pairs []Pair

	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			printDecodeContext(dec, data, "reading key token")
			return nil, fmt.Errorf("decode key at %d: %w", dec.InputOffset(), err)
		}

		key, ok := kTok.(string)
		if !ok {
			printDecodeContext(dec, data, "key is not a string")
			return nil, fmt.Errorf("expected string key, got %T (%v)", kTok, kTok)
		}

		var v any
		if err := dec.Decode(&v); err != nil {
			printDecodeContext(dec, data, "decoding value")
			return nil, fmt.Errorf("decode value for key %q at %d: %w", key, dec.InputOffset(), err)
		}

		pairs = append(pairs, Pair{Key: key, Val: v})
	}

	t, err = dec.Token()
	if err != nil {
		printDecodeContext(dec, data, "reading object end")
		return nil, fmt.Errorf("decode token (object end) at %d: %w", dec.InputOffset(), err)
	}
	if d, ok := t.(json.Delim); !ok || d != '}' {
		return nil, fmt.Errorf("expected object end '}', got %v", t)
	}

	return pairs, nil
}

func ToInt(v any) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil

	case json.Number:
		i64, err := x.Int64()
		if err != nil {
			return 0, fmt.Errorf("json.Number not an int: %w", err)
		}
		return int(i64), nil

	case float64:
		if math.Trunc(x) != x {
			return 0, fmt.Errorf("float64 is not an integer: %v", x)
		}
		return int(x), nil

	case string:
		i, err := strconv.Atoi(x)
		if err != nil {
			return 0, fmt.Errorf("string not an int: %w", err)
		}
		return i, nil

	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}

func renameFile(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func createNewWriter(path string) (*btreeWriting.Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return btreeWriting.NewWriter(f), nil
}

func iteratorWriter(it *btree.Iter, spw *btreeWriting.Writer, breakAtBytes uint64) int {
	for {
		item, ok := it.Next()
		if !ok {
			_ = spw.Close()
			break
		}

		if writeErr := spw.WriteItem(item); writeErr != nil {
			slog.Error("operation failed", "err", writeErr)
			closeWriterErr := spw.Close()
			if closeWriterErr != nil {
				slog.Error("operation failed", "err", closeWriterErr)
			}
		}

		if spw.BytesWritten >= breakAtBytes && breakAtBytes != 0 {
			_ = spw.Close()
			return item.PK
		}
	}
	return 0
}

func resetInMemoryState() {
	tr = btree.New(32)
	currentMapSize = 0
	setMinMaxKey = false
	minKey, maxKey = 0, 0
}

func writeMapToFile(storagePath string, MaxMemorySize uint64) {
	HalfMemorySize := MaxMemorySize / 2
	lowerFile := filepath.Join(storagePath, fmt.Sprintf("%d_%s.spdb", minKey, "lower"))

	spw, createWriterError := createNewWriter(lowerFile)
	if createWriterError != nil {
		slog.Error("operation failed", "err", createWriterError)
	}

	it := tr.IterAscend()
	// first file
	minSplitMax := iteratorWriter(it, spw, HalfMemorySize)
	finalLower := filepath.Join(storagePath, fmt.Sprintf("%d_%d.spdb", minKey, minSplitMax))
	_ = renameFile(lowerFile, finalLower)

	// second file
	item, ok := it.Next()
	if !ok {
		resetInMemoryState()
		return
	}
	maxSplitMin := item.PK
	upperFile := filepath.Join(storagePath, fmt.Sprintf("%d_%d.spdb", maxSplitMin, maxKey))
	spw, createWriterError = createNewWriter(upperFile)
	if createWriterError != nil {
		slog.Error("operation failed", "err", createWriterError)
	}
	// write the item we just gathered
	if writeErr := spw.WriteItem(item); writeErr != nil {
		slog.Error("operation failed", "err", writeErr)
		_ = spw.Close()
	}
	_ = iteratorWriter(it, spw, 0)

	resetInMemoryState()
}

func importDataFromFile(filePath string, MaxMemorySize uint64, storagePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		slog.Error("operation failed", "err", err)
		os.Exit(1)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		slog.Error("operation failed", "err", err)
	}
	sizeBytes := uint64(info.Size())

	writeToDisk := false
	if sizeBytes > MaxMemorySize {
		writeToDisk = true
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 10*1024*1024)
	var lineSize uint64
	for scanner.Scan() {
		line := scanner.Bytes()

		if writeToDisk {
			lineSize = uint64(len(line))
			if lineSize+currentMapSize > MaxMemorySize {
				writeMapToFile(storagePath, MaxMemorySize)
			}
		}
		dec := json.NewDecoder(bytes.NewReader(line))

		pairs, readOrderedError := readOrderedObject(dec, line)
		if readOrderedError != nil {
			slog.Error("operation failed", "err", readOrderedError, "file", filePath, "line", string(line))
			os.Exit(1)
		}

		var PrimaryKey int
		var tempMap = map[string]any{}
		for index, pair := range pairs {
			// primary key check
			if index == 0 {
				i, convertPKError := ToInt(pair.Val)
				if convertPKError != nil {
					slog.Error("operation failed", "err", convertPKError)
				}
				PrimaryKey = i

				if !setMinMaxKey {
					minKey = PrimaryKey
					maxKey = PrimaryKey
					setMinMaxKey = true
				}
				if PrimaryKey < minKey {
					minKey = PrimaryKey
				}
				if PrimaryKey > maxKey {
					maxKey = PrimaryKey
				}
			} else {
				tempMap[pair.Key] = pair.Val
			}
		}
		tr.Upsert(btree.Item{PK: PrimaryKey, Row: tempMap})
		if writeToDisk {
			currentMapSize += lineSize
		}
	}
	if writeToDisk {
		if tr.Len() > 0 {
			writeMapToFile(storagePath, MaxMemorySize)
		}
	}

	fmt.Println("Current Map Size: {:%d}", currentMapSize, minKey, maxKey)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Alloc = %.2f MiB\n", float64(m.Alloc)/1024/1024)           // bytes allocated and still in use
	fmt.Printf("TotalAlloc = %.2f MiB\n", float64(m.TotalAlloc)/1024/1024) // bytes allocated over time
	fmt.Printf("Sys = %.2f MiB\n", float64(m.Sys)/1024/1024)               // bytes obtained from OS
	fmt.Printf("HeapInuse = %.2f MiB\n", float64(m.HeapInuse)/1024/1024)
	fmt.Printf("NumGC = %d\n", m.NumGC)
}

func determineSeekPoints(user string, password string, url string, port string, schema string, table string) (rowBytes uint64, orderSlice []string, seekMap map[string]SeekPoints) {
	rowBytes, columnSizeMap, orderSlice, err := structuredDB.GetRowSizeSQL(user, password, url, port, schema, table)
	if err != nil {
		log.Panic(err)
	}

	seekMap = make(map[string]SeekPoints, len(orderSlice))
	var previousEndPoint uint32 = 0
	for _, column := range orderSlice {
		// -1 because we are including the bytes in the range. 8 bytes including 0 are position 0-7
		size := uint32(columnSizeMap[column])

		seekMap[column] = SeekPoints{
			Start: previousEndPoint,
			End:   previousEndPoint + size - 1,
		}

		previousEndPoint += size
	}

	fmt.Println("Row Size: ", rowBytes, ", Column Size: ", columnSizeMap, ", Order Slice: ", orderSlice, ", seekMap: ", seekMap)
	return rowBytes, orderSlice, seekMap
}

func createManifest(rowBytes uint64, orderSlice []string, seekMap map[string]SeekPoints, workingDirectory string, tableName string) (string, error) {
	f, err := os.OpenFile(
		filepath.Join(workingDirectory, fmt.Sprintf("%s_manifest.json", tableName)),
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0o644,
	)
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	m := Manifest{
		BytesPerRow: rowBytes,
		RowOrder:    orderSlice,
		SeekMap:     seekMap,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	if err := enc.Encode(m); err != nil {
		return "", err
	}

	return f.Name(), nil
}

func main() {
	start := time.Now()
	wd, err := os.Getwd()
	if err != nil {
		log.Fatal("operation failed", "err", err)
	}

	DataStoragePath := flag.String("f", wd, "Path to file storage folder")
	MaxMemorySize := flag.Uint64("m", 10_737_418_240, "Maximum amount of memory to use. Default is 10 GB (10737418240)")
	user := flag.String("user", "", "User for database extraction")
	password := flag.String("password", "", "Password for database extraction")
	url := flag.String("url", "127.0.0.1", "URL for database extraction")
	port := flag.String("port", "3306", "Port for database extraction")
	schema := flag.String("schema", "benchdb", "Schema for database extraction")
	table := flag.String("table", "big10g", "Table for database extraction")

	flag.Parse()
	//uds := flag.String("uds", "/tmp/kvdb.sock", "UDS socket path")
	//shards := flag.Int("shards", 64, "number of shards")
	//debug := flag.Bool("debug", false, "enable debug logging")

	var logDir = *DataStoragePath
	logFile := "SpeedyDb.log"
	logPath := filepath.Join(logDir, logFile)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("open log file: %v", err)
	}
	defer f.Close()

	logger := slog.New(slog.NewJSONHandler(f, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("config",
		"data_storage_path", *DataStoragePath,
		"max_memory_size", *MaxMemorySize,
	)

	if *DataStoragePath != "" {
		createBtree(*DataStoragePath)
	}

	rowBytes, orderSlice, seekMap := determineSeekPoints(*user, *password, *url, *port, *schema, *table)
	manifestFile, createManifestError := createManifest(rowBytes, orderSlice, seekMap, *DataStoragePath, fmt.Sprintf("%s_%s", *schema, *table))
	if createManifestError != nil {
		slog.Error("operation failed", "err", createManifestError)
	}
	fmt.Println("manifestFile:", manifestFile)
	//importDataFromFile("/Users/griffinpilz/GolandProjects/SpeedyDb/inputTest.txt", *MaxMemorySize, *DataStoragePath)
	elapsed := time.Since(start)
	fmt.Println("elapsed:", elapsed)
}
