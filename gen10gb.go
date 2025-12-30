package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var words = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
	"kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango",
	"uniform", "victor", "whiskey", "xray", "yankee", "zulu", "world", "hello", "test",
}

var labels = []string{"hotel", "romeo", "whiskey", "bravo", "india", "kilo", "papa", "zulu", "echo", "delta", "juliet"}
var names = []string{"kilo", "delta", "romeo", "zulu", "whiskey", "hotel", "india", "papa", "echo", "juliet", "xray", "golf", "tango", "oscar"}

func main() {
	out := flag.String("o", "inputTest.txt", "output file path")
	target := flag.Int64("size", 10_737_418_240, "target size in bytes (default 10 GiB)")
	seed := flag.Int64("seed", 42, "random seed")
	flag.Parse()

	f, err := os.OpenFile(*out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Big buffer = fewer syscalls, faster.
	bw := bufio.NewWriterSize(f, 16<<20) // 16 MiB
	defer bw.Flush()

	rng := rand.New(rand.NewSource(*seed))

	var written int64
	pk := 1

	start := time.Now()
	for written < *target {
		// Build a JSON line (similar shape to your examples)
		testIsString := pk%2 == 1

		var testPart string
		if testIsString {
			testPart = fmt.Sprintf("\"test\": %q", words[rng.Intn(len(words))])
		} else {
			testPart = fmt.Sprintf("\"test\": %d", rng.Intn(999999)+1)
		}

		ts := 1_760_000_000 + rng.Intn(10_000_000)
		active := "false"
		if pk%2 == 1 {
			active = "true"
		}

		count := rng.Intn(999999) + 1
		score := rng.Float64() * 1000.0

		tag1 := words[rng.Intn(len(words))]
		tag2 := words[rng.Intn(len(words))]

		metaV := rng.Intn(9999) + 1
		metaLabel := labels[rng.Intn(len(labels))]
		name := fmt.Sprintf("%s_%d", names[rng.Intn(len(names))], rng.Intn(9999)+1)

		line := fmt.Sprintf(
			"{\"primary_key\": %d, %s, \"ts\": %d, \"active\": %s, \"tags\": [%q, %q], \"score\": %.3f, \"count\": %d, \"name\": %q, \"meta\": {\"v\": %d, \"label\": %q}}\n",
			pk, testPart, ts, active, tag1, tag2, score, count, name, metaV, metaLabel,
		)

		// Write line; allow slight overshoot (keeps valid JSON lines).
		n, err := bw.WriteString(line)
		if err != nil {
			panic(err)
		}

		written += int64(n)
		pk++

		// Optional: flush occasionally (not required, but keeps buffers moving)
		// if pk%2_000_000 == 0 { _ = bw.Flush() }
	}

	if err := bw.Flush(); err != nil {
		panic(err)
	}

	fmt.Printf("Wrote ~%d bytes (%0.2f GiB) in %s (%d records)\n",
		written, float64(written)/(1024*1024*1024), time.Since(start).Round(time.Second), pk-1)
}
