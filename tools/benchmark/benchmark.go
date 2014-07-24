// Copyright 2013-2014 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/citrusleaf/aerospike-client-go"
	. "github.com/citrusleaf/aerospike-client-go/logger"
)

type TStats struct {
	Exit         bool
	W, R, WE, RE int
}

var countReportChan = make(chan *TStats, 100) // async chan

var host = flag.String("h", "127.0.0.1", "Aerospike server seed hostnames or IP addresses")
var port = flag.Int("p", 3000, "Aerospike server seed hostname or IP address port number.")
var namespace = flag.String("n", "test", "Aerospike namespace.")
var set = flag.String("s", "testset", "Aerospike set name.")
var keyCount = flag.Int("k", 1000000, "Key/record count or key/record range.")

var binDef = flag.String("o", "I", "Bin object specification.\n\tI\t: Read/write integer bin.\n\tB:200\t: Read/write byte array bin of length 200.\n\tS:50\t: Read/write string bin of length 50.")
var concurrency = flag.Int("c", 32, "Number of goroutines to generate load.")
var workloadDef = flag.String("w", "I:100", "Desired workload.\n\tI:60\t: Linear 'insert' workload initializing 60% of the keys.\n\tRU:80\t: Random read/update workload with 80% reads and 20% writes.")
var throughput = flag.Int("g", 0, "Throttle transactions per second to a maximum value.\n\tIf tps is zero, do not throttle throughput.\n\tUsed in read/write mode only.")
var timeout = flag.Int("T", 0, "Read/Write timeout in milliseconds.")
var maxRetries = flag.Int("maxRetries", 2, "Maximum number of retries before aborting the current transaction.")

var randBinData = flag.Bool("R", false, "Use dynamically generated random bin values instead of default static fixed bin values.")
var debugMode = flag.Bool("d", false, "Run benchmarks in debug mode.")
var showUsage = flag.Bool("u", false, "Show usage information.")

// profiling
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

// parsed data
var binDataType string
var binDataSize int
var workloadType string
var workloadPercent int

// group mutex to wait for all load generating go routines to finish
var wg sync.WaitGroup

func main() {
	// use all cpus in the system for concurrency
	runtime.GOMAXPROCS(runtime.NumCPU())
	readFlags()

	// start cpu profiler if a filename is provided
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	printBenchmarkParams()

	client, err := NewClient(*host, *port)
	if err != nil {
		log.Fatal(err)
	}

	go reporter()
	for i := 1; i < *concurrency; i++ {
		wg.Add(1)
		go runBench(client, i-1, *keyCount / *concurrency)
	}
	wg.Add(1)
	go runBench(client, *concurrency-1, *keyCount / *concurrency + *keyCount%*concurrency)

	wg.Wait()

	// send term to reporter, and wait for it to terminate
	countReportChan <- &TStats{true, 0, 0, 0, 0}
	time.Sleep(10 * time.Millisecond)
	<-countReportChan
}

func workloadToString() string {
	switch workloadType {
	case "RU":
		return fmt.Sprintf("Read %d%%, Write %d%%", workloadPercent, 100-workloadPercent)
	default:
		return fmt.Sprintf("Initialize %d%% of records", workloadPercent)
	}
}

func throughputToString() string {
	if *throughput <= 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d", *throughput)
}

func printBenchmarkParams() {
	log.Printf("hosts:\t\t%s", *host)
	log.Printf("port:\t\t%d", *port)
	log.Printf("namespace:\t\t%s", *namespace)
	log.Printf("set:\t\t%s", *set)
	log.Printf("keys/records:\t%d", *keyCount)
	log.Printf("object spec:\t%s, size: %d", binDataType, binDataSize)
	log.Printf("random bin values\t%v", *randBinData)
	log.Printf("workload:\t\t%s", workloadToString())
	log.Printf("concurrency:\t%d", *concurrency)
	log.Printf("max throughput\t%s", throughputToString())
	log.Printf("timeout\t\t%v ms", *timeout)
	log.Printf("max retries\t\t%d", *maxRetries)
	log.Printf("debug:\t\t%v", *debugMode)
	// log.Printf("latency:\t\t%v")     //        false
}

// parses an string of (key:value) type
func parseValuedParam(param string) (string, *int) {
	re := regexp.MustCompile(`(\w+)([:,](\d+))?`)
	values := re.FindStringSubmatch(param)

	parStr := strings.ToUpper(strings.Trim(values[1], " "))

	// see if the value is supplied
	if len(values) > 3 && strings.Trim(values[3], " ") != "" {
		if value, err := strconv.Atoi(strings.Trim(values[3], " ")); err == nil {
			return parStr, &value
		}
	}

	return parStr, nil
}

// reads input flags and interprets the complex ones
func readFlags() {
	flag.Parse()

	if *showUsage {
		flag.Usage()
		os.Exit(0)
	}

	if *debugMode {
		Logger.SetLevel(INFO)
	}

	var binDataSz, workloadPct *int

	binDataType, binDataSz = parseValuedParam(*binDef)
	if binDataSz != nil {
		binDataSize = *binDataSz
	} else {
		switch binDataType {
		case "B":
			binDataSize = 200
		case "S":
			binDataSize = 50
		}
	}

	workloadType, workloadPct = parseValuedParam(*workloadDef)
	if workloadPct != nil {
		workloadPercent = *workloadPct
	} else {
		switch workloadType {
		case "I":
			workloadPercent = 100
		case "RU":
			workloadPercent = 50
		}
	}
}

// new random bin generator based on benchmark specs
func getBin(rnd *rand.Rand) *Bin {
	var bin *Bin
	switch binDataType {
	case "S":
		bin = &Bin{Name: "information", Value: NewStringValue(randString(binDataSize, rnd))}
	default:
		bin = &Bin{Name: "information", Value: NewLongValue(rnd.Int63())}
	}

	return bin
}

var r *Record

// generates a random strings of specified length
func randString(size int, rnd *rand.Rand) string {
	const random_alpha_num = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const l = 62

	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = random_alpha_num[rnd.Intn(l)]
	}

	return string(buf)
}

func runBench(client *Client, ident int, times int) {
	defer wg.Done()

	var err error

	writepolicy := NewWritePolicy(0, 0)
	writepolicy.Timeout = time.Duration(*timeout) * time.Millisecond
	writepolicy.MaxRetries = *maxRetries

	readpolicy := writepolicy.GetBasePolicy()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// defaultBin := NewBin(randString(10, rnd), rnd.Int63())
	defaultBin := NewBin(randString(1, rnd), rnd.Int63())

	t := time.Now()
	var WCount, RCount int
	var writeErr, readErr int
	for i := 1; workloadType == "RU" || i <= times; i++ {
		bin := defaultBin
		// if randomBin data has been requested
		if *randBinData {
			bin = getBin(rnd)
		}

		key, _ := NewKey(*namespace, *set, ident*times+(i%times))
		if workloadType == "I" || rnd.Intn(100) >= workloadPercent {
			WCount++
			if err = client.PutBins(writepolicy, key, bin); err != nil {
				writeErr++
			}
		} else {
			RCount++
			if r, err = client.Get(readpolicy, key, defaultBin.Name); err != nil {
				readErr++
			}
		}

		if time.Now().Sub(t) > (100 * time.Millisecond) {
			countReportChan <- &TStats{false, WCount, RCount, writeErr, readErr}
			WCount, RCount = 0, 0
			writeErr, readErr = 0, 0
			t = time.Now()
		}
	}
	countReportChan <- &TStats{false, WCount, RCount, writeErr, readErr}
}

// calculates transactions per second
func calcTPS(count int, duration time.Duration) int {
	return int(float64(count) / (float64(duration) / float64(time.Second)))
}

// listens to transaction report channel, and print them out on intervals
func reporter() {
	var totalWCount, totalRCount, totalWErrCount, totalRErrCount int
	var totalCount, totalErrCount int
	lastReportTime := time.Now()

Loop:
	for {
		select {
		case stats := <-countReportChan:
			totalWCount += stats.W
			totalRCount += stats.R
			totalWErrCount += stats.WE
			totalRErrCount += stats.RE
			totalCount += (stats.W + stats.R)
			totalErrCount += (stats.WE + stats.RE)

			if stats.Exit || time.Now().Sub(lastReportTime) >= time.Second {
				if workloadType == "I" {
					log.Printf("write(tps=%d timeouts=%d errors=%d totalCount=%d)", calcTPS(totalWCount+totalRCount, time.Now().Sub(lastReportTime)), 0, totalErrCount, totalCount)
				} else {
					log.Printf(
						"write(tps=%d timeouts=%d errors=%d) read(tps=%d timeouts=%d errors=%d) total(tps=%d timeouts=%d errors=%d, count=%d)",
						calcTPS(totalWCount, time.Now().Sub(lastReportTime)), 0, totalWErrCount,
						calcTPS(totalRCount, time.Now().Sub(lastReportTime)), 0, totalRErrCount,
						calcTPS(totalWCount+totalRCount, time.Now().Sub(lastReportTime)), 0, totalErrCount, totalCount,
					)
				}
				totalWCount, totalRCount = 0, 0
				totalWErrCount, totalRErrCount = 0, 0
				lastReportTime = time.Now()

				if stats.Exit {
					break Loop
				}
			}
		}
	}
	countReportChan <- &TStats{false, 0, 0, 0, 0}
}
