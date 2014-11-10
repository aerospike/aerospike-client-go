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
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/aerospike/aerospike-client-go"
	. "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
)

type TStats struct {
	Exit       bool
	W, R       int // write and read counts
	WE, RE     int // write and read errors
	WTO, RTO   int // write and read timeouts
	WMin, WMax int64
	RMin, RMax int64
	WLat, RLat int64
	Wn, Rn     []int64
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
var latency = flag.String("L", "", "Latency <columns>,<shift>.\n\tShow transaction latency percentages using elapsed time ranges.\n\t<columns> Number of elapsed time ranges.\n\t<shift>   Power of 2 multiple between each range starting at column 3.")
var throughput = flag.Int("g", 0, "Throttle transactions per second to a maximum value.\n\tIf tps is zero, do not throttle throughput.\n\tUsed in read/write mode only.")
var timeout = flag.Int("T", 0, "Read/Write timeout in milliseconds.")
var maxRetries = flag.Int("maxRetries", 2, "Maximum number of retries before aborting the current transaction.")
var connQueueSize = flag.Int("queueSize", 4096, "Maximum number of connections to pool.")

var randBinData = flag.Bool("R", false, "Use dynamically generated random bin values instead of default static fixed bin values.")
var debugMode = flag.Bool("d", false, "Run benchmarks in debug mode.")
var profileMode = flag.Bool("profile", false, "Run benchmarks with profiler active on port 6060.")
var showUsage = flag.Bool("u", false, "Show usage information.")

// parsed data
var binDataType string
var binDataSize int
var workloadType string
var workloadPercent int
var latBase, latCols int

// group mutex to wait for all load generating go routines to finish
var wg sync.WaitGroup

func main() {
	log.SetOutput(os.Stdout)

	// use all cpus in the system for concurrency
	runtime.GOMAXPROCS(runtime.NumCPU())
	readFlags()

	// launch profiler if in profile mode
	if *profileMode {
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	printBenchmarkParams()

	clientPolicy := NewClientPolicy()
	// cache lots  connections
	clientPolicy.ConnectionQueueSize = *connQueueSize
	client, err := NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Nodes Found:", client.GetNodeNames())

	go reporter()
	for i := 1; i < *concurrency; i++ {
		wg.Add(1)
		go runBench(client, i-1, *keyCount / *concurrency)
	}
	wg.Add(1)
	go runBench(client, *concurrency-1, *keyCount / *concurrency + *keyCount%*concurrency)

	wg.Wait()

	// send term to reporter, and wait for it to terminate
	countReportChan <- &TStats{Exit: true}
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
	log.Printf("latency:\t\t%d:%d", latBase, latCols)
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

func parseLatency(param string) (int, int) {
	re := regexp.MustCompile(`(\d+)[:,](\d+)`)
	values := re.FindStringSubmatch(param)

	// see if the value is supplied
	if len(values) > 2 && strings.Trim(values[1], " ") != "" && strings.Trim(values[2], " ") != "" {
		if value1, err := strconv.Atoi(strings.Trim(values[1], " ")); err == nil {
			if value2, err := strconv.Atoi(strings.Trim(values[2], " ")); err == nil {
				return value1, value2
			}
		}
	}

	log.Fatal("Wrong latency values requested.")
	return 0, 0
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

	if *latency != "" {
		latCols, latBase = parseLatency(*latency)
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
	case "B":
		bin = &Bin{Name: "information", Value: NewBytesValue([]byte(randString(binDataSize, rnd)))}
	case "S":
		bin = &Bin{Name: "information", Value: NewStringValue(randString(binDataSize, rnd))}
	default:
		bin = &Bin{Name: "information", Value: NewLongValue(rnd.Int63())}
	}

	return bin
}

var r *Record

const random_alpha_num = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const l = 62

// generates a random strings of specified length
func randString(size int, rnd *rand.Rand) string {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = random_alpha_num[rnd.Intn(l)]
	}

	return string(buf)
}

func incOnError(op, timeout *int, err error) {
	if ae, ok := err.(AerospikeError); ok && ae.ResultCode() == TIMEOUT {
		*timeout++
	} else {
		*op++
	}
}

func runBench(client *Client, ident int, times int) {
	defer wg.Done()

	var err error

	writepolicy := NewWritePolicy(0, 0)
	writepolicy.Timeout = time.Duration(*timeout) * time.Millisecond
	writepolicy.MaxRetries = *maxRetries

	readpolicy := writepolicy.GetBasePolicy()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	defaultBin := getBin(rnd)

	t := time.Now()
	var WCount, RCount int
	var writeErr, readErr int
	var writeTOErr, readTOErr int

	var tm time.Time
	var wLat, rLat int64
	var wLatTotal, rLatTotal int64
	var wMinLat, rMinLat int64
	var wMaxLat, rMaxLat int64

	wLatList := make([]int64, latCols+1)
	rLatList := make([]int64, latCols+1)

	bin := defaultBin
	for i := 1; workloadType == "RU" || i <= times; i++ {
		rLat, wLat = 0, 0
		// if randomBin data has been requested
		if *randBinData {
			bin = getBin(rnd)
		}

		key, _ := NewKey(*namespace, *set, ident*times+(i%times))
		if workloadType == "I" || rnd.Intn(100) >= workloadPercent {
			WCount++
			tm = time.Now()
			if err = client.PutBins(writepolicy, key, bin); err != nil {
				incOnError(&writeErr, &writeTOErr, err)
			}
			wLat = int64(time.Now().Sub(tm) / time.Millisecond)
			wLatTotal += wLat

			// under 1 ms
			if wLat <= 1 {
				wLatList[0]++
			}

			for i := latCols; i > 0; i-- {
				if wLat > int64(latBase<<uint(i-1)) {
					wLatList[i]++
				}
			}

			if wLat < wMinLat {
				wMinLat = wLat
			}

			if wLat > wMaxLat {
				wMaxLat = wLat
			}
		} else {
			RCount++
			tm = time.Now()
			if r, err = client.Get(readpolicy, key, bin.Name); err != nil {
				incOnError(&readErr, &readTOErr, err)
			}
			rLat = int64(time.Now().Sub(tm) / time.Millisecond)
			rLatTotal += rLat
			// under 1 ms
			if rLat <= 1 {
				rLatList[0]++
			}

			// under 1 ms
			for i := latCols; i > 0; i-- {
				if rLat > int64(latBase<<uint(i-1)) {
					rLatList[i]++
				}
			}

			if rLat < rMinLat {
				rMinLat = rLat
			}
			if rLat > rMaxLat {
				rMaxLat = rLat
			}
		}

		if time.Now().Sub(t) > (100 * time.Millisecond) {
			countReportChan <- &TStats{false, WCount, RCount, writeErr, readErr, writeTOErr, readTOErr, wMinLat, wMaxLat, rMinLat, rMaxLat, wLatTotal, rLatTotal, wLatList, rLatList}
			WCount, RCount = 0, 0
			writeErr, readErr = 0, 0
			writeTOErr, readTOErr = 0, 0

			// reset stats
			wLatTotal, rLatTotal = 0, 0
			wMinLat, wMaxLat = 0, 0
			rMinLat, rMaxLat = 0, 0

			wLatList = make([]int64, latCols+1)
			rLatList = make([]int64, latCols+1)

			t = time.Now()
		}
	}
	countReportChan <- &TStats{false, WCount, RCount, writeErr, readErr, writeTOErr, readTOErr, wMinLat, wMaxLat, rMinLat, rMaxLat, wLatTotal, rLatTotal, wLatList, rLatList}
}

// calculates transactions per second
func calcTPS(count int, duration time.Duration) int {
	return int(float64(count) / (float64(duration) / float64(time.Second)))
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// listens to transaction report channel, and print them out on intervals
func reporter() {
	var totalWCount, totalRCount int
	var totalWErrCount, totalRErrCount int
	var totalWTOCount, totalRTOCount int
	var totalCount, totalTOCount, totalErrCount int
	lastReportTime := time.Now()

	var memStats = new(runtime.MemStats)
	var lastTotalAllocs, lastPauseNs uint64

	// var wLat, rLat int64
	var wTotalLat, rTotalLat int64
	var wMinLat, rMinLat int64
	var wMaxLat, rMaxLat int64
	wLatList := make([]int64, latCols+1)
	rLatList := make([]int64, latCols+1)

	var strBuff bytes.Buffer

	memProfileStr := func() string {
		var res string
		if *debugMode {
			// GC stats
			runtime.ReadMemStats(memStats)
			allocMem := (memStats.TotalAlloc - lastTotalAllocs) / (1024)
			pauseNs := (memStats.PauseTotalNs - lastPauseNs) / 1e6
			res = fmt.Sprintf(" (malloc (KiB): %d, GC pause(ms): %d)",
				allocMem,
				pauseNs,
			)
			// GC
			lastPauseNs = memStats.PauseTotalNs
			lastTotalAllocs = memStats.TotalAlloc
		}

		return res
	}

Loop:
	for {
		select {
		case stats := <-countReportChan:
			totalWCount += stats.W
			totalRCount += stats.R

			totalWErrCount += stats.WE
			totalRErrCount += stats.RE

			totalWTOCount += stats.WTO
			totalRTOCount += stats.RTO

			totalCount += (stats.W + stats.R)
			totalErrCount += (stats.WE + stats.RE)
			totalTOCount += (stats.WTO + stats.RTO)

			wTotalLat += stats.WLat
			rTotalLat += stats.RLat

			for i := 0; i <= latCols; i++ {
				wLatList[i] += stats.Wn[i]
				rLatList[i] += stats.Rn[i]
			}

			if stats.RMax > rMaxLat {
				rMaxLat = stats.RMax
			}
			if stats.RMin < rMinLat {
				rMinLat = stats.RMin
			}
			if stats.WMax > wMaxLat {
				wMaxLat = stats.WMax
			}
			if stats.WMin < wMinLat {
				wMinLat = stats.WMin
			}

			if stats.Exit || time.Now().Sub(lastReportTime) >= time.Second {
				wtps := calcTPS(totalWCount, time.Now().Sub(lastReportTime))
				rtps := calcTPS(totalRCount, time.Now().Sub(lastReportTime))
				if workloadType == "I" {
					log.Printf("write(tps=%d timeouts=%d errors=%d totalCount=%d)%s",
						wtps, totalTOCount, totalErrCount, totalCount,
						memProfileStr(),
					)
				} else {
					log.Printf(
						"write(tps=%d timeouts=%d errors=%d) read(tps=%d timeouts=%d errors=%d) total(tps=%d timeouts=%d errors=%d, count=%d)%s",
						wtps, totalWCount, totalWErrCount,
						rtps, totalRCount, totalRErrCount,
						wtps+rtps, totalTOCount, totalErrCount, totalCount,
						memProfileStr(),
					)
				}

				if *latency != "" {
					strBuff.WriteString("\t\tMin\tAvg\tMax\t|<=1 ms\t\t|>1")
					for i := 1; i < latCols; i++ {
						strBuff.WriteString(fmt.Sprintf("\t\t|>%d", latBase<<uint(i)))
					}
					log.Println(strBuff.String())
					strBuff.Reset()

					strBuff.WriteString(fmt.Sprintf("\tREAD\t%d\t%2.0f\t%d", rMinLat, float64(rTotalLat)/float64(rtps+1), rMaxLat))
					for i := 0; i <= latCols; i++ {
						strBuff.WriteString(fmt.Sprintf("\t|%7d/%4.2f", rLatList[i], float64(rLatList[i])/float64(rtps+1)*100))
					}
					log.Println(strBuff.String())
					strBuff.Reset()

					strBuff.WriteString(fmt.Sprintf("\tWRITE\t%d\t%2.0f\t%d", wMinLat, float64(wTotalLat)/float64(wtps+1), wMaxLat))
					for i := 0; i <= latCols; i++ {
						strBuff.WriteString(fmt.Sprintf("\t|%7d/%4.2f", wLatList[i], float64(wLatList[i])/float64(wtps+1)*100))
					}
					log.Println(strBuff.String())
					strBuff.Reset()
				}

				// reset stats
				wTotalLat, rTotalLat = 0, 0
				wMinLat, wMaxLat = 0, 0
				rMinLat, rMaxLat = 0, 0
				for i := 0; i <= latCols; i++ {
					wLatList[i] = 0
					rLatList[i] = 0
				}

				totalWCount, totalRCount = 0, 0
				totalWErrCount, totalRErrCount = 0, 0
				totalTOCount, totalWTOCount, totalRTOCount = 0, 0, 0
				lastReportTime = time.Now()

				if stats.Exit {
					break Loop
				}
			}
		}
	}
	countReportChan <- &TStats{}
}
