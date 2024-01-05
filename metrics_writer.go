package aerospike

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v6/types"
	"github.com/shirou/gopsutil/v3/process"
)

const (
	TIMESTAMP_FORMAT = "2006-01-02 03:04:05.000"
)

type MetricsWriter struct {
	enabled bool
	sb      strings.Builder
	dir     string
	file    *os.File
}

func (mw *MetricsWriter) onEnable(clstr *Cluster, policy *MetricsPolicy) error {
	mw.sb.Reset()
	now := time.Now()

	err := os.Mkdir(mw.dir, 0644)
	if err != nil {
		return err
	}

	path := mw.dir + string(os.PathSeparator) + "metrics-" + now.Format("20060102030405") + ".log"

	mw.file, err = os.OpenFile(path, os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	mw.sb.Reset()

	formatted_time := now.Format(TIMESTAMP_FORMAT)
	mw.sb.WriteString(formatted_time)

	mw.sb.WriteString(" header(1)")
	// TODO: need to update this
	mw.sb.WriteString(" cluster[name,cpu,mem,tranCount,retryCount,node[]]")
	mw.sb.WriteString(" node[name,address,port,errors,latency[]]")
	mw.sb.WriteString(" latency(")
	mw.sb.WriteString(string(policy.latencyColumns))
	mw.sb.WriteString(",")
	mw.sb.WriteString(string(policy.latencyShift))
	mw.sb.WriteString(")")
	mw.sb.WriteString("[type[l1,l2,l3...]]")
	mw.writeLine()

	mw.enabled = true

	return nil
}

func (mw *MetricsWriter) writeLine() error {
	mw.sb.WriteString("\n")
	_, err := mw.file.WriteString(mw.sb.String())
	if err != nil {
		return err
	}
	return nil
}

func (mw *MetricsWriter) onSnapshot(clstr *Cluster) error {
	if mw.enabled {
		err := mw.writeCluster(clstr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mw *MetricsWriter) onNodeClose(node *Node) error {
	var err error = nil
	if mw.enabled {
		mw.sb.Reset()
		mw.sb.WriteString(time.Now().Format(TIMESTAMP_FORMAT))
		mw.sb.WriteString(" node")
		mw.writeNode(node)
		err = mw.writeLine()
	}
	return err
}

func (mw *MetricsWriter) onDisable(clstr *Cluster) error {
	if mw.enabled {
		mw.enabled = false
		err := mw.writeCluster(clstr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mw *MetricsWriter) writeCluster(clstr *Cluster) error {
	cpu, err := getProcessCpuLoad()
	if err != nil {
		return err
	}

	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)
	mem := memStats.HeapAlloc

	// Formatting must match that of the Java client
	// See https://aerospike.atlassian.net/wiki/spaces/~bnichols/pages/3122856433/Java+Client+Metrics

	mw.sb.Reset()
	mw.sb.WriteString(time.Now().Format(TIMESTAMP_FORMAT))
	mw.sb.WriteString(" cluster[")

	// TODO: cluster name is missing

	cpu_rounded_down := int(cpu)
	mw.sb.WriteString(string(cpu_rounded_down))

	mw.sb.WriteString(",")
	mw.sb.WriteString(string(mem))
	mw.sb.WriteString(",")
	// TODO: threadsInUse: cluster.threadPool doesn't exist in the Go client
	// TODO: recoverQueueSize: cluster.recoverCount doesn't exist
	// TODO: cluster.invalidNodeCount doesn't exist
	mw.sb.WriteString(clstr.tranCount.String())
	mw.sb.WriteString(",")
	mw.sb.WriteString(clstr.retryCount.String())
	// TODO: cluster.delayQueue doesn't exist
	mw.sb.WriteString(",[")
	// TODO: cluster.eventLoops doesn't exist
	mw.sb.WriteString("],[")

	nodes := clstr.GetNodes()

	for i, node := range nodes {
		if i > 0 {
			mw.sb.WriteString(",")
		}
		mw.writeNode(node)
	}

	return nil
}

func (mw *MetricsWriter) writeNode(node *Node) {
	mw.sb.WriteString("[")
	mw.sb.WriteString(node.GetName())
	mw.sb.WriteString(",")

	host := node.GetHost()
	mw.sb.WriteString(host.Name)
	mw.sb.WriteString(",")
	mw.sb.WriteString(string(host.Port))
	mw.sb.WriteString(",")
	// TODO: node's connection stats doesn't exist
	// TODO: node's async connection stats doesn't exist
	mw.sb.WriteString(node.errorCount.String())
	mw.sb.WriteString(",")
	// TODO: node doesn't have timeoutCount
	mw.sb.WriteString("[")

	max := LATENCY_NONE
	for i := 0; i < max; i++ {
		if i > 0 {
			mw.sb.WriteString(",")
		}

		mw.sb.WriteString(latency_bucket_names[i])
		mw.sb.WriteString("[")

		buckets := node.metrics.latency[i]
		for i, bucket := range buckets.buckets {
			if i > 0 {
				mw.sb.WriteString(",")
			}
			mw.sb.WriteString(string(bucket.Get()))
		}
		mw.sb.WriteString("]")
	}

	mw.sb.WriteString("]]")
}

// stdlib's runtime package doesn't have a way to measure CPU usage (as far as I know)
func getProcessCpuLoad() (float64, error) {
	currPid := os.Getpid()
	if currPid > math.MaxInt32 || currPid < math.MinInt32 {
		// PID must be a 32 bit integer in order to be passed to gopsutil
		errorMsg := fmt.Sprintf("The PID of the application must be a 32-bit integer. Its value is %d", currPid)
		return -1.0, newError(types.COMMON_ERROR, errorMsg)
	}

	proc, err := process.NewProcess(int32(currPid))
	if err != nil {
		return -1.0, err
	}

	cpuUsagePerc, err := proc.CPUPercent()
	if err != nil {
		return -1.0, err
	}

	return cpuUsagePerc, nil
}
