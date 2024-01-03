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

type MetricsWriter struct {
	enabled bool
	sb      strings.Builder
}

func (mw *MetricsWriter) onDisable(clstr *Cluster) {
	if mw.enabled {
		mw.enabled = false
		mw.writeCluster(clstr)
	}
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
	// TODO: format time properly
	mw.sb.Reset()
	mw.sb.WriteString(time.Now().String())
	mw.sb.WriteString(" cluster[")
	mw.sb.WriteString(clstr.clusterName)
	mw.sb.WriteString(",")

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
	mw.sb.WriteString(",")
	mw.sb.WriteString(clstr.delayQueueTimeoutCount.String())
	mw.sb.WriteString(",[")
	// TODO: cluster.eventLoops doesn't exist
	mw.sb.WriteString("],[")

	nodes := clstr.GetNodes()

	for i, node := range nodes {
		if i > 0 {
			sb.WriteString(",")
		}
		writeNode(node)
	}
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
	// TODO: nodeStats doesn't match Java client's ConnectionStats
	// mw.sb.WriteString(node.stats)
	// TODO: node's async connection stats doesn't exist
	mw.sb.WriteString(node.errorCount.String())
	mw.sb.WriteString(",")
	// TODO: node doesn't have timeoutCount
	mw.sb.WriteString("[")

	max := LATENCY_NONE
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
