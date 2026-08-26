// Standalone stuck-server for the context.AfterFunc vs hand-rolled goroutine
// demo. Runs as its own OS process, deliberately separate from the client
// binary, so the client's goroutine/heap/CPU measurements aren't polluted by
// server-side goroutines sharing the same runtime.
//
// Accepts connections and never responds, never closes -- every accepted
// connection blocks forever until the client closes it. No stats, no
// tuning knobs: start it once, leave it running, run the client binary
// against it as many times as needed.
//
//	go run .
package main

import (
	"fmt"
	"net"
	"syscall"
)

const listenAddr = "127.0.0.1:19191"

// raiseFileDescriptorLimit raises this process's own soft NOFILE limit to
// its hard limit -- a per-process resource limit, not a system-wide
// setting -- so accepting thousands of held-open connections doesn't run
// into "too many open files" at high client concurrency.
func raiseFileDescriptorLimit() {
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		return
	}
	rlim.Cur = rlim.Max
	_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlim)
}

func main() {
	raiseFileDescriptorLimit()

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		panic(err)
	}
	fmt.Println("stuck-server listening on", listenAddr, "-- accepts connections, never responds, never closes")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			var buf [1]byte
			for {
				if _, err := c.Read(buf[:]); err != nil {
					return // client closed
				}
			}
		}(conn)
	}
}