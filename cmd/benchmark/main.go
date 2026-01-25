package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ColorReset  = "\033[0m"
	ColorCyan   = "\033[36m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorRed    = "\033[31m"
	ColorBlue   = "\033[34m"
	ColorBold   = "\033[1m"
)

const (
	Addr      = "127.0.0.1:11211"
	TotalOps  = 1000000
	Workers   = 500
	DataValue = "bench-value-payload"
)

func main() {
	printHeader()

	// --- PHASE 1: SET (Write) ---
	fmt.Printf("%s%s🔹 Phase 1: Heavy Write (SET) %d keys...%s\n", ColorBold, ColorCyan, TotalOps, ColorReset)
	runTest(true)

	// --- PHASE 2: GET (Read) ---
	fmt.Printf("\n%s%s🔹 Phase 2: Integrity Check (GET) %d keys...%s\n", ColorBold, ColorCyan, TotalOps, ColorReset)
	runTest(false)

	fmt.Printf("\n%s%s✨ Benchmark Session Completed!%s\n", ColorBold, ColorGreen, ColorReset)
}

func runTest(isSet bool) {
	var wg sync.WaitGroup
	var opsDone int64
	var errors int64
	start := time.Now()

	for i := 0; i < Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// RETRY LOGIC for stable connection under high OS pressure
			var conn net.Conn
			var err error
			for retries := 0; retries < 30; retries++ {
				conn, err = net.Dial("tcp", Addr)
				if err == nil {
					break
				}
				time.Sleep(30 * time.Millisecond)
			}

			if err != nil {
				atomic.AddInt64(&errors, 1)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			writer := bufio.NewWriter(conn)

			for {
				current := atomic.AddInt64(&opsDone, 1)
				if current > TotalOps {
					break
				}

				key := fmt.Sprintf("key-%d", current)

				if isSet {
					fmt.Fprintf(writer, "set %s 0 0 %d\r\n%s\r\n", key, len(DataValue), DataValue)
					writer.Flush()

					resp, _ := reader.ReadString('\n')
					if !strings.Contains(resp, "STORED") {
						atomic.AddInt64(&errors, 1)
					}
				} else {
					fmt.Fprintf(writer, "get %s\r\n", key)
					writer.Flush()

					resp, _ := reader.ReadString('\n')
					if strings.Contains(resp, "VALUE") {
						data, _ := reader.ReadString('\n')
						reader.ReadString('\n')
						if strings.TrimSpace(data) != DataValue {
							atomic.AddInt64(&errors, 1)
						}
					} else {
						atomic.AddInt64(&errors, 1)
					}
				}

				if current%500000 == 0 {
					fmt.Printf("   %sProgress:%s %d / %d ops (%d%%)\n",
						ColorYellow, ColorReset, current, TotalOps, (current*100)/TotalOps)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	opsPerSec := float64(TotalOps) / duration.Seconds()

	mode := "WRITE (SET)"
	if !isSet {
		mode = "READ (GET)"
	}

	fmt.Printf("\n%s🏁 %s RESULTS:%s\n", ColorBold, mode, ColorReset)
	fmt.Printf("   %s⏱  Time:%s     %v\n", ColorBlue, ColorReset, duration)
	fmt.Printf("   %s🚀 Speed:%s    %s%.0f ops/sec%s\n", ColorBlue, ColorReset, ColorGreen, opsPerSec, ColorReset)

	errColor := ColorGreen
	if errors > 0 {
		errColor = ColorRed
	}
	fmt.Printf("   %s❌ Errors:%s   %s%d%s\n", ColorBlue, ColorReset, errColor, errors, ColorReset)
}

func printHeader() {
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("%s%s  LSM-TREE ENGINE HIGH-LOAD BENCHMARK%s\n", ColorBold, ColorYellow, ColorReset)
	fmt.Printf("  Target: %s%s%s | Workers: %s%d%s\n",
		ColorCyan, Addr, ColorReset, ColorCyan, Workers, ColorReset)
	fmt.Println(strings.Repeat("=", 60))
}
