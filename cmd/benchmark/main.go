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
	Addr     = "127.0.0.1:11211"
	TotalOps = 1000000 // 100к записей
	Workers  = 500     // 50 параллельных соединений
	Value    = "bench-value-payload"
)

func main() {
	fmt.Printf("🚀 Запуск комплексного бенчмарка (%s)\n", Addr)

	// --- ФАЗА 1: SET ---
	fmt.Printf("\n🔹 Фаза 1: Запись (SET) %d ключей...\n", TotalOps)
	runTest(true)

	// --- ФАЗА 2: GET ---
	fmt.Printf("\n🔹 Фаза 2: Чтение и проверка (GET) %d ключей...\n", TotalOps)
	runTest(false)
}

func runTest(isSet bool) {
	var wg sync.WaitGroup
	var opsDone int64
	var errors int64
	start := time.Now()

	for i := 0; i < Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", Addr)
			if err != nil {
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
					// Команда SET
					fmt.Fprintf(writer, "set %s 0 0 %d\r\n%s\r\n", key, len(Value), Value)
					writer.Flush()

					// Ждем STORED\r\n
					resp, _ := reader.ReadString('\n')
					if !strings.Contains(resp, "STORED") {
						atomic.AddInt64(&errors, 1)
					}
				} else {
					// Команда GET
					fmt.Fprintf(writer, "get %s\r\n", key)
					writer.Flush()

					// Читаем VALUE <key> <flags> <bytes>\r\n
					resp, _ := reader.ReadString('\n')
					if strings.Contains(resp, "VALUE") {
						// Читаем саму строку данных
						data, _ := reader.ReadString('\n')
						data = strings.TrimSpace(data)

						// Читаем финальный END\r\n
						reader.ReadString('\n')

						if data != Value {
							atomic.AddInt64(&errors, 1)
						}
					} else {
						// Ключ не найден (END)
						atomic.AddInt64(&errors, 1)
					}
				}

				if current%100000 == 0 {
					fmt.Printf("   ... обработано %d\n", current)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	mode := "SET"
	if !isSet {
		mode = "GET"
	}

	fmt.Printf("🏁 Результаты %s:\n", mode)
	fmt.Printf("   Время: %v | Скорость: %.0f ops/sec | Ошибок: %d\n",
		duration, float64(TotalOps)/duration.Seconds(), errors)
}
