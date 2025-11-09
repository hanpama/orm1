package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type BenchResult struct {
	Name      string
	NsPerOp   float64
	BytesPerOp int64
	AllocsPerOp int64
}

type TestCategory struct {
	Name    string
	Results map[string]*BenchResult // library -> result
}

func main() {
	fmt.Println("Running benchmarks...")
	fmt.Println()

	// Run benchmarks and capture output
	cmd := exec.Command("go", "test", "-bench=.", "-benchmem", "-benchtime=1s", "./tests")
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nBenchmark failed: %v\n", err)
		os.Exit(1)
	}

	// Parse and display comparison tables first
	results := parseBenchmarkOutput(string(output))
	printComparison(results)
	fmt.Println()

	// Print raw benchmark output below
	fmt.Println("=" + strings.Repeat("=", 139))
	fmt.Println("RAW BENCHMARK OUTPUT")
	fmt.Println("=" + strings.Repeat("=", 139))
	fmt.Println()
	fmt.Println(string(output))
}

func parseBenchmarkOutput(output string) map[string]*TestCategory {
	categories := make(map[string]*TestCategory)

	// Regex to parse benchmark lines
	// Example: BenchmarkORM1_Postgres_Simple_Insert-12    	    1000	   1234567 ns/op	    5000 B/op	     100 allocs/op
	re := regexp.MustCompile(`Benchmark(\w+)_Postgres_(\w+)_(\w+)-\d+\s+\d+\s+(\d+(?:\.\d+)?)\s+ns/op\s+(\d+)\s+B/op\s+(\d+)\s+allocs/op`)

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		if len(matches) != 7 {
			continue
		}

		library := matches[1]    // ORM1, GORM, Bun, etc.
		category := matches[2]   // Simple, Aggregate
		test := matches[3]       // Insert, Select, Update, etc.
		nsPerOp, _ := strconv.ParseFloat(matches[4], 64)
		bytesPerOp, _ := strconv.ParseInt(matches[5], 10, 64)
		allocsPerOp, _ := strconv.ParseInt(matches[6], 10, 64)

		// Create category key
		catKey := fmt.Sprintf("%s_%s", category, test)

		if _, exists := categories[catKey]; !exists {
			categories[catKey] = &TestCategory{
				Name:    catKey,
				Results: make(map[string]*BenchResult),
			}
		}

		categories[catKey].Results[library] = &BenchResult{
			Name:        fmt.Sprintf("%s_%s_%s", library, category, test),
			NsPerOp:     nsPerOp,
			BytesPerOp:  bytesPerOp,
			AllocsPerOp: allocsPerOp,
		}
	}

	return categories
}

func printComparison(categories map[string]*TestCategory) {
	// Define test order
	testOrder := []string{
		"Simple_Insert", "Simple_Select", "Simple_Update", "Simple_BatchSelect", "Simple_ReadSlice", "Simple_Delete",
		"Aggregate_Insert", "Aggregate_Select", "Aggregate_Update", "Aggregate_BatchSelect", "Aggregate_ReadSlice", "Aggregate_Delete",
	}

	// Print three ranking tables: Time, Memory, Allocations
	printRankingTable(categories, testOrder, "TIME", func(r *BenchResult) float64 { return r.NsPerOp })
	fmt.Println()
	printRankingTable(categories, testOrder, "MEMORY", func(r *BenchResult) float64 { return float64(r.BytesPerOp) })
	fmt.Println()
	printRankingTable(categories, testOrder, "ALLOCATIONS", func(r *BenchResult) float64 { return float64(r.AllocsPerOp) })
}

func printRankingTable(categories map[string]*TestCategory, testOrder []string, title string, getValue func(*BenchResult) float64) {
	fmt.Println("=" + strings.Repeat("=", 139))
	fmt.Printf("%s RANKINGS\n", title)
	fmt.Println("=" + strings.Repeat("=", 139))
	fmt.Println()

	// Print header
	fmt.Printf("%-30s | %-18s | %-18s | %-18s | %-18s | %-18s | %-18s\n",
		"Test", "1st Place", "2nd Place", "3rd Place", "4th Place", "5th Place", "6th Place")
	fmt.Println(strings.Repeat("-", 140))

	// Process each test category in order
	for _, testKey := range testOrder {
		cat, exists := categories[testKey]
		if !exists {
			continue
		}

		// Sort results by the specified metric
		type rankEntry struct {
			library string
			result  *BenchResult
		}
		var ranked []rankEntry
		for lib, res := range cat.Results {
			ranked = append(ranked, rankEntry{lib, res})
		}
		sort.Slice(ranked, func(i, j int) bool {
			return getValue(ranked[i].result) < getValue(ranked[j].result)
		})

		// Print row
		fmt.Printf("%-30s", testKey)
		for i := 0; i < 6 && i < len(ranked); i++ {
			var formatted string
			if title == "TIME" {
				formatted = formatTimeEntry(ranked[i].library, ranked[i].result)
			} else if title == "MEMORY" {
				formatted = formatMemoryEntry(ranked[i].library, ranked[i].result)
			} else {
				formatted = formatAllocsEntry(ranked[i].library, ranked[i].result)
			}
			fmt.Printf(" | %-18s", formatted)
		}
		fmt.Println()
	}

	fmt.Println()
}

func formatTimeEntry(library string, result *BenchResult) string {
	var unit string
	var value float64

	if result.NsPerOp < 1000 {
		value = result.NsPerOp
		unit = "ns"
	} else if result.NsPerOp < 1000000 {
		value = result.NsPerOp / 1000
		unit = "μs"
	} else {
		value = result.NsPerOp / 1000000
		unit = "ms"
	}

	return fmt.Sprintf("%s (%.1f%s)", library, value, unit)
}

func formatMemoryEntry(library string, result *BenchResult) string {
	var unit string
	var value float64

	bytes := float64(result.BytesPerOp)
	if bytes < 1024 {
		value = bytes
		unit = "B"
	} else if bytes < 1024*1024 {
		value = bytes / 1024
		unit = "KB"
	} else {
		value = bytes / (1024 * 1024)
		unit = "MB"
	}

	return fmt.Sprintf("%s (%.1f%s)", library, value, unit)
}

func formatAllocsEntry(library string, result *BenchResult) string {
	return fmt.Sprintf("%s (%d)", library, result.AllocsPerOp)
}
