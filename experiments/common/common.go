package common

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// GenerateDataset writes a file of `count` random numbers in [1, max] to path.
// dupRatio controls what fraction of numbers are repeats of earlier numbers
// (0.0 = no duplicates, 0.5 = ~50% of entries are repeats).
// This lets us control the theoretical dedup ratio for experiment 02.
func GenerateDataset(path string, count int, max uint64, dupRatio float64) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	seen := make([]uint64, 0, count/2)
	var sb strings.Builder
	for i := 0; i < count; i++ {
		var n uint64
		if len(seen) > 0 && rand.Float64() < dupRatio {
			n = seen[rand.Intn(len(seen))]
		} else {
			n = uint64(rand.Int63n(int64(max))) + 1
			seen = append(seen, n)
		}
		sb.WriteString(strconv.FormatUint(n, 10))
		sb.WriteByte('\n')
		if sb.Len() > 1<<20 { // flush every 1MB
			f.WriteString(sb.String())
			sb.Reset()
		}
	}
	f.WriteString(sb.String())
	return nil
}

// WriteTextFile writes a string to path, creating parent directories as needed.
func WriteTextFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0644)
}

// Result is one row of experiment output.
type Result struct {
	Experiment string
	Fields     map[string]string
	Order      []string // preserves field order for CSV header
}

// NewResult creates a result row for the given experiment name.
func NewResult(experiment string) *Result {
	return &Result{Experiment: experiment, Fields: make(map[string]string)}
}

// Set adds or overwrites a field. Call in the order you want columns to appear.
func (r *Result) Set(key, value string) {
	if _, exists := r.Fields[key]; !exists {
		r.Order = append(r.Order, key)
	}
	r.Fields[key] = value
}

func (r *Result) SetInt(key string, v int)       { r.Set(key, strconv.Itoa(v)) }
func (r *Result) SetInt64(key string, v int64)   { r.Set(key, strconv.FormatInt(v, 10)) }
func (r *Result) SetFloat(key string, v float64) { r.Set(key, strconv.FormatFloat(v, 'f', 2, 64)) }
func (r *Result) SetDuration(key string, d time.Duration) {
	r.Set(key, fmt.Sprintf("%.3f", d.Seconds()))
}

// WriteCSV appends this result as a row to experiments/results/<experiment>.csv,
// writing a header row first if the file doesn't exist yet.
func (r *Result) WriteCSV() error {
	dir := "experiments/results"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(dir, r.Experiment+".csv")

	writeHeader := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		writeHeader = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if writeHeader {
		f.WriteString(strings.Join(r.Order, ",") + "\n")
	}
	vals := make([]string, len(r.Order))
	for i, k := range r.Order {
		vals[i] = r.Fields[k]
	}
	_, err = f.WriteString(strings.Join(vals, ",") + "\n")
	return err
}

// PrintTable prints the result as a human-readable table row to stdout.
func (r *Result) PrintTable() {
	fmt.Println()
	fmt.Printf("=== %s ===\n", r.Experiment)
	for _, k := range r.Order {
		fmt.Printf("  %-24s %s\n", k+":", r.Fields[k])
	}
}

// CountLines counts non-empty lines in a file (used to count primes written).
func CountLines(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	count := 0
	for _, l := range lines {
		if strings.TrimSpace(l) != "" {
			count++
		}
	}
	return count, nil
}

// HasDuplicates checks whether a file of newline-separated numbers contains
// any duplicate values. Returns (hasDupes, uniqueCount, totalCount).
func HasDuplicates(path string) (bool, int, int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, 0, 0, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	seen := make(map[string]bool)
	total := 0
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		total++
		seen[l] = true
	}
	return len(seen) != total, len(seen), total, nil
}
