package main

import (
	"github.com/pterm/pterm"
	"strconv"
	"sync"
	"time"
)

// statistics contains metrics about relevant to the process of truncating DynamoDB tables.
//
// This includes the total number of deleted items, the total number of used read capacity units (RCU) and total number
// of used write capacity units.
type statistics struct {
	// mu is the mutex used to sync write operations to the statistics.
	mu sync.Mutex
	// deleted number of items
	deleted uint64
	// rcu is the number of consumed read capacity units
	rcu float64
	// wcu is the number of consumed write capacity units
	wcu float64
}

// increaseDeleted adds the given number n of deleted items to the statistics.
func (s *statistics) increaseDeleted(n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted += n
}

// addRCU adds the given number n of read capacity units to the statistics.
func (s *statistics) addRCU(n float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rcu += n
}

// addWCU adds the given number n of write capacity units to the statistics.
func (s *statistics) addWCU(n float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wcu += n
}

// printStatistics does print the given statistics as table to stdout.
func printStatistics(stats *statistics, start time.Time) {
	pterm.DefaultSection.Println("Statistics")

	err := pterm.DefaultTable.WithData(pterm.TableData{
		{"Deleted items:", strconv.FormatUint(stats.deleted, 10)},
		{"Duration:", time.Since(start).String()},
		{"Consumed Read Capacity Units:", strconv.FormatFloat(stats.rcu, 'f', 6, 64)},
		{"Consumed Write Capacity Units:", strconv.FormatFloat(stats.wcu, 'f', 6, 64)},
	}).Render()
	if err != nil {
		pterm.Error.Printf("Unable to print statistics table: %v\n", err)
	}
}
