package main

import (
	"github.com/pterm/pterm"
	"strconv"
	"sync"
	"time"
)

type statistics struct {
	mu sync.Mutex
	// deleted number of items
	deleted uint64
	// rcu is the number of consumed read capacity units
	rcu float64
	// wcu is the number of consumed write capacity units
	wcu float64
}

func (s *statistics) increaseDeleted(n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted += n
}

func (s *statistics) addRCU(n float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rcu += n
}

func (s *statistics) addWCU(n float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wcu += n
}

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
