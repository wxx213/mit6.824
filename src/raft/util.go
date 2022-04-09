package raft

import (
	"log"
	"sort"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func minIndex(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func findNthMinIndex(index []int, n int) int {
	if len(index) == 0 {
		return 0
	}
	if len(index) <= n {
		return 0
	}
	sort.Ints(index)
	return index[n]
}