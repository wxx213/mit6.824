package raft

import (
	"fmt"
	"log"
	"runtime"
	"sort"
)

// Debugging
const Debug = 0

func init() {
	if Debug > 0 {
		log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	}
}

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

func stack() string {
	var buf [2 << 10]byte
	return string(buf[:runtime.Stack(buf[:], false)])
}

// for debug, print node role
func printNodeInfo(cfg *config) {
	var term int
	var isleader bool

	if cfg == nil {
		return
	}
	if len(cfg.rafts) == 0 {
		return
	}
	log.Println("start printNodeInfo")
	fmt.Println("server term isleader electiontimeoutms role connected logindex commitindex applylogs  matchIndex        logs")
	for i:=0;i<cfg.n;i++ {
		term, isleader = cfg.rafts[i].GetState()
		fmt.Println(i,"    ",term,"  ",isleader,"   ",cfg.rafts[i].electiontimeoutms,
			"             ",cfg.rafts[i].roleState,"    ", cfg.connected[i],
			"    ",cfg.rafts[i].logIndex, "       ", cfg.rafts[i].commitIndex, "    ",
			len(cfg.logs[i]), "     ", cfg.rafts[i].matchIndex, "    ", cfg.rafts[i].log)
	}
	log.Println("stack: ", stack())
}