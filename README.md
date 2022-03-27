# Introduction

Labs for [mit6.824](http://nil.csail.mit.edu/6.824/2020/schedule.html)

# debug

add some extra code to debug

set log

```go
log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
```

print node info

```go
// for debug, print node role
func checkrole(cfg *config) {
	var term int
	var isleader bool

	log.Println("start checkrole")
	fmt.Println("server term isleader electiontimeoutms role connected")
	for i:=0;i<cfg.n;i++ {
		term, isleader = cfg.rafts[i].GetState()
		fmt.Println(i,"    ",term,"  ",isleader,"   ",cfg.rafts[i].electiontimeoutms,
			"             ",cfg.rafts[i].roleState,"    ", cfg.connected[i])
	}
}
```

