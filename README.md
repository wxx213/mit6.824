# 介绍

[mit6.824](http://nil.csail.mit.edu/6.824/2020/schedule.html) 课程的Lab代码

原始的代码仓库地址

```
git://g.csail.mit.edu/6.824-golabs-2020
```

# Raft

网页地址：http://nil.csail.mit.edu/6.824/2020/labs/lab-raft.html

## 基本概念

### 日志复制

日志提交(commit)：for follower, 日志已经被保存到了本地日志序列；for leader, 日志已经被大部分节点复制

日志应用(apply)：日志（作为元数据）已经被应用到了本地状态机（本地存储系统）

## 调试

设置日志可以显示行号和毫秒时间戳

```go
log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
```

打印节点角色信息

```go
func stack() string {
	var buf [2 << 10]byte
	return string(buf[:runtime.Stack(buf[:], false)])
}

// for debug, print node role
func checkrole(cfg *config) {
	var term int
	var isleader bool

	log.Println("start checkrole")
	fmt.Println("server term isleader electiontimeoutms role connected logindex commitindex nextIndex")
	for i:=0;i<cfg.n;i++ {
		term, isleader = cfg.rafts[i].GetState()
		fmt.Println(i,"    ",term,"  ",isleader,"   ",cfg.rafts[i].electiontimeoutms,
			"             ",cfg.rafts[i].roleState,"    ", cfg.connected[i],
			"    ",cfg.rafts[i].logIndex, "", cfg.rafts[i].commitIndex, "  ", cfg.rafts[i].nextIndex)
	}
	log.Println("stack: ", stack())
}
```

