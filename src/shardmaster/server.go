package shardmaster

import (
  "net"
  "strconv"
  "time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  // added by Shusen Xu
  seqNum int  //processed sequence number with paxos
}


type Op struct {
  // Your data here.
  // added by Shusen Xu
  Op string
  GID int64
  Servers []string
  Shard int
  Pid string
}

// added by Shusen Xu
// definition of operation
const (
  JoinOp = "Join"
  LeaveOp = "Leave"
  MoveOp = "Move"
  QueryOp = "Query"
)


// helper functions added by Shusen Xu
func (sm *ShardMaster)WaitAgreement(seqNum int) Op{
  to := 10 * time.Millisecond
  for {
    decided, val := sm.px.Status(seqNum)
    if decided{
      return val.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second{
      to *= 2
    }
  }
}

// hints:
// create a new Config based on a previous one, you need to create a new map object (with make())
// and copy the keys and values individually

func (sm *ShardMaster) ProcessHelper(op Op){
  switch op.Op {
  case JoinOp:
    cfg := sm.NewConfig()
    // re-duplicate, if a config has been added, does not process
    if _, exist := cfg.Groups[op.GID]; !exist {
      cfg.Groups[op.GID] = op.Servers
      sm.configs = append(sm.configs, *cfg)
      sm.LoadBalance()
    }
  case LeaveOp:
    // create a new Config to keep all shards in case of a group leave
    cfg := sm.NewConfig()
    for idx, gid := range cfg.Shards {
      if gid == op.GID {
        // default group, waiting for reassign in re-balance
        cfg.Shards[idx] = 0
      }
    }
    delete(cfg.Groups, op.GID)
    sm.configs = append(sm.configs, *cfg)
    sm.LoadBalance()
  case MoveOp:
    cfg := sm.NewConfig()
    cfg.Shards[op.Shard] = op.GID
    sm.configs = append(sm.configs, *cfg)
  }

}

func (sm *ShardMaster) ProcessOp(op Op){
  var tmpOp Op
  for{
    decided, val := sm.px.Status(sm.seqNum+1)
    if decided{
      tmpOp = val.(Op)
    }else{
      sm.px.Start(sm.seqNum+1, op)
      tmpOp = sm.WaitAgreement(sm.seqNum+1)
    }
    sm.ProcessHelper(tmpOp)
    sm.seqNum += 1
    sm.px.Done(sm.seqNum)
    // op operation has been processed
    if tmpOp.Pid == op.Pid {
      break
    }
  }
}

func (sm *ShardMaster) CreatePid() string {
  return strconv.Itoa(int(time.Now().UnixNano()))+"_"+strconv.Itoa(int(sm.me))
}

func (sm *ShardMaster) NewConfig() *Config {
  newCfg := Config{Num :len(sm.configs), Groups: make(map[int64][]string)}
  if len(sm.configs)> 0{
    oldCfg := sm.configs[len(sm.configs)-1]
    // assign shards
    for key, value := range oldCfg.Shards{
      newCfg.Shards[key] = value
    }
    // assign groups
    for gid, server := range oldCfg.Groups{
      newCfg.Groups[gid] = server
    }
  }
  return &newCfg
}


func (sm *ShardMaster) LoadBalance() {
  // get shards and gid count info
  cfg := &sm.configs[len(sm.configs)-1]
  counter := make(map[int64]int)
  unassign := 0
  for gid, _ := range cfg.Groups {
    counter[gid] = 0
  }
  for _, gid := range cfg.Shards {
    if gid <= 0 {
      unassign++
      continue
    }
    count, _ := counter[gid]
    counter[gid] = count + 1
  }
  avgNum := NShards / len(cfg.Groups)
  reminder := NShards % len(cfg.Groups)


  if unassign == 0 {
    // join new group, not the initialized state
    assignNum := make(map[int64]int)
    targetGid := int64(0)
    for gid, count := range counter {
      if count > avgNum {
        if reminder > 0 {
          count--
          reminder--
        }
        if count == avgNum {
          continue
        }
        assignNum[gid] = count - avgNum
      } else if count == 0 {
        targetGid = gid
      }
    }

    // evenly assign
    for idx, gid := range cfg.Shards {
      if count, exist := assignNum[gid]; exist && count > 0 {
        assignNum[gid] = count - 1
        cfg.Shards[idx] = targetGid
      }
    }
  } else {
    // leave or the initialized state
    for gid, count := range counter {
      if count <= avgNum {
        if reminder > 0 {
         count--
         reminder--
        }
        if avgNum == count {
          continue
        }
        for i:=0; i < NShards && 0 < avgNum-count; i++ {
          if cfg.Shards[i] <= 0 {
            cfg.Shards[i] = gid
          }
        }
      }
    }
  }
}
// the above helper functions added by Shusen Xu



//  react by creating a new configuration that includes the new replica group.
// The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  // added by Shusen Xu
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.ProcessOp(Op{"Join", args.GID, args.Servers, 0, sm.CreatePid()})
  return nil
}


//  create a new configuration that does not include the group,
//  and that assigns the group’s shards to the remaining groups.
//  The new configuration should divide the shards as evenly as possible among the groups,
//  and should move as few shards as possible to achieve that goal.
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  // added by Shusen Xu
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.ProcessOp(Op{"Leave", args.GID, nil, 0, sm.CreatePid()})
  return nil
}

// create a new configuration in which the shard is assigned to the group
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  // added by Shusen Xu
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.ProcessOp(Op{"Move", args.GID, nil, args.Shard, sm.CreatePid()})
  return nil
}


// If the number is -1 or bigger than the biggest known configuration number,
// the shardmaster should reply with the latest configuration.
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  // added by Shusen Xu
  sm.mu.Lock()
  defer sm.mu.Unlock()
  sm.ProcessOp(Op{"Query", 0, nil, 0, sm.CreatePid()})
  if args.Num == -1 || args.Num >= len(sm.configs) {
    // reply with latest configurations
    reply.Config = sm.configs[len(sm.configs)-1]
  } else {
    reply.Config = sm.configs[args.Num]
  }
  return nil
}


// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
