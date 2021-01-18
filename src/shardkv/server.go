package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

const (
  OpGet  = "Get"
  OpPut  = "Put"
  OpJoin = "Join"
  OpLeave = "Leave"
)
// helper struct added by Shusen Xu
type Value struct {
  Val     string
  Version int
}

type Op struct {
  // Your definitions here.
  Op   string
  Key  string
  Val  string
  Hash bool
  Pid  string
  Data map[string]Value
  ReqData  map[string]kvdata
  seqNum  int
}

type kvdata struct {
  Key string
  Val string
}

type State struct {
  data   map[string]kvdata
  reqmap map[string]string
  queue  []string
  head   int
  tail   int
  size   int
}

type ShardKV struct {
  mu         sync.Mutex
  l          net.Listener
  me         int
  dead       bool // for testing
  unreliable bool // for testing
  sm         *shardmaster.Clerk
  px         *paxos.Paxos
  gid        int64 // my replica group ID
  // Your definitions here.
  cfg        *shardmaster.Config
  configLock    sync.Mutex
  data       map[string]Value
  seq        int
  state         State
}
// the above are helper structs added by Shusen Xu

// the following are helper functions added by Shusen Xu
func (state *State) Check(pid string) bool {
  _, ok := state.data[pid]
  return ok
}

func (state *State) Append(pid string, key string, val string) {
  if len(state.data) == state.size {
    oldpid := state.queue[state.head]
    kv := state.data[oldpid]
    delete(state.data, pid)
    delete(state.reqmap, kv.Key)
    state.head = (state.head + 1) % state.size
  }

  state.queue[state.tail] = pid
  state.tail = (state.tail + 1) % state.size
  state.data[pid] = kvdata{key, val}
  state.reqmap[key] = pid
}

func (state *State) GetByKey(key string) (string, string) {
  return state.reqmap[key], state.data[state.reqmap[key]].Val
}

func (kv *ShardKV) waitForAgreement(seq int) Op {
  to := 10 * time.Millisecond
  for {
    if ok, val := kv.px.Status(seq); ok {
      return val.(Op)
    }
    time.Sleep(to)
    if to < 10*time.Second {
      to *= 2
    }
  }
}

func (kv *ShardKV) ProcessHelper(op Op) {

  switch op.Op {
  case OpGet:
    //val, _ := kv.data[o.Key]
    //kv.v.Add(o.Pid, val)
    DPrintf("Get result key:%s ret:%s gid:%d seq:%d pid:%s\n",
      op.Key, kv.data[op.Key].Val, kv.gid, kv.cfg.Num, op.Pid)

  case OpPut:
    oldv, _ := kv.data[op.Key]
    if op.Hash {
      newval := strconv.Itoa(int(hash(oldv.Val + op.Val)))
      kv.data[op.Key] = Value{newval, oldv.Version + 1}

      kv.state.Append(op.Pid, op.Key, oldv.Val)
    } else {
      kv.data[op.Key] = Value{op.Val, oldv.Version + 1}
    }
  case OpJoin:
    for key, val := range op.Data {
      if val.Version > kv.data[key].Version {
        kv.data[key] = val
      }
    }

    for pid, v := range op.ReqData {
      kv.state.Append(pid, v.Key, v.Val)
    }
  case OpLeave:
    newdata := make(map[string]Value)
    for key, val := range kv.data {
      newdata[key] = val
    }
    for key, _ := range op.Data {
      delete(newdata, key)
    }
    kv.data = newdata
  }
}

func (kv *ShardKV) ProcessOp(op Op) {
  var tmpOp Op
  if kv.state.Check(op.Pid) {
    return
  }
  //fmt.Printf("begin to sync %#v\n", o)
  for {
    ok, v := kv.px.Status(kv.seq + 1)
    if ok {
      tmpOp = v.(Op)
    } else {
      kv.px.Start(kv.seq+1, op)
      tmpOp = kv.waitForAgreement(kv.seq + 1)
    }

    kv.ProcessHelper(tmpOp)
    kv.seq++
    kv.px.Done(kv.seq)
    if tmpOp.Pid == op.Pid {
      break
    }
  }
}

func (kv *ShardKV) GetShard(req *GetShardArgs, rsp *GetShardReply) error {
  kv.mu.Lock()

  // Use a fake operation to synchorize the data
  fake := Op{Op: OpGet, Key: "tmp", Pid: kv.getPid(int(rand.Int31()), int(rand.Int31()))}
  kv.ProcessOp(fake)

  op := Op{Op: OpLeave, Data: make(map[string]Value)}
  rsp.Data = make(map[string]Value)
  rsp.ReqData = make(map[string]kvdata)
  for k, v := range kv.data {

    if req.Shard == key2shard(k) {
      rsp.Data[k] = v
      op.Data[k] = v
      pid, val := kv.state.GetByKey(k)
      rsp.ReqData[pid] = kvdata{k, val}
    }
  }
  kv.cfg.Shards[req.Shard] = req.Gid
  kv.mu.Unlock()
  return nil
}

func (kv *ShardKV) getPid(cfgnum int, shard int) string {
  return fmt.Sprintf("ChangeJoinState_%d_%d", cfgnum, shard)
}

func (kv *ShardKV) Reconfig(cfg *shardmaster.Config) {
  for i := kv.cfg.Num + 1; i <= cfg.Num; i++ {
    ncfg := kv.sm.Query(i)
    if kv.cfg.Num == 0 {
      kv.cfg = &ncfg
      continue
    }
    // query new config
    for j := 0; j < shardmaster.NShards; j++ {
      if kv.cfg.Shards[j] != kv.gid && ncfg.Shards[j] == kv.gid {
        req := GetShardArgs{ncfg.Num, j, kv.gid}
        rsp := GetShardReply{}
        svrs := kv.cfg.Groups[kv.cfg.Shards[j]]
        pos := 0
        for len(svrs) > 0 {
          if call(svrs[pos], "ShardKV.GetShard", &req, &rsp) {
            break
          }
          pos = (pos + 1) % len(svrs)
        }

        if len(rsp.Data) > 0 {
          kv.mu.Lock()
          kv.ProcessOp(Op{Op: OpJoin, Pid: kv.getPid(cfg.Num, j),
            Data: rsp.Data, ReqData: rsp.ReqData, seqNum: cfg.Num})
          kv.mu.Unlock()
        }
        kv.cfg.Shards[j] = kv.gid
      }
    }

    kv.cfg = &ncfg

  }
}
// the above  are helper functions added by Shusen Xu

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  // added by Shusen Xu
  kv.mu.Lock()
  defer kv.mu.Unlock()

  if kv.cfg.Shards[key2shard(args.Key)] != kv.gid {
    // Get error, not my group
    reply.Err = ErrWrongGroup
    return nil
  }

  reply.Err = OK
  kv.ProcessOp(Op{Op: OpGet, Key: args.Key, Pid: args.Pid})
  reply.Value = kv.data[args.Key].Val
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  // added by Shusen Xu
  kv.mu.Lock()
  defer kv.mu.Unlock()

  if kv.cfg.Shards[key2shard(args.Key)] != kv.gid {
    // Set error group
    reply.Err = ErrWrongGroup
    return nil

  }

  reply.Err = OK
  reply.PreviousValue = ""
  kv.ProcessOp(Op{Op: OpPut, Key: args.Key, Val: args.Value,
    Pid: args.Pid, Hash: args.DoHash})

  if val, ok := kv.state.data[args.Pid]; ok {
    reply.PreviousValue = val.Val
  }

  return nil
}

func (kv *ShardKV) tick() {
  kv.configLock.Lock()
  defer kv.configLock.Unlock()
  config := kv.sm.Query(-1)
  if config.Num > kv.cfg.Num {
    kv.Reconfig(&config)
  }
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
    servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.cfg = &shardmaster.Config{Num: 0}
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  // Your initialization code here.
  kv.data = make(map[string]Value)
  kv.state.data = make(map[string]kvdata)
  kv.state.reqmap = make(map[string]string)
  kv.state.size = 1024
  kv.state.queue = make([]string, 1024)
  // Don't call Join().


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me])
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63()%1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()
  return kv
}