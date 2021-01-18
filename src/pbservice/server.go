package pbservice

import (
  "errors"
  "net"
  "strconv"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  // added by Shusen Xu

  view viewservice.View
  // content contains three types of value:
  // previous.client-name => uuid of rpc
  // previousReply.client-name => reply value of last rpc
  // key => value
  content map[string] string
  mu sync.Mutex
  isinitBackup bool    // when backup is created, check if it has been initialized by current primary

}

// helper function added by Shusen Xu
func (pb *PBServer) isPrimary() bool{
  return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool{
  return pb.view.Backup == pb.me
}

func (pb *PBServer) hasPrimary() bool{
  return pb.view.Primary != ""
}

func (pb *PBServer) hasBackup() bool{
  return pb.view.Backup != ""
}

func (pb *PBServer) Append(args *AppendArgs) error {
  if !pb.hasBackup(){
    return nil
  }
  var reply AppendReply
  ok := call(pb.view.Backup, "PBServer.DoAppend", args, &reply)
  if !ok {
    return errors.New("doappend fail")
  }
  return nil
}

func (pb *PBServer) DoAppend(args *AppendArgs, reply *AppendReply) error{
  pb.mu.Lock()
  if !pb.isBackup(){
    pb.mu.Unlock()
    return errors.New("wrong server: not backup server")
  }

  for key,value := range args.Content{
    pb.content[key] = value
  }
  pb.mu.Unlock()
  return nil
}

// for update concurrency
func(pb *PBServer) InitBackup(args *InitStateArgs, reply *InitStateReply) error{
  pb.mu.Lock()
  defer pb.mu.Unlock()
  if pb.isBackup() && !pb.isinitBackup {
    pb.isinitBackup = true
    //pb.content = args.Content
    for key,value := range args.Content{
     pb.content[key] = value
    }
  }
  reply.Err = OK
  return nil
}
// the above are helper function added by Shusen Xu


func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  // added by Shusen Xu
  pb.mu.Lock()
  if !pb.isPrimary(){
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return errors.New("[put] wrong server, no primary")
  }

  //// update primary's data into backup and syn
  //var initReply InitStateReply
  //tmpArgs := InitStateArgs{pb.content}
  //call(pb.view.Backup, "PBServer.InitBackup", tmpArgs, &initReply)

  key, value, client, uid := args.Key, args.Value, args.Me, args.UUID

  // for AtMostOne case
  // client identification, here for filtering duplication
  if pb.content["previous." + client] == uid{
    reply.PreviousValue = pb.content["previousReply." + client]
    pb.mu.Unlock()
    return nil
  }

  if args.DoHash{
    reply.PreviousValue = pb.content[key]
    value = strconv.Itoa(int(hash(reply.PreviousValue + value)))
  }

  // Append
  appendArgs := &AppendArgs{map[string]string{
    key : value,
    "previous."+client : uid,
    "previousReply."+client : reply.PreviousValue}}
  err := pb.Append(appendArgs)
  if err != nil{
   pb.mu.Unlock()
   return errors.New("append fail")
  }
  for key, value := range appendArgs.Content{
    pb.content[key] = value
  }

  pb.mu.Unlock()
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  // added by Shusen Xu
  pb.mu.Lock()
  if !pb.isPrimary() {
    reply.Err = ErrWrongServer
    pb.mu.Unlock()
    return errors.New("[get] wrong server: not primary but received Get request")
  }

  // update primary's data into backup and syn
  var initReply InitStateReply
  tmpArgs := InitStateArgs{pb.content}
  call(pb.view.Backup, "PBServer.InitBackup", tmpArgs, &initReply)

  reply.Value = pb.content[args.Key]
  pb.mu.Unlock()
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  // added by Shusen Xu
  pb.mu.Lock()
  defer pb.mu.Unlock()
  view, err := pb.vs.Ping(pb.view.Viewnum)
  if err != nil {
  }
  // update primary's data into backup every tick
  pb.view = view
  if view.Backup != "" && view.Backup != pb.view.Backup && pb.isPrimary() {
    pb.Append(&AppendArgs{Content:pb.content})
  }
  // update initBackup flag
  //if !pb.isBackup() && !pb.isPrimary(){
  //  pb.isinitBackup = false
  //}

}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  // added by Shusen Xu
  pb.content = map[string]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait()
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
