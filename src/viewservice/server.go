package viewservice

import (
  "net"
)
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  // added by Shusen Xu
  currentView *View
  newView *View
  isPrimaryAck bool
  primaryTick int
  backupTick  int
  //  (a server that’s been Pinging but is neither the primary nor the backup
  idleServers map[string]int    // track available extra server(server port name  ---> ticks)
}


// the following is helper functions added by Shusen Xu
func(vs *ViewServer) isSwitch() bool {
  if vs.currentView.Backup == "" && len(vs.idleServers) == 0{
    return false
  }
  if vs.primaryTick > 0 && vs.backupTick <= 0{
    // no alive backup, find from extra idle servers as backup server
    availableServer := ""
    if len(vs.idleServers) == 0{
      availableServer = ""
    }else{
      for server := range vs.idleServers {
        delete(vs.idleServers, server)
        availableServer = server
      }
    }
    vs.setNewView(vs.currentView.Primary, availableServer)
  }else{
    if vs.primaryTick <= 0{
      // the primary is dead or crashed
      if vs.backupTick > 0{
        availableServer := ""
        if len(vs.idleServers) == 0{
          availableServer = ""
        }else{
          for server := range vs.idleServers {
            delete(vs.idleServers, server)
            availableServer = server
          }
        }
        vs.setNewView(vs.currentView.Backup, availableServer)

      }else{
        // both primary and backup are dead
        // cannot use uninitialized idle servers
        vs.setNewView("", "")
      }
    }
  }
  // switch
  if vs.newView != nil {
    vs.currentView, vs.newView = vs.newView, nil
    return true
  }
  return false
}


func(vs *ViewServer) setNewView(p string, b string){
  if vs.currentView != nil{
    if vs.newView == nil{
      // create new empty view
      tmpView := new(View)
      tmpView.Viewnum = vs.currentView.Viewnum + 1
      tmpView.Primary = p
      tmpView.Backup  = b
      vs.newView = tmpView
    }else{
      vs.newView.Primary = p
      vs.newView.Backup = b
    }
  }
}
// the above is helper functions added by Shusen XU


//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  // Your code here.
  // added by Shusen Xu
  vs.mu.Lock()
  defer vs.mu.Unlock()
  ck := args.Me
  viewNum := args.Viewnum
  if viewNum == 0{
    if vs.currentView == nil{
      // when the viewservice first starts, it should accept any server at all as the first primary
      tmpView := new(View)
      tmpView.Viewnum = 1
      tmpView.Primary = ck
      tmpView.Backup  = ""
      vs.currentView = tmpView
    }else{
      if ck == vs.currentView.Primary{
        // restarted primary, treated like dead
        vs.primaryTick = 0
        if vs.isPrimaryAck && vs.isSwitch(){
          vs.isPrimaryAck = false
        }
      }
      if ck != "" && ck != vs.currentView.Backup {
        vs.idleServers[ck] = DeadPings
      }
    }
  }else{
    if ck == vs.currentView.Primary{
      if viewNum == vs.currentView.Viewnum{
        // switch to new view
        if vs.newView != nil {
          vs.currentView, vs.newView = vs.newView, nil
          vs.isPrimaryAck = false
        }
        // acknowledged
        vs.isPrimaryAck = true
      }
    }
  }

  // calculate Ticks
  if ck == vs.currentView.Primary{
    vs.primaryTick = DeadPings
  }else if ck == vs.currentView.Backup{
    vs.backupTick = DeadPings
  }else{
    vs.idleServers[ck] = DeadPings
  }
  reply.View = *vs.currentView
  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  // added by Shusen Xu
  vs.mu.Lock()
  defer vs.mu.Unlock()

  if vs.currentView == nil {
    // create new empty view
    tmpView := new(View)
    tmpView.Viewnum = 0
    tmpView.Primary = ""
    tmpView.Backup  = ""
    reply.View = *tmpView
  } else {
    reply.View = *vs.currentView
  }
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  // added by Shusen Xu
  // hint:  your viewservice needs to make periodic decisions,
  // promote the backup if the viewservice has missed DeadPings pings from the primary.
  if vs.currentView != nil{
    // clean all dead server
    for server := range vs.idleServers{
      if vs.idleServers[server] <=0 {
        delete(vs.idleServers, server)
      }else{
        // for every tick, update ticks value for every server
        vs.idleServers[server]--
      }
    }
    // intend to change to the new view
    if vs.isPrimaryAck && vs.isSwitch() {
      vs.isPrimaryAck = false
    }
    // update primary tick value and backup ticks value
    if vs.currentView.Primary == ""{
      vs.primaryTick = 0
    }
    if vs.currentView.Backup  == "" {
      vs.backupTick = 0
    }
    if vs.primaryTick > 0{
      vs.primaryTick--
    }
    if vs.backupTick > 0{
      vs.backupTick--
    }
  }

}
//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  // added by Shusen Xu
  vs.idleServers = make(map[string] int)


  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
