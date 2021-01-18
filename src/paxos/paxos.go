package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
  "net"
)
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
  mu         sync.Mutex
  l          net.Listener
  dead       bool
  unreliable bool
  rpcCount   int
  peers      []string
  me         int // index into peers[]

  // Your data here.
  acceptorLock sync.Mutex
  instances    InstanceMap
  maxSeq       int
  doneMap      []int
  toCleanSeq   int
}

type Proposer struct {
  proposedNumber            int
  highestSeenProposedNumber int
}

type Acceptor struct {
  highestProposedNumber int
  highestAcceptedNumber int
  highestAcceptedValue  interface{}
}

type Instance struct {
  mu           sync.Mutex
  proposer     Proposer
  acceptor     Acceptor
  decidedValue interface{}
}

type InstanceMap map[int]*Instance

func (px *Paxos) getInstance(seq int) *Instance {
  px.mu.Lock()
  defer px.mu.Unlock()
  if _, ok := px.instances[seq]; !ok {
    px.instances[seq] = &Instance{}
  }
  return px.instances[seq]
}

const (
  PROPOSE = "PROPOSE"
  ACCEPT  = "ACCEPT"
  DECIDE  = "DECIDE"
)

type Proposal struct {
  Type        string
  ProposedNum int
  Seq         int
  Value       interface{}
  Meta        MetaData
}

type Response struct {
  Type     string
  Approved bool
  Number   int
  Value    interface{}
  Meta     MetaData
}

type MetaData struct {
  Me   int
  Done int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  if seq < px.Min() {
    return
  }
  go func() {
    instance := px.getInstance(seq)
    instance.mu.Lock()
    defer instance.mu.Unlock()
    for !px.dead {
      if instance.decidedValue != nil {
        break
      }
      instance.proposer.highestSeenProposedNumber++
      instance.proposer.proposedNumber = instance.proposer.highestSeenProposedNumber
      ok, value := px.propose(instance, seq)
      if !ok {
        continue
      }
      if value != nil {
        v = value
      }
      if !px.requestAccept(instance, seq, v) {
        continue
      }
      px.decide(seq, v)
      break
    }
  }()
}

func (px *Paxos) propose(instance *Instance, seq int) (bool, interface{}) {
  count := 0
  highestProposedNumber := instance.proposer.highestSeenProposedNumber
  highestAcceptedNumber := -1
  var highestAcceptedValue interface{}
  for i := 0; i < len(px.peers); i++ {
    args := &Proposal{PROPOSE, instance.proposer.proposedNumber, seq, nil, px.initMeta()}
    reply := Response{}
    flag := true
    if i != px.me {
      flag = call(px.peers[i], "Paxos.Receive", args, &reply)
    } else {
      px.Receive(args, &reply)
    }
    if !flag {
      continue
    }
    px.updateMeta(reply.Meta)
    if reply.Approved {
      count++
      if reply.Number > highestAcceptedNumber {
        //fmt.Printf("Get Propose reponse: %d back to %d, seq: %d, N_a: %d, N_v: %d \n", reply.Meta.Me, px.me, seq, reply.Number, reply.Value)
        highestAcceptedNumber = reply.Number
        highestAcceptedValue = reply.Value
      }
    } else {
      highestProposedNumber = max(highestProposedNumber, reply.Number)
    }
  }
  // make sure the proposer get the latest and max proposed number
  instance.proposer.highestSeenProposedNumber = highestProposedNumber
  if count > len(px.peers)/2 {
    return true, highestAcceptedValue
  } else {
    return false, nil
  }
}

func (px *Paxos) requestAccept(instance *Instance, seq int, value interface{}) bool {
  highestProposedNumber := instance.proposer.highestSeenProposedNumber
  count := 0
  for i := 0; i < len(px.peers); i++ {
    args := &Proposal{ACCEPT, instance.proposer.proposedNumber, seq, value, px.initMeta()}
    reply := Response{}
    flag := true
    if i != px.me {
      flag = call(px.peers[i], "Paxos.Receive", args, &reply)
    } else {
      px.Receive(args, &reply)
    }
    if flag {
      px.updateMeta(reply.Meta)
      if reply.Approved {
        count++
      } else {
        highestProposedNumber = max(highestProposedNumber, reply.Number)
      }
    }
  }
  // make sure the proposer get the latest and max proposed number
  instance.proposer.highestSeenProposedNumber = highestProposedNumber
  return count > len(px.peers)/2
}

func (px *Paxos) decide(seq int, value interface{}) {
  var records = make([]bool, len(px.peers))
  for i := range records {
    records[i] = false
  }
  count := 0

  for i := 0; count < len(px.peers); i = (i + 1) % len(px.peers) {
    if records[i] {
      continue
    }
    args := &Proposal{DECIDE, 0, seq, value, px.initMeta()}
    reply := Response{}
    flag := true
    if i != px.me {
      flag = call(px.peers[i], "Paxos.Receive", args, &reply)
    } else {
      px.Receive(args, &reply)
    }
    if flag {
      px.updateMeta(reply.Meta)
      if reply.Approved {
        count++
        records[i] = true
      }
    }
  }
}

func (px *Paxos) Receive(proposal *Proposal, response *Response) error {
  px.updateMeta(proposal.Meta)
  px.acceptorLock.Lock()
  defer px.acceptorLock.Unlock()
  instance := px.getInstance(proposal.Seq)

  response.Meta = px.initMeta()
  response.Type = proposal.Type
  if proposal.Type == PROPOSE {
    if proposal.ProposedNum <= instance.acceptor.highestProposedNumber {
      response.Approved = false
      response.Number = instance.acceptor.highestProposedNumber
    } else {
      instance.acceptor.highestProposedNumber = proposal.ProposedNum
      response.Approved = true
      response.Number = instance.acceptor.highestAcceptedNumber
      response.Value = instance.acceptor.highestAcceptedValue
    }
  } else if proposal.Type == ACCEPT {
    if proposal.ProposedNum < instance.acceptor.highestProposedNumber {
      response.Approved = false
      response.Number = instance.acceptor.highestProposedNumber
    } else {
      //fmt.Printf("Accept %d to %d, seq: %d, N: %d, V: %d \n",
      //  proposal.Meta.Me, px.me, proposal.Seq, proposal.ProposedNum, proposal.Value)
      instance.acceptor.highestProposedNumber = proposal.ProposedNum
      instance.acceptor.highestAcceptedNumber = proposal.ProposedNum
      instance.acceptor.highestAcceptedValue = proposal.Value
      response.Approved = true
      response.Number = proposal.ProposedNum
    }
  } else if proposal.Type == DECIDE {
    instance.decidedValue = proposal.Value
    px.maxSeq = max(px.maxSeq, proposal.Seq)
    response.Approved = true
  }
  return nil
}

func (px *Paxos) initMeta() MetaData {
  return MetaData{
    Me:   px.me,
    Done: px.doneMap[px.me],
  }
}

func (px *Paxos) updateMeta(meta MetaData) {
  px.mu.Lock()
  defer px.mu.Unlock()
  px.doneMap[meta.Me] = max(px.doneMap[meta.Me], meta.Done)
  px.cleanDoneValues()
}

func (px *Paxos) cleanDoneValues() {
  end := px.Min()
  for seq := px.toCleanSeq; seq < end; seq++ {
    delete(px.instances, seq)
  }
  px.toCleanSeq = end
}

func max(a, b int) int {
  if a > b {
    return a
  }
  return b
}

func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}

func minOfArray(arr []int) int {
  if len(arr) == 0 {
    return -1
  }
  if len(arr) == 1 {
    return arr[0]
  }
  res := arr[0]
  for i := 1; i < len(arr); i++ {
    res = min(res, arr[i])
  }
  return res
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  px.doneMap[px.me] = max(px.doneMap[px.me], seq)
  px.cleanDoneValues()
  return
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  // You code here.
  return minOfArray(px.doneMap) + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  instance, ok := px.instances[seq]
  if !ok {
    return false, nil
  }

  return instance.decidedValue != nil, instance.decidedValue
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me

  // Your initialization code here.
  px.instances = make(map[int]*Instance)
  px.doneMap = make([]int, len(px.peers))
  for i := 0; i < len(px.doneMap); i++ {
    px.doneMap[i] = -1
  }
  px.maxSeq = -1

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me])
    if e != nil {
      log.Fatal("listen error: ", e)
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63()%1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63()%1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }

  return px
}
