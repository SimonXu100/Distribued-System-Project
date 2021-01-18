package kvpaxos

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

const Debug = 0

const (
	PUT      = "PUT"
	GET      = "GET"
	PUT_HASH = "PUTHASH"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Timestamp int64
	ClientID  int64
	Type      string
	Key       string
	Value     string
}

type Response struct {
	timestamp int64
	err       Err
	value     string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db             map[string]string
	committedSeq   int
	latestRequests map[int64]*Response
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Timestamp: args.Timestamp,
		ClientID:  args.ClientId,
		Type:      GET,
		Key:       args.Key,
	}
	err, value := kv.writeLog(op)
	reply.Value = value
	reply.Err = err
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var op Op
	if args.DoHash {
		op = Op{
			Timestamp: args.Timestamp,
			ClientID:  args.ClientId,
			Type:      PUT_HASH,
			Key:       args.Key,
			Value:     args.Value,
		}
	} else {
		op = Op{
			Timestamp: args.Timestamp,
			ClientID:  args.ClientId,
			Type:      PUT,
			Key:       args.Key,
			Value:     args.Value,
		}
	}
	_, value := kv.writeLog(op)
	if args.DoHash {
		reply.PreviousValue = value
	}
	return nil
}

func (kv *KVPaxos) writeLog(op Op) (Err, string) {
	// Your code here.
	latestResponse, ok := kv.latestRequests[op.ClientID]
	if ok {
		if op.Timestamp == latestResponse.timestamp {
			return latestResponse.err, latestResponse.value
		} else if op.Timestamp < latestResponse.timestamp {
			return OutdatedRequest, ""
		}
	}
	for {
		seq := kv.committedSeq + 1
		kv.px.Start(seq, op)
		returnedOp := kv.getLog(seq)
		err, value := kv.executeLog(returnedOp)
		kv.latestRequests[returnedOp.ClientID] = &Response{returnedOp.Timestamp, err, value}
		kv.committedSeq++
		if returnedOp.ClientID == op.ClientID && returnedOp.Timestamp == op.Timestamp {
			kv.px.Done(kv.committedSeq)
			return err, value
		}
	}
}

func (kv *KVPaxos) executeLog(op Op) (Err, string) {
	if op.Type == GET {
		value, ok := kv.db[op.Key]
		if ok {
			return "", value
		} else {
			return ErrNoKey, ""
		}
	} else if op.Type == PUT {
		kv.db[op.Key] = op.Value
		return "", ""
	} else if op.Type == PUT_HASH {
		value, _ := kv.db[op.Key]
		h := hash(value + op.Value)
		kv.db[op.Key] = strconv.Itoa(int(h))
		return "", value
	}
	return "Unknown Type", ""
}

func (kv *KVPaxos) getLog(seq int) Op {
	var returnedRaw interface{}
	decided := false
	for !decided {
		decided, returnedRaw = kv.px.Status(seq)
		time.Sleep(20 * time.Millisecond)
	}
	return returnedRaw.(Op)
}

func (kv *KVPaxos) autoUpdate() {
	for !kv.dead {
		kv.mu.Lock()
		// update here
		maxSeq := kv.px.Max()
		for kv.committedSeq < maxSeq {
			op := kv.getLog(kv.committedSeq + 1)
			err, value := kv.executeLog(op)
			kv.latestRequests[op.ClientID] = &Response{op.Timestamp, err, value}
			kv.committedSeq++
		}
		kv.px.Done(kv.committedSeq)
		kv.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.latestRequests = make(map[int64]*Response)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	// start my auto update
	go kv.autoUpdate()
	return kv
}
