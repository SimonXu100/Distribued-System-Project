package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}


// added by Shusen Xu
// part 2
// part 3
func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  // Modified by Shusen Xu
  for i := 0; i < mr.nMap; i++ {
    worker := <- mr.registerChannel
    args := &DoJobArgs{}
    var reply DoJobReply
    args.Operation = Map
    args.JobNumber = i
    args.File = mr.file
    args.NumOtherPhase = mr.nReduce
    ok := call(worker, "Worker.DoJob", args, &reply)
    fmt.Println("rpc call status", ok)

    // when tasks completed, release this worker
    // push it back to registerChannel
    if ok {
      go func ()  {
        mr.registerChannel <- worker
      }()
    }else{
      // DoMap failed, re-execute
      fmt.Printf("Worker %v doMap failed.\n", i)
      i = i -1
    }
  }

  for i := 0; i < mr.nReduce; i++ {
   worker := <- mr.registerChannel
   args := &DoJobArgs{}
   var reply DoJobReply
   args.Operation = Reduce
   args.JobNumber = i
   args.File = mr.file
   args.NumOtherPhase = mr.nMap
   ok := call(worker, "Worker.DoJob", args, &reply)
   fmt.Println("rpc call status", ok)
   // when tasks completed, release this worker
   if ok {
     go func() {
       mr.registerChannel <- worker
     }()
   }else{
     // DoReduce failed, re-execute
     fmt.Printf("Worker %v DoReduce failed.\n", i)
     i = i -1
   }
  }
  return mr.KillWorkers()
}
