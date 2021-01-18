package pbservice

import (
  "crypto/rand"
  "hash/fnv"
  "math/big"
)

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  // added by Shusen Xu

  // Field names must start with capital letters,
  // otherwise RPC will break.
  Forward bool // identify whether it's primary => backup forward request
  UUID string  //  used for outstanding RPC request
  Me string // clerk's identification
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  // added by Shusen Xu
  UUID int64
}

type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.
// added by Shusen Xu
// a list of RPC definitions
type AppendArgs struct {
  Content map[string]string
}

type AppendReply struct {
  Err Err
}

type InitStateArgs struct {
  Content map[string]string
}

type InitStateReply struct {
  Err   Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func nrand() int64 {
  max := big.NewInt(int64(int64(1) << 62))
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}


