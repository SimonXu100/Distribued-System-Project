# Paxos-based Sharded Key/Value Database

## Introduction

The project implements a distributed key/value storage system that supports concurrent *Put*, *Get* and *PutHash* requests from multiple clients with sequential consistency. The database realizes scalability and fault tolerance by - respectively - sharding  keys over a set of replica groups and incorporating Paxos protocol in every replica group. 

A shard is a subset of all key/value pairs in the database. For example, all the keys starting with "a" can be a shard, all the keys starting with "b" another, etc. The reason for sharding is scalability. Each replica group handles client requests for just a few shards, and different groups operate in parallel, thus total system throughput (the number of puts and gets per unit time) increases in proportion to the number of groups. 

The sharded key/value database has two main components. 

1. A set of replica groups. Each replica group is responsible for a subset of all the shards. A replica consists of a handful of servers (kvserver) that use Paxos to replicate the group's shard. 

2. The shard master. It decides which replica group should serve each shard; this information is called the configuration. 

Configuration changes over time. Clients consult the shard master in order to find the replica group for a key, and replica groups consult the master in order to find out what shards to serve. There is a single shard master for the whole system, implemented as a fault-tolerant service using Paxos.

Only RPC may be used for interaction between clients and servers, between different servers, and between different clients. 

The general architecture (a configuration service and a set of replica groups) of the project is patterned at a high level on a number of systems: Flat Datacenter Storage, BigTable, Spanner, FAWN, Apache HBase, Rosebud, and many others. These systems differ in many details from this project, though, and are also typically more sophisticated and capable. For example, the project lacks persistent storage for key/value pairs and for the Paxos log; it sends more messages than required per Paxos agreement; it cannot evolve the sets of peers in each Paxos group; its data and query models are very simple; and handoff of shards is slow and doesn't allow concurrent client access.

------------

### Shardmaster

The shard master is responsible for configuration, that is which server is responsible for handling client requests of a certain shard. 

A sharded storage system must be able to shift shards among replica groups. One reason is that some groups may become more loaded than others, so that shards need to be moved to balance the load. Another reason is that replica groups may join and leave the system: new replica groups may be added to increase capacity, or existing replica groups may be taken offline for repair or retirement.

The main challenge is handling reconfiguration in the replica groups. Within a single replica group, all group members must agree on when a reconfiguration occurs relative to client requests. For example, a Put may arrive at about the same time as a reconfiguration that causes the replica group to stop being responsible for the shard holding the Put's key. 

The shardmaster manages a sequence of numbered configurations. Each configuration describes a set of replica groups and an assignment of shards to replica groups. Whenever this assignment needs to change, the shard master creates a new configuration with the new assignment. Clients and kvservers contact the shardmaster when they want to know the current (or a past) configuration.

### Paxos

As a consensus protocol, Paxos supports an indefinite sequence of agreement instances numbered with sequence numbers. Each Paxos instance is a log, and the order of operations in the log is the order in which all kvservers within a replica group will apply the operations to their key/value databases. Paxos will ensure that the kvpaxos servers agree on this order. 

A set of replicas will process all client requests in the same order, using Paxos to agree on the order. Paxos will get the agreement right even if some of the replica servers are unavailable, or have unreliable network connections, or even if subsets of the replica servers are isolated in their own network partitions. 

### KVServer

Kvserver is the basic unit for data storage. A replica group is made up of several kvservers with Paxos to ensure the data availability and durability, and identical operation sequence. Each kvserver within a group is a replica of others, as they store the same information.

We can assume that a majority of servers in each Paxos replica group are alive and can talk to each other, can talk to a majority of the shardmaster servers, and can talk to a majority of each other replica group. The implementation is able to operate (serve requests and be able to re-configure as needed) if a minority of servers in some replica group(s) are dead, temporarily unavailable, or slow.

Kvserver within a replica group should stay identical; the only exception is that some replicas may lag others if they are not reachable. If a replica isn't reachable for a while, but then starts being reachable, it should eventually catch up (learn about operations that it missed).

It is ensured that at most one replica group is serving requests for each shard, as it is reasonable to assume that each replica group is always available, because each group uses Paxos for replication and thus can tolerate some network and server failures. 

### Client

Client can send requests via RPC to replica groups, which are *Put(key, val)* - to update the value of a key, *Get(key)* - to get current value of a key, and *PutHash(key, val)* - to update the value of a key and retrieve its previous value, all stored in hash value, where new_val = hash(prv_val + val).

The storage system provides sequential consistency to applications that use its client interface. That is, completed application calls to the *Clerk.Get()*, *Clerk.Put()*, and *Clerk.PutHash()* methods in shardkv/client.go must appear to have affected all replicas in the same order and have at-most-once semantics. A *Clerk.Get()* should see the value written by the most recent *Put()/PutHash()* to the same key. 

One consequence of this is that you must ensure that each application call to *Clerk.Put()* must appear in that order just once (i.e., write the key/value database just once), even though internally your client.go may have to send *Put()* and *PutHash()* RPCs multiple times until it finds a kvpaxos server replica that replies due to unreliable network. This will get tricky when Gets and Puts arrive at about the same time as configuration changes.

## Usage

To test the script, navigate to the corresponding folder and set GOPATH before running

`go test`

To run a single test function in the test_test.go file, run

`go test -run FUNCTION_NAME`

To avoid warnings, run 

`go test | egrep -v "keyword1|keyword2|keyword3"`

















Additional Instructions
* [MapReduce](instructions/MapReduce.md)
* [Primary-Backup Key/Value Service](instructions/PB-KV.md)
* [Paxos-based Key/Value Service](instructions/Paxos-KV.md)
* [Paxos-based Sharded Key/Value Service](instructions/Shard-KV.md)
* Model Checking Paxos: part5_pkg
