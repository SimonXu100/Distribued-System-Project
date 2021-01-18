package paxos

import (
	"coms4113/hw5/pkg/base"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	switch message.(type) {
	case *ProposeResponse:
		return server.proposerHandler(message, Propose)
	case *AcceptResponse:
		return server.proposerHandler(message, Accept)
	case *ProposeRequest:
		return server.acceptorHandler(message, Propose)
	case *AcceptRequest:
		return server.acceptorHandler(message, Accept)
	case *DecideRequest:
		return server.acceptorHandler(message, Decide)
	default:
		return []base.Node{server}
	}
}

func (server *Server) address2Index(target base.Address) int {
	for i, addr := range server.peers {
		if target == addr {
			return i
		}
	}

	return -1
}

// To start a new round of Paxos.
func (server *Server) StartPropose() {
	server.sendPropose()
}

func (server *Server) proposerHandler(message base.Message, phase string) []base.Node {
	switch phase {
	case Propose:
		if server.proposer.Phase != Propose {
			return []base.Node{server}
		}

		response, ok := message.(*ProposeResponse)
		if !ok {
			return []base.Node{server}
		}

		i := server.address2Index(response.From())
		if i == -1 || server.proposer.Responses[i] {
			return []base.Node{server}
		}

		if response.SessionId != server.proposer.SessionId {
			return []base.Node{server}
		}

		subNode := server.copy()
		subNode.proposer.Responses[i] = true
		subNode.proposer.ResponseCount++

		if response.Ok {
			subNode.proposer.SuccessCount++

			if response.N_a > subNode.proposer.N_a_max {
				subNode.proposer.N_a_max = response.N_a
				subNode.proposer.V = response.V_a
			}
		} else {
			subNode.n_p = max(subNode.n_p, response.N_p)
		}

		subNodes := make([]base.Node, 0, 2)

		// Acknowledged by majority of peers, proceed to next phase
		if subNode.proposer.SuccessCount > len(subNode.peers)/2 {
			newPhaseNode := subNode.copy()
			newPhaseNode.sendAccept()
			subNodes = append(subNodes, newPhaseNode)
		}

		// if not getting response from every peer, then wait.
		if subNode.proposer.ResponseCount < len(subNode.peers) ||
			subNode.proposer.SuccessCount <= len(subNode.peers)/2 {

			subNodes = append(subNodes, subNode)
		}

		return subNodes
	case Accept:
		if server.proposer.Phase != Accept {
			return []base.Node{server}
		}

		response, ok := message.(*AcceptResponse)
		if !ok {
			return []base.Node{server}
		}

		i := server.address2Index(response.From())
		if i == -1 || server.proposer.Responses[i] {
			return []base.Node{server}
		}

		if response.SessionId != server.proposer.SessionId {
			return []base.Node{server}
		}

		subNode := server.copy()
		subNode.proposer.Responses[i] = true
		subNode.proposer.ResponseCount++

		if response.Ok {
			subNode.proposer.SuccessCount++
		} else {
			subNode.n_p = max(subNode.n_p, response.N_p)
		}

		subNodes := make([]base.Node, 0, 2)

		// Acknowledged by majority of peers, proceed to next phase
		if subNode.proposer.SuccessCount > len(subNode.peers)/2 {
			newPhaseNode := subNode.copy()
			newPhaseNode.sendDecide()
			subNodes = append(subNodes, newPhaseNode)
		}

		// waiting for response from every peer
		if subNode.proposer.ResponseCount < len(subNode.peers) ||
			subNode.proposer.SuccessCount <= len(subNode.peers)/2 {

			subNodes = append(subNodes, subNode)
		}

		return subNodes
	default:
		return []base.Node{server}
	}
}

func (server *Server) acceptorHandler(message base.Message, phase string) []base.Node {
	switch phase {
	case Propose:
		request, ok := message.(*ProposeRequest)
		if !ok {
			return []base.Node{server}
		}

		subNode := server.copy()
		var response *ProposeResponse
		if request.N > subNode.n_p {
			subNode.n_p = request.N

			response = &ProposeResponse{
				CoreMessage: base.MakeCoreMessage(server.Address(), request.From()),
				Ok:          true,
				N_p:         request.N,
				N_a:         subNode.n_a,
				V_a:         subNode.v_a,
				SessionId:   request.SessionId,
			}
		} else {
			response = &ProposeResponse{
				CoreMessage: base.MakeCoreMessage(server.Address(), request.From()),
				Ok:          false,
				N_p:         subNode.n_p,
				SessionId:   request.SessionId,
			}
		}
		subNode.SetSingleResponse(response)

		return []base.Node{subNode}
	case Accept:
		request, ok := message.(*AcceptRequest)
		if !ok {
			return []base.Node{server}
		}

		subNode := server.copy()
		var response *AcceptResponse
		if request.N >= subNode.n_p {
			subNode.n_p = request.N
			subNode.n_a = request.N
			subNode.v_a = request.V

			response = &AcceptResponse{
				CoreMessage: base.MakeCoreMessage(server.Address(), request.From()),
				Ok:          true,
				N_p:         request.N,
				SessionId:   request.SessionId,
			}
		} else {
			response = &AcceptResponse{
				CoreMessage: base.MakeCoreMessage(server.Address(), request.From()),
				Ok:          false,
				N_p:         subNode.n_p,
				SessionId:   request.SessionId,
			}
		}
		subNode.SetSingleResponse(response)

		return []base.Node{subNode}
	case Decide:
		request, ok := message.(*DecideRequest)
		if !ok {
			return []base.Node{server}
		}

		subNode := server.copy()
		subNode.agreedValue = request.V
		return []base.Node{subNode}
	default:
		return []base.Node{server}
	}
}

func (server *Server) sendPropose() {
	if base.IsNil(server.proposer.InitialValue) {
		return
	}

	server.proposer.SessionId++
	server.proposer.Phase = Propose
	server.proposer.N = server.n_p + 1
	server.proposer.N_a_max = 0
	server.proposer.V = server.proposer.InitialValue
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	server.proposer.SuccessCount = 0

	messages := make([]base.Message, 0, len(server.peers))
	for _, peer := range server.peers {
		message := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(server.Address(), peer),
			N:           server.proposer.N,
			SessionId:   server.proposer.SessionId,
		}
		messages = append(messages, message)
	}
	server.SetResponse(messages)
}

func (server *Server) sendAccept() {
	server.proposer.Phase = Accept
	for i := range server.proposer.Responses {
		server.proposer.Responses[i] = false
	}
	server.proposer.ResponseCount = 0
	server.proposer.SuccessCount = 0

	messages := make([]base.Message, 0, len(server.peers))
	for _, peer := range server.peers {
		message := &AcceptRequest{
			CoreMessage: base.MakeCoreMessage(server.Address(), peer),
			N:           server.proposer.N,
			V:           server.proposer.V,
			SessionId:   server.proposer.SessionId,
		}
		messages = append(messages, message)
	}
	server.SetResponse(messages)
}

func (server *Server) sendDecide() {
	server.proposer.Phase = Decide
	for i := range server.proposer.Responses {
		server.proposer.Responses[i] = false
	}
	server.proposer.ResponseCount = 0
	server.proposer.SuccessCount = 0

	messages := make([]base.Message, 0, len(server.peers))
	for _, peer := range server.peers {
		message := &DecideRequest{
			CoreMessage: base.MakeCoreMessage(server.Address(), peer),
			V:           server.proposer.V,
			SessionId:   server.proposer.SessionId,
		}
		messages = append(messages, message)
	}
	server.SetResponse(messages)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
