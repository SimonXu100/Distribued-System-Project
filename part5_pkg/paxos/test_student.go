package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	g1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	}

	g2 := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.N > s2.n_p
	}

	g3 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	}

	return []func(s *base.State) bool{g1, g2, g3}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	return []func(s *base.State) bool{}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	g1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.SuccessCount == 1
	}

	g2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.SuccessCount == 2
	}

	g3 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.SuccessCount == 1
	}

	g4 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.SuccessCount == 2
	}

	g5 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0
	}

	g6 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0 && s1.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{g1, g2, g3, g4, g5, g6}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	g1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.SuccessCount == 1
	}

	g2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.SuccessCount == 2
	}

	g3 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 0
	}

	g4 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 0 && s3.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{g1, g2, g3, g4}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	g1 := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.SuccessCount == 2
	}

	g2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0
	}

	g3 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.SuccessCount == 0 && s1.proposer.ResponseCount == 1
	}

	return []func(s *base.State) bool{g1, g2, g3}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Propose
		if valid {
			fmt.Println("... p1 entered Propose phase")
		}
		return valid
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.n_a == 0 && s2.n_a == 0 && s3.n_a == 0
		if valid {
			fmt.Println("... p1 entered Accept phase")
		}
		return valid
	}
	p3PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Propose && s1.n_a == 0 && s2.n_a == 0 && s3.n_a == 0
		if valid {
			fmt.Println("... p3 entered Propose phase")
		}
		return valid
	}
	p3AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.ResponseCount == 0 &&
			s1.n_p == 2 && s2.n_p == 2 && s3.n_p == 2
		if valid {
			fmt.Println("... p3 entered Accept phase")
		}
		return valid
	}

	p1AcceptResponsePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		valid := s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 1
		if valid {
			fmt.Println("... p1 entered Accept phase but get failure responses")
		}
		return valid
	}
	return []func(s *base.State) bool{
		p1PreparePhase,
		p1AcceptPhase,
		p3PreparePhase,
		p3AcceptPhase,
		p1AcceptResponsePhase,
	}
}

// Fill in the function to lead the program continue  P3's proposal  and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3AcceptOkPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Accept && s3.proposer.SuccessCount == 2
		if valid {
			fmt.Println("... p3 entered Accept OK phase")
		}
		return valid
	}
	p3DecidePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		valid := s3.proposer.Phase == Decide
		if valid {
			fmt.Println("... p3 entered Decide phase")
		}
		return valid
	}

	return []func(s *base.State) bool{
		p3AcceptOkPhase,
		p3DecidePhase,
	}
}
