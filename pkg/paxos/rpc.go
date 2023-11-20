package paxos

import "net/rpc"

// ProposalNumber is a tuple of ProposalId and ProposerId
type ProposalNumber struct {
	ProposalId int
	ProposerId int
}

type ProposalValue string

// Lt returns true if p is less than o
func (p ProposalNumber) Lt(o ProposalNumber) bool {
	if p.ProposalId > o.ProposalId {
		return true
	}
	if p.ProposalId < o.ProposalId {
		return false
	}
	if p.ProposerId > o.ProposerId {
		return true
	}
	return false
}

// PrepareRequest is sent by Proposer to Acceptor in Prepare phase
type PrepareRequest struct {
	ProposalNumber ProposalNumber
}

// PrepareResponse is sent by Acceptor back to Proposer in Prepare phase
type PrepareResponse struct {
	Ok bool

	ProposalNumber ProposalNumber
	ProposalValue  ProposalValue
}

type AcceptRequest struct {
	ProposalNumber ProposalNumber
	ProposalValue  ProposalValue
}

type AcceptResponse struct {
	Ok bool
}

// AcceptorClient is a wrapper around net/rpc.Client
type AcceptorClient struct {
	c *rpc.Client
}

func (c *AcceptorClient) SendPrepareRequest(req PrepareRequest) (PrepareResponse, error) {
	var res PrepareResponse
	err := c.c.Call("Acceptor.Prepare", req, &res)
	if err != nil {
		return PrepareResponse{}, err
	}
	return res, nil
}

func (c *AcceptorClient) SendAcceptRequest(req AcceptRequest) (AcceptResponse, error) {
	var res AcceptResponse
	err := c.c.Call("Acceptor.Accept", req, &res)
	if err != nil {
		return AcceptResponse{}, err
	}
	return res, nil
}
