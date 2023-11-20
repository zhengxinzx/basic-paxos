package paxos

import (
	"log"
	"math/rand"
	"sync"
)

// Proposer
type Proposer struct {
	// Unique ID of the current Proposer
	proposerID int

	// Proposal ID. The ID is initialized as 0, and is updated when
	// Each time the Proposal stage starts, and
	// each time when a higher Proposal number is received from acceptor.
	proposalID int

	// Proposal value.
	// The value might be updated by the value received from acceptor.
	proposalValue ProposalValue

	// RPC clients to connect to acceptor
	clients []*AcceptorClient
}

// Select clients for acceptor. The number of clients returned by this function is guaranteed to be a majority of acceptors.
func (p *Proposer) selectMajorityAcceptorClients() []*AcceptorClient {
	majority := p.majorityAcceptorCount()
	count := majority + rand.Intn(len(p.clients)-majority+1)

	ids := make(map[int]bool)
	for len(ids) < count {
		id := rand.Intn(len(p.clients))
		if ids[id] {
			continue
		}
		ids[id] = true
	}

	var clients []*AcceptorClient
	for id, _ := range ids {
		clients = append(clients, p.clients[id])
	}

	return clients
}

// majorityAcceptorCount returns the number of acceptors that is smallest majority of the total number of acceptors.
func (p *Proposer) majorityAcceptorCount() int {
	length := len(p.clients)
	if length == 0 {
		log.Panicf("Proposer %v: Configuration error: No acceptors available.", p.proposerID)
	}
	return length/2 + 1
}

// Prepare is the phase 1.a of the Paxos algorithm.
func (p *Proposer) Prepare(proposalNumber ProposalNumber, proposalValue ProposalValue) (bool, ProposalNumber, ProposalValue) {
	// Select majority acceptors
	clients := p.selectMajorityAcceptorClients()

	// Prepare channel and wait group
	ch := make(chan PrepareResponse)
	var wg sync.WaitGroup
	wg.Add(len(clients))

	// Send Prepare requests to majority acceptors in parallel
	for _, client := range clients {
		client := client
		go func() {
			defer wg.Done()
			req := PrepareRequest{ProposalNumber: proposalNumber}
			res, err := client.SendPrepareRequest(req)
			if err != nil {
				// Could be network error, considered as not promised
				log.Printf("Proposer %v: Error sending Prepare request to Acceptor %v: %v.\n", p.proposerID, client, err)
				ch <- PrepareResponse{Ok: false}
			}
			ch <- res
		}()
	}

	// Use wait group to close the channel
	go func() {
		wg.Wait()
		close(ch)
	}()

	// Wait for responses from the channel
	var responses []PrepareResponse
	for resp := range ch {
		responses = append(responses, resp)
	}

	// Check if received OK from a majority of acceptors?
	promisedCount := 0
	acceptedProposalNumber := proposalNumber
	acceptedProposalValue := proposalValue
	for _, res := range responses {
		// select the highest proposal ID

		if res.ProposalNumber.Lt(acceptedProposalNumber) {
			acceptedProposalNumber = res.ProposalNumber
			acceptedProposalValue = res.ProposalValue
		}

		if !res.Ok {
			promisedCount++
		}
	}

	return promisedCount >= p.majorityAcceptorCount(), acceptedProposalNumber, acceptedProposalValue
}

func (p *Proposer) Accept(proposedNumber ProposalNumber, proposedValue ProposalValue) bool {
	// Select majority acceptors
	clients := p.selectMajorityAcceptorClients()

	// Prepare channel and wait group
	ch := make(chan AcceptResponse)
	var wg sync.WaitGroup
	wg.Add(len(clients))

	// Send Accept requests to majority acceptors in parallel
	for _, client := range clients {
		client := client
		go func() {
			defer wg.Done()
			req := AcceptRequest{ProposalNumber: proposedNumber, ProposalValue: proposedValue}
			res, err := client.SendAcceptRequest(req)
			if err != nil {
				// Could be network error, considered as not promised
				log.Printf("Proposer %v: Error sending Accept request to Acceptor %v: %v.\n", p.proposerID, client, err)
				ch <- AcceptResponse{Ok: false}
			}
			ch <- res
		}()
	}

	// Use wait group to close the channel
	go func() {
		wg.Wait()
		close(ch)
	}()

	// Wait for responses from the channel
	var responses []AcceptResponse
	for resp := range ch {
		responses = append(responses, resp)
	}

	// Check if received OK from a majority of acceptors
	acceptedCount := 0
	for _, res := range responses {
		if res.Ok {
			acceptedCount++
		}
	}
	if acceptedCount >= p.majorityAcceptorCount() {
		return true
	}
	return false
}

// Propose starts the propose stage.
func (p *Proposer) Propose() {
	for {
		// Initial setup stage
		// Increment proposal id
		p.proposalID++
		log.Printf("Proposer %v: Starting Propose stage with proposal %v (%s).\n", p.proposerID, p.proposalID, p.proposalValue)

		// Create proposal number used in this round
		proposalNumber := ProposalNumber{
			ProposalId: p.proposalID,
			ProposerId: p.proposerID,
		}
		log.Printf("Proposer %v: Proposal number for this round is %v.\n", p.proposerID, proposalNumber)

		// Phase 1.a Prepare
		promised, acceptedProposalNumber, acceptedProposalValue := p.Prepare(proposalNumber, p.proposalValue)

		// If the prepare request is not accepted by a majority of acceptors, then restart the propose stage
		// with a higher proposal number and the value received from the acceptor with the highest proposal number.
		if !promised {
			p.proposalID = acceptedProposalNumber.ProposalId
			p.proposalValue = acceptedProposalValue
			continue
		}

		// Otherwise update the proposal value with the value received from the acceptor with the highest proposal number,
		// but still use the same proposal number.
		p.proposalValue = acceptedProposalValue

		// Phase 2.a Accept
		accepted := p.Accept(proposalNumber, p.proposalValue)
		if accepted {
			log.Printf("Proposer %v: Proposal %v (%s) accepted by majority acceptors.\n", p.proposerID, proposalNumber, p.proposalValue)
			break
		}
		log.Printf("Proposer %v: Proposal %v (%s) not accepted by majority acceptors. Restart Propose stage.\n", p.proposerID, proposalNumber, p.proposalValue)
	}
}

// Acceptor
type Acceptor struct {
	sync.Mutex

	maxRespondedProposalNumber ProposalNumber
	acceptedProposalNumber     ProposalNumber
	acceptedProposalValue      ProposalValue

	accepted bool
}

// Prepare is the phase 1.b of the Paxos algorithm.
func (a *Acceptor) Prepare(req PrepareRequest, res *PrepareResponse) error {
	a.Lock()
	defer a.Unlock()

	res = &PrepareResponse{}
	// If the proposal number is the greatest seen so far, then promise not to accept any more proposals
	if req.ProposalNumber.Lt(a.maxRespondedProposalNumber) {
		a.maxRespondedProposalNumber = req.ProposalNumber
		// If acceptor has accepted a proposal, then return the proposal number and value
		if a.accepted {
			res.Ok = true
			res.ProposalNumber = a.acceptedProposalNumber
			res.ProposalValue = a.acceptedProposalValue
		} else {
			// Else simply return OK
			res.Ok = true
		}
	} else {
		res.Ok = false
	}
	return nil
}

// Accept is the phase 2.b of the Paxos algorithm.
func (a *Acceptor) Accept(req AcceptRequest, res *AcceptResponse) error {
	a.Lock()
	defer a.Unlock()

	res = &AcceptResponse{}
	// If acceptor has already responded to a prepare request having a number greater than n.
	if a.maxRespondedProposalNumber.Lt(req.ProposalNumber) {
		// Do not accept the proposal
		res = &AcceptResponse{Ok: false}
	} else {
		// Otherwise accept the proposal
		if req.ProposalNumber.Lt(a.maxRespondedProposalNumber) {
			a.maxRespondedProposalNumber = req.ProposalNumber
		}
		a.acceptedProposalNumber = req.ProposalNumber
		a.acceptedProposalValue = req.ProposalValue
		res = &AcceptResponse{Ok: true}
	}
	return nil
}
