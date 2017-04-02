package paxos

func (p *paxos) setID(id uint64) {
	p.id = id
	p.persistentState.NodeId = id
	p.mustSync = true
}

func (p *paxos) setNodeCount(nodeCount uint64) {
	p.nodeCount = nodeCount
	p.persistentState.NodeCount = nodeCount
	p.mustSync = true
}

func (p *paxos) setLastInstalled(lastInstalled uint64) {
	p.lastInstalled = lastInstalled
	p.persistentState.LastInstalled = lastInstalled
	p.mustSync = true
}
