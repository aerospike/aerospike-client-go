package aerospike

import "sync"

var binMapPool = sync.Pool{
	New: func() interface{} {
		return make(BinMap, 16)
	},
}

var resultPool = sync.Pool{
	New: func() interface{} {
		return make(map[interface{}]interface{}, 8)
	},
}

func (rc *Record) FreeBins() {
	if rc.Bins == nil {
		return
	}

	for k := range rc.Bins {
		m, ok := rc.Bins[k].(map[interface{}]interface{})
		if ok {
			for j := range m {
				delete(m, j)
			}
			resultPool.Put(m)
		}

		delete(rc.Bins, k)
	}

	binMapPool.Put(rc.Bins)
	rc.Bins = nil
}

func (clnt *Client) GetWithPool(policy *BasePolicy, key *Key, binNames ...string) (*Record, error) {
	policy = clnt.getUsablePolicy(policy)

	command, err := newReadCommand(clnt.cluster, policy, key, binNames, nil)
	if err != nil {
		return nil, err
	}
	command.withPool = true

	if err := command.Execute(); err != nil {
		return nil, err
	}
	return command.GetRecord(), nil
}
