package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

type DataSet struct{}

func NewDataSet(namespace, set string) (*DataSet, error) {
	return nil, nil
}

func MustNewDataSet(namespace, set string) *DataSet {
	return nil
}

// KeyValue constrains the id types DataSet keys can be built from.
type KeyValue interface {
	string | int | int64 | []byte
}

// Key and Keys are free functions, not methods on *DataSet: a method cannot
// declare its own type parameter, so this stays a helper in the D-28 sense.
func Key[K KeyValue](d *DataSet, id K) *as.Key {
	return nil
}

func Keys[K KeyValue](d *DataSet, ids []K) []*as.Key {
	return nil
}

func (d *DataSet) ID(id any) (*as.Key, error) {
	return nil, nil
}

func (d *DataSet) IDFromDigest(digest []byte) (*as.Key, error) {
	return nil, nil
}
