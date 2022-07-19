package emqlookupd

import "sync"

type RegiostrationDB struct {
	mtx sync.RWMutex
}

type Registration struct {
}

func NewRegiostrationDB() *RegiostrationDB {
	return nil
}
