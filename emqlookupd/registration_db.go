package emqlookupd

import (
	"sync"
	"time"
)

type RegiostrationDB struct {
	mtx             sync.RWMutex
	registrationMap map[Registration]map[string]*Producer
}

type Registration struct {
	Category string
	Key      string
	SubKey   string
}

func (k Registration) IsMatch(category, key, subKey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && key != k.Key {
		return false
	}
	if subKey != "*" && subKey != k.SubKey {
		return false
	}
	return true
}

type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string
	HostName         string
	BroadcastAddress string
	TCPPort          int
	HTTPPort         int
}

type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

func NewRegiostrationDB() *RegiostrationDB {
	return &RegiostrationDB{
		registrationMap: make(map[Registration]map[string]*Producer, 0),
	}
}

func (r *RegiostrationDB) AddRegistration(k Registration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

func (r *RegiostrationDB) RemoveRegistration(k Registration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	delete(r.registrationMap, k)
}

func (r *RegiostrationDB) AddProducer(k Registration, p *Producer) bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, ok := r.registrationMap[k]; !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}

	producers := r.registrationMap[k]
	if _, found := producers[p.peerInfo.id]; !found {
		producers[p.peerInfo.id] = p
		return true
	}

	return false
}

func (r *RegiostrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}

	if _, found := producers[id]; found {
		delete(producers, id)
		return true, len(producers)
	}

	return false, len(producers)
}

func (r *RegiostrationDB) FindRegistrations(category, key, subKey string) []Registration {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if !r.needFilter(key, subKey) {
		k := Registration{
			Category: category,
			Key:      key,
			SubKey:   subKey,
		}

		if _, ok := r.registrationMap[k]; ok {
			return []Registration{k}
		}

		return []Registration{} // 没有搜到
	}

	// 需要过滤出所有的符合通配符
	results := make([]Registration, 0)
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subKey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

func (r *RegiostrationDB) needFilter(key, subKey string) bool {
	return key == "*" || subKey == "*"
}
