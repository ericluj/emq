package emqlookupd

import (
	"sync"
)

type RegiostrationDB struct {
	mtx             sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

type Registration struct {
	Category string
	Key      string
	SubKey   string
}
type Registrations []Registration

type Producer struct {
	peerInfo *PeerInfo
	// tombstoned   bool
	// tombstonedAt time.Time
}
type Producers []*Producer
type ProducerMap map[string]*Producer

type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string
	HostName         string
	BroadcastAddress string
	TCPPort          int
	HTTPPort         int
}

func NewRegiostrationDB() *RegiostrationDB {
	return &RegiostrationDB{
		registrationMap: make(map[Registration]ProducerMap, 0),
	}
}

// 含有*模糊匹配需要过滤
func (db *RegiostrationDB) NeedFilter(key, subKey string) bool {
	return key == "*" || subKey == "*"
}

func (db *RegiostrationDB) AddRegistration(k Registration) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	_, ok := db.registrationMap[k]
	if !ok {
		db.registrationMap[k] = make(ProducerMap)
	}
}

func (db *RegiostrationDB) RemoveRegistration(k Registration) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	delete(db.registrationMap, k)
}

func (db *RegiostrationDB) AddProducer(k Registration, p *Producer) bool {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if _, ok := db.registrationMap[k]; !ok {
		db.registrationMap[k] = make(ProducerMap)
	}

	producers := db.registrationMap[k]
	if _, found := producers[p.peerInfo.id]; !found {
		producers[p.peerInfo.id] = p
		return true
	}

	return false
}

func (db *RegiostrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	producers, ok := db.registrationMap[k]
	if !ok {
		return false, 0
	}

	if _, found := producers[id]; found {
		delete(producers, id)
		return true, len(producers)
	}

	return false, len(producers)
}

// 获取db.map的key
func (db *RegiostrationDB) FindRegistrations(category, key, subKey string) Registrations {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	// 不含*，精确匹配
	if !db.NeedFilter(key, subKey) {
		k := Registration{
			Category: category,
			Key:      key,
			SubKey:   subKey,
		}

		if _, ok := db.registrationMap[k]; ok {
			return Registrations{k}
		}

		return Registrations{} // 没有搜到
	}

	// 含有*，过滤，模糊匹配
	results := make(Registrations, 0)
	for k := range db.registrationMap {
		if !k.IsMatch(category, key, subKey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

// 获取db.map的Producer
func (db *RegiostrationDB) FindProducers(category, key, subKey string) Producers {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	// 不含*，精确匹配
	if !db.NeedFilter(key, subKey) {
		k := Registration{
			Category: category,
			Key:      key,
			SubKey:   subKey,
		}

		if pm, ok := db.registrationMap[k]; ok {
			return pm.ToSlice()
		}

		return Producers{} // 没有搜到
	}

	// 含有*，过滤，模糊匹配
	m := make(map[string]struct{}, 0) // 用来判读重复
	ps := make(Producers, 0)
	for k, producerMap := range db.registrationMap {
		if !k.IsMatch(category, key, subKey) {
			continue
		}

		for _, producer := range producerMap {
			_, found := m[producer.peerInfo.id]
			if !found {
				ps = append(ps, producer)
				m[producer.peerInfo.id] = struct{}{}
			}
		}
	}
	return ps
}

// 查看含有peerInfo.id的Registration
func (db *RegiostrationDB) LookupRegistrations(id string) Registrations {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	results := make(Registrations, 0)
	for k, producerMap := range db.registrationMap {
		if _, found := producerMap[id]; found {
			results = append(results, k)
		}
	}
	return results
}

func (r Registration) IsMatch(category, key, subKey string) bool {
	if category != r.Category { // category不同不匹配
		return false
	}
	if key != "*" && key != r.Key { // 含有*，全匹配
		return false
	}
	if subKey != "*" && subKey != r.SubKey { // 含有*，全匹配
		return false
	}
	return true
}

func (rs Registrations) Filter(category, key, subKey string) Registrations {
	data := make(Registrations, 0)
	for _, v := range rs {
		if v.IsMatch(category, key, subKey) {
			data = append(data, v)
		}
	}
	return data
}

func (rs Registrations) Keys() []string {
	keys := make([]string, len(rs))
	for i, v := range rs {
		keys[i] = v.Key
	}
	return keys
}

func (rs Registrations) SubKeys() []string {
	subkeys := make([]string, len(rs))
	for i, v := range rs {
		subkeys[i] = v.SubKey
	}
	return subkeys
}

func (pm ProducerMap) ToSlice() Producers {
	var producers Producers
	for _, p := range pm {
		producers = append(producers, p)
	}
	return producers
}

func (ps Producers) PeerInfo() []*PeerInfo {
	results := make([]*PeerInfo, 0)
	for _, p := range ps {
		results = append(results, p.peerInfo)
	}
	return results
}
