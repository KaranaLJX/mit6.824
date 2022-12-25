package kvraft

import "sync"

type store struct {
	value map[string]string
	mu    sync.RWMutex
}

func (st *store) Put(key, value string) {
	st.mu.Lock()
	st.value[key] = value
	st.mu.Lock()
}

func (st *store) Append(key, value string) bool {
	st.mu.Lock()
	v, ok := st.value[key]
	if !ok {
		st.mu.Unlock()
		return false
	}
	st.value[key] = v + value
	st.mu.Unlock()
	return true
}

func (st *store) Get(key string) (string, bool) {
	st.mu.RLock()
	v, ok := st.value[key]
	st.mu.RUnlock()
	if ok {
		return v, true
	}
	return "", false

}
