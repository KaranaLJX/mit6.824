package kvraft

import "sync"

type store struct {
	value map[string]string
	mu    sync.RWMutex
}

func (st *store) Put(key, value string) {
	st.mu.Lock()
	st.value[key] = value
	DPrintf("sttttt put %v v %v", key, value)
	st.mu.Unlock()
}

func (st *store) Append(key, value string) bool {
	st.mu.Lock()

	st.value[key] = st.value[key] + value
	st.mu.Unlock()
	return true
}

func (st *store) Get(key string) (string, bool) {
	st.mu.RLock()
	v, ok := st.value[key]
	st.mu.RUnlock()
	if ok {
		DPrintf("sttttt get %v v %v", key, v)

		return v, true
	}
	DPrintf("sttttt get %v ERR", key)

	return "", false

}
