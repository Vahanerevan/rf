package rf

import (
	`sync`
	`time`
)

type node struct {
	seq       int64
	body      []byte
	timestamp int64
	next      *node
}

type db struct {
	root *node
	tail *node
	len  int64
	seq  int64
}

type store struct {
	db *db
	sync.Mutex
}

func newStore() Store {
	return &store{
		db: &db{
			root: nil,
			tail: nil,
			len:  0,
			seq:  0,
		},
	}
}

type Store interface {
	insert(body []byte) error
	fetchAfter(seq int64) ([]node, error)
	fetchAfterTimestamp(microTime int64) ([]node, error)
	
	deleteBeforeTimestamp(ts int64) error
	drop()
}

func (store *store) drop() {
	store.Lock()
	defer store.Unlock()
	store.db.root, store.db.tail = nil, nil
	store.db.len, store.db.seq = 0, 0
}

func (store *store) deleteBeforeTimestamp(ts int64) error {
	store.Lock()
	defer store.Unlock()
	for e := store.db.root; nil != e; e = e.next {
		if e.timestamp < ts {
			store.db.len--
			store.db.root = e.next
		}
	}
	return nil
}
func (store *store) fetchAfter(seq int64) ([]node, error) {
	store.Lock()
	defer store.Unlock()
	
	var result []node
	
	for item := store.db.root; nil != item; item = item.next {
		if item.seq > seq {
			result = append(result, *item)
		}
	}
	return result, nil
}
func (store *store) fetchAfterTimestamp(microTime int64) ([]node, error) {
	store.Lock()
	defer store.Unlock()
	
	var result []node
	for item := store.db.root; nil != item; item = item.next {
		if item.timestamp > microTime {
			result = append(result, *item)
		}
	}
	return result, nil
}
func (store *store) insert(body []byte) error {
	store.Lock()
	defer store.Unlock()
	
	store.db.seq++
	store.db.len++
	
	item := &node{
		seq:       store.db.seq,
		body:      body,
		timestamp: time.Now().UnixMicro(),
	}
	if nil == store.db.root {
		store.db.root = item
		store.db.tail = item
	} else {
		store.db.tail.next = item
		store.db.tail = item
	}
	
	return nil
}
