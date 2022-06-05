package main

import (
	`errors`
	`sync/atomic`
	`time`
)

type Subscriber interface {
	Fetch() ([]Record, error)
	FetchTimestamp(ts time.Time) ([]Record, error)
	Pull() ([]Record, error)
	Stop() error
	Commit() (int64, error)
}

type Record interface {
	Seq() int64
	Body() []byte
	Timestamp() time.Time
}

type record struct {
	seq       int64
	body      []byte
	timestamp int64
}

func (r record) Seq() int64 {
	return r.seq
}

func (r record) Body() []byte {
	return r.body
}

func (r record) Timestamp() time.Time {
	return time.UnixMicro(r.timestamp)
}

type subscriber struct {
	seq      int64
	tmpSeq   int64
	exchange *exchange
}

func (sub *subscriber) Fetch() ([]Record, error) {
	
	if !sub.exchange.hasSubscriber(sub) {
		return nil, errors.New(`unsubscribed fetch attempt`)
	}
	
	items, err := sub.exchange.store.fetchAfter(sub.seq)
	if nil != err {
		return nil, err
	}
	return sub.iterateNodes(items)
}

func (sub *subscriber) FetchTimestamp(ts time.Time) ([]Record, error) {
	if !sub.exchange.hasSubscriber(sub) {
		return nil, errors.New(`unsubscribed fetch attempt`)
	}
	
	items, err := sub.exchange.store.fetchAfterTimestamp(ts.UnixNano())
	if nil != err {
		return nil, err
	}
	return sub.iterateNodes(items)
}

func (sub *subscriber) iterateNodes(items []node) ([]Record, error) {
	var result []Record
	for _, item := range items {
		result = append(result, record{
			seq:       item.seq,
			body:      item.body,
			timestamp: item.timestamp,
		})
	}
	if len(result) > 0 {
		atomic.StoreInt64(&sub.tmpSeq, result[len(result)-1].Seq())
	}
	return result, nil
}

func (sub *subscriber) Stop() error {
	return sub.exchange.unsubscribe(sub)
}

func newSubscriber(exchange *exchange) *subscriber {
	return &subscriber{exchange: exchange}
}

func (sub *subscriber) Pull() ([]Record, error) {
	return nil, nil
}

func (sub *subscriber) Commit() (int64, error) {
	atomic.StoreInt64(&sub.seq, sub.tmpSeq)
	
	return sub.seq, nil
}
