package rf

import (
	`fmt`
	`sync`
	`time`
)

// TODO: make longer
const zombieTime = time.Minute / 6

func Topic(topic string) Option {
	return func(ex *exchange) {
		ex.options.topic = topic
	}
}

func MaxLife(maxLife time.Duration) Option {
	return func(ex *exchange) {
		ex.options.MaxLife = maxLife
	}
}

type Option func(*exchange)

type options struct {
	topic   string
	MaxLife time.Duration
}

func (o *options) init() {
	if o.MaxLife <= 0 {
		o.MaxLife = time.Minute * 5
	}
}

type Exchange interface {
	Publish(body []byte) error
	Subscriber() Subscriber
	HasSubscribers() bool
	Close() error
	Topic() string
}

type exchange struct {
	options        options
	lifeTimeTicker *time.Ticker
	zombieTicker   *time.Ticker
	subscribers    map[Subscriber]struct{}
	sync           sync.Mutex
	store          Store
	isRunning      bool
}

func (ex *exchange) Topic() string {
	return ex.options.topic
}

func (ex *exchange) Subscriber() Subscriber {
	ex.sync.Lock()
	defer ex.sync.Unlock()
	
	sub := newSubscriber(ex)
	ex.subscribers[sub] = struct{}{}
	return sub
}

func NewExchange(opts ...Option) Exchange {
	
	ex := &exchange{
		store:       newStore(),
		options:     options{},
		subscribers: make(map[Subscriber]struct{}),
		isRunning:   true,
	}
	for _, opt := range opts {
		opt(ex)
	}
	
	ex.options.init()
	ex.lifeTimeTicker = time.NewTicker(ex.options.MaxLife)
	ex.zombieTicker = time.NewTicker(zombieTime)
	ex.run()
	return ex
}

func (ex *exchange) Close() error {
	ex.subscribers = nil
	ex.isRunning = false
	
	ex.lifeTimeTicker.Stop()
	ex.zombieTicker.Stop()
	ex.store.drop()
	
	return nil
}

func (ex *exchange) Publish(body []byte) error {
	ex.sync.Lock()
	defer ex.sync.Unlock()
	return ex.store.insert(body)
}

func (ex *exchange) HasSubscribers() bool {
	ex.sync.Lock()
	defer ex.sync.Unlock()
	return len(ex.subscribers) > 0
}

func (ex *exchange) run() {
	go func() {
		for ex.isRunning {
			select {
			case <-ex.zombieTicker.C:
				if false == ex.HasSubscribers() {
					fmt.Println(`Exchange: No subscribers, stopping...`)
					err := ex.Close()
					if nil != err {
						fmt.Println(`Exchange: Error closing exchange:`, err)
					}
				}
			case <-ex.lifeTimeTicker.C:
				fmt.Println(`Exchange: Cleaning up...`)
				_ = ex.store.deleteBeforeTimestamp(time.Now().UnixMicro())
			}
			
		}
	}()
}

func (ex *exchange) unsubscribe(sub Subscriber) error {
	ex.sync.Lock()
	defer ex.sync.Unlock()
	if _, ok := ex.subscribers[sub]; ok {
		delete(ex.subscribers, sub)
		return nil
	}
	return nil
}

func (ex *exchange) hasSubscriber(sub Subscriber) bool {
	ex.sync.Lock()
	defer ex.sync.Unlock()
	_, ok := ex.subscribers[sub]
	return ok
}
