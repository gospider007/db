package db

import (
	"context"
	"sync"
	"time"
)

type ClientOption struct {
	TTL time.Duration
}

func NewClient(ctx context.Context, option ClientOption) *Client {
	if ctx == nil {
		ctx = context.TODO()
	}
	context, cancel := context.WithCancel(ctx)
	client := &Client{
		ttl: int64(option.TTL),
		ctx: context,
		cnl: cancel,
	}
	if client.ttl > 0 {
		go client.run()
	}
	return client
}

type rawData struct {
	data []byte
	time int64
}
type Client struct {
	ttl  int64
	data sync.Map
	ctx  context.Context
	cnl  context.CancelFunc
}

func (obj *Client) run() {
	for {
		select {
		case <-obj.ctx.Done():
			return
		default:
			obj.data.Range(func(key, value any) bool {
				val := value.(rawData)
				if time.Now().Unix()-val.time > obj.ttl {
					obj.data.Delete(key)
				}
				return true
			})
			select {
			case <-obj.ctx.Done():
				return
			case <-time.After(time.Second * 30):
			}
		}
	}
}

func (obj *Client) set(key string, data []byte) {
	obj.data.Store(key, rawData{
		data: data,
		time: time.Now().Unix(),
	})
}

func (obj *Client) get(key string) ([]byte, bool) {
	val, ok := obj.data.Load(key)
	if !ok {
		return nil, false
	}
	return val.(rawData).data, true
}
func (obj *Client) Set(key string, data []byte) {
	obj.set(key, data)
}

func (obj *Client) Get(key string) ([]byte, bool) {
	return obj.get(key)
}
func (obj *Client) Close() {
	obj.cnl()
	obj.data.Clear()
}
