package db

import (
	"bytes"
	"context"
	"embed"
	"encoding/gob"
	"os"
	"sync"
	"time"

	"github.com/gospider007/tools"
)

type FS struct {
	FS  *embed.FS
	Dir string
}
type ClientOption struct {
	TTL time.Duration
	Dir string
	FS  FS
}

func NewClient(ctx context.Context, option ClientOption) (*Client, error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	context, cancel := context.WithCancel(ctx)
	client := &Client{
		ttl: int64(option.TTL),
		ctx: context,
		cnl: cancel,
		dir: option.Dir,
		fs:  option.FS,
	}

	if client.dir != "" && !tools.PathExist(client.dir) {
		err := os.MkdirAll(client.dir, 0777)
		if err != nil {
			return nil, err
		}
	}
	if client.ttl > 0 && client.dir != "" {
		go client.run()
	}
	return client, nil
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
	dir  string
	fs   FS
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
func (obj *Client) Set(key string, data any) error {
	key = tools.Hex(tools.Md5(key))
	b := bytes.NewBuffer(nil)
	err := gob.NewEncoder(b).Encode(data)
	if err != nil {
		return err
	}
	if obj.dir != "" {
		return os.WriteFile(tools.PathJoin(obj.dir, key), b.Bytes(), 0777)
	} else {
		obj.set(key, b.Bytes())
	}
	return nil
}

func (obj *Client) Get(key string, data any) (bool, error) {
	key = tools.Hex(tools.Md5(key))
	if obj.dir != "" && !tools.PathExist(tools.PathJoin(obj.dir, key)) {
		b, err := os.ReadFile(tools.PathJoin(obj.dir, key))
		if err == nil {
			return true, gob.NewDecoder(bytes.NewBuffer(b)).Decode(data)
		}
	}
	if obj.fs.FS != nil && obj.fs.Dir != "" {
		b, err := obj.fs.FS.ReadFile(obj.fs.Dir + "/" + key)
		if err == nil {
			return true, gob.NewDecoder(bytes.NewBuffer(b)).Decode(data)
		}
	}
	b, ok := obj.get(key)
	if ok {
		return true, gob.NewDecoder(bytes.NewBuffer(b)).Decode(data)
	}
	return false, nil
}
func (obj *Client) Close() {
	obj.cnl()
	obj.data.Clear()
}
