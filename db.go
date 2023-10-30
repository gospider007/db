package db

import (
	"bytes"
	"encoding/gob"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gospider007/tools"
)

type ClientOption struct {
	Dir string
	Ttl time.Duration
}

func NewClient(option ClientOption) (*Client, error) {
	opt := badger.DefaultOptions(option.Dir)
	if option.Dir == "" {
		opt = opt.WithInMemory(true)
	}
	opt.WithLogger(nil)
	db, err := badger.Open(opt)
	return &Client{db: db, ttl: option.Ttl}, err
}

type Client struct {
	db  *badger.DB
	ttl time.Duration
}
type SetOption struct {
	Ttl time.Duration
}

func (obj *Client) Set(key []byte, data any, options ...SetOption) error {
	var option SetOption
	if len(options) > 0 {
		option = options[0]
	}
	var val []byte
	switch con := data.(type) {
	case []byte:
		val = con
	case string:
		val = tools.StringToBytes(con)
	default:
		b := bytes.NewBuffer(nil)
		if err := gob.NewEncoder(b).Encode(con); err != nil {
			return err
		}
		val = b.Bytes()
	}
	if option.Ttl == 0 {
		option.Ttl = obj.ttl
	}
	return obj.db.Update(func(txn *badger.Txn) error {
		if option.Ttl > 0 {
			return txn.SetEntry(badger.NewEntry(key, val).WithTTL(option.Ttl))
		} else {
			return txn.SetEntry(badger.NewEntry(key, val))
		}
	})
}

func (obj *Client) Get(key []byte, t ...any) (val []byte, err error) {
	err = obj.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err == nil && len(t) > 0 {
		err = gob.NewDecoder(bytes.NewBuffer(val)).Decode(t[0])
	}
	return
}
func (obj *Client) Close() error {
	return obj.db.Close()
}
