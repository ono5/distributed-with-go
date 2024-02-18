package server

import (
	"fmt"
	"sync"
)

var ErrOffSetNotFound = fmt.Errorf("offset not found")

// ログを書き込むためのレコード
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// コミットログ
type Log struct {
	mu       sync.Mutex
	recoreds []Record
}

func NewLog() *Log {
	return &Log{}
}

// レコードにログを追加
func (c *Log) Append(recored Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	recored.Offset = uint64(len(c.recoreds))
	c.recoreds = append(c.recoreds, recored)
	return recored.Offset, nil
}

// offsetに紐づくレコードを読み込む
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.recoreds)) {
		return Record{}, ErrOffSetNotFound
	}
	return c.recoreds[offset], nil
}
