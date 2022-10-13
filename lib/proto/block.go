// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package proto

import (
	"errors"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"sync"
	"sync/atomic"
)

type Block struct {
	names               []string
	Packet              byte
	Columns             []column.Interface
	ConcurrentWriteFlag atomic.Bool
	writeThreadSize     int
	ColBatchSize        int
	writeChans          []chan *Payload
	wg                  sync.WaitGroup
	lastErr             atomic.Value
}
type Payload struct {
	idxs []int
	vals []interface{}
}

func (b *Block) OpenConcurrentWrite() error {
	cbs := b.ColBatchSize
	if cbs <= 50 {
		return fmt.Errorf("please set ColBatchSize and ColBatchSize must grather 50")
	}
	if b.ConcurrentWriteFlag.Load() {
		return fmt.Errorf("concurrentWrite has opened")
	}
	wts := len(b.Columns) / cbs
	if len(b.Columns)%cbs != 0 {
		wts++
	}
	if wts <= 0 {
		wts = 1
	}
	b.writeThreadSize = wts
	for i := 0; i < wts; i++ {
		b.writeChans = append(b.writeChans, make(chan *Payload, 500))
	}
	for _, writeChan := range b.writeChans {
		ch := writeChan
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			for p := range ch {
				if !b.ConcurrentWriteFlag.Load() {
					b.lastErr.Store(&BlockError{
						Op:         "AppendRow",
						Err:        fmt.Errorf("concurrentWrite is closed"),
						ColumnName: "",
					})
					return
				}
				if len(p.idxs) != len(p.vals) {
					b.lastErr.Store(&BlockError{
						Op:         "AppendRow",
						Err:        fmt.Errorf("idxs size is not equal vals size"),
						ColumnName: "",
					})
					return
				}
				for i, idx := range p.idxs {
					if err := b.Columns[idx].AppendRow(p.vals[i]); err != nil {
						b.lastErr.Store(&BlockError{
							Op:         "AppendRow",
							Err:        err,
							ColumnName: b.Columns[idx].Name(),
						})
						return
					}
					//fmt.Printf("%d -> %d: %v\n", i, idx, p.vals[i])
				}
			}
		}()
	}
	b.ConcurrentWriteFlag.Store(true)
	return nil
}

func (b *Block) CloseConcurrentWrite() error {
	if !b.ConcurrentWriteFlag.Load() {
		return nil
	}
	for _, writeChan := range b.writeChans {
		close(writeChan)
	}
	b.wg.Wait()
	b.ConcurrentWriteFlag.Store(false)
	if b.lastErr.Load() != nil {
		err := b.lastErr.Load()
		if e, ok := err.(*BlockError); ok {
			return e
		}
		return fmt.Errorf("unexpected error")
	}
	return nil
}

func (b *Block) Rows() int {
	if len(b.Columns) == 0 {
		return 0
	}
	return b.Columns[0].Rows()
}

func (b *Block) AddColumn(name string, ct column.Type) error {
	column, err := ct.Column(name)
	if err != nil {
		return err
	}
	b.names, b.Columns = append(b.names, name), append(b.Columns, column)
	return nil
}

func (b *Block) Append(v ...interface{}) (err error) {
	columns := b.Columns
	if len(columns) != len(v) {
		return &BlockError{
			Op:  "Append",
			Err: fmt.Errorf("clickhouse: expected %d arguments, got %d", len(columns), len(v)),
		}
	}

	if b.ConcurrentWriteFlag.Load() {
		if len(v) == 0 {
			return nil
		}

		//st := time.Now()
		mwg := sync.WaitGroup{}
		beginIdx := 0
		for i := 0; i < b.writeThreadSize; i++ {
			if beginIdx >= len(columns) {
				break
			}
			endIdx := beginIdx + b.ColBatchSize
			if endIdx > len(columns) {
				endIdx = len(columns)
			}
			if beginIdx == endIdx {
				break
			}
			var tmpIdx []int
			var tmpVal []interface{}
			for j := beginIdx; j < endIdx; j++ {
				tmpIdx = append(tmpIdx, j)
				tmpVal = append(tmpVal, v[j])
			}
			mwg.Add(1)
			go func() {
				defer mwg.Done()
				b.writeChans[i] <- &Payload{idxs: tmpIdx, vals: tmpVal}
			}()
			beginIdx += b.ColBatchSize
		}
		mwg.Wait()
		//fmt.Printf("write writeChans duration %v ms with %v threads\n", time.Since(st).Milliseconds(), b.writeThreadSize)
	} else {
		for i, val := range v {
			if err := b.Columns[i].AppendRow(val); err != nil {
				return &BlockError{
					Op:         "AppendRow",
					Err:        err,
					ColumnName: columns[i].Name(),
				}
			}
		}
	}
	return nil
}

func (b *Block) ColumnsNames() []string {
	return b.names
}

func (b *Block) Encode(buffer *proto.Buffer, revision uint64) error {
	if revision > 0 {
		encodeBlockInfo(buffer)
	}
	var rows int
	if len(b.Columns) != 0 {
		rows = b.Columns[0].Rows()
		for _, c := range b.Columns[1:] {
			cRows := c.Rows()
			if rows != cRows {
				return &BlockError{
					Op:  "Encode",
					Err: fmt.Errorf("mismatched len of columns - expected %d, recieved %d for col %s", rows, cRows, c.Name()),
				}
			}
		}
	}
	buffer.PutUVarInt(uint64(len(b.Columns)))
	buffer.PutUVarInt(uint64(rows))
	for _, c := range b.Columns {
		buffer.PutString(c.Name())
		buffer.PutString(string(c.Type()))
		if serialize, ok := c.(column.CustomSerialization); ok {
			if err := serialize.WriteStatePrefix(buffer); err != nil {
				return &BlockError{
					Op:         "Encode",
					Err:        err,
					ColumnName: c.Name(),
				}
			}
		}
		c.Encode(buffer)
	}
	return nil
}

func (b *Block) Decode(reader *proto.Reader, revision uint64) (err error) {
	if revision > 0 {
		if err := decodeBlockInfo(reader); err != nil {
			return err
		}
	}
	var (
		numRows uint64
		numCols uint64
	)
	if numCols, err = reader.UVarInt(); err != nil {
		return err
	}
	if numRows, err = reader.UVarInt(); err != nil {
		return err
	}
	if numRows > 1_000_000_000 {
		return &BlockError{
			Op:  "Decode",
			Err: errors.New("more then 1 billion rows in block"),
		}
	}
	b.Columns = make([]column.Interface, 0, numCols)
	for i := 0; i < int(numCols); i++ {
		var (
			columnName string
			columnType string
		)
		if columnName, err = reader.Str(); err != nil {
			return err
		}
		if columnType, err = reader.Str(); err != nil {
			return err
		}
		c, err := column.Type(columnType).Column(columnName)
		if err != nil {
			return err
		}
		if numRows != 0 {
			if serialize, ok := c.(column.CustomSerialization); ok {
				if err := serialize.ReadStatePrefix(reader); err != nil {
					return &BlockError{
						Op:         "Decode",
						Err:        err,
						ColumnName: columnName,
					}
				}
			}
			if err := c.Decode(reader, int(numRows)); err != nil {
				return &BlockError{
					Op:         "Decode",
					Err:        err,
					ColumnName: columnName,
				}
			}
		}
		b.names, b.Columns = append(b.names, columnName), append(b.Columns, c)
	}
	return nil
}

func (b *Block) Reset() {
	for i := range b.Columns {
		b.Columns[i].Reset()
	}
}

func encodeBlockInfo(buffer *proto.Buffer) {
	buffer.PutUVarInt(1)
	buffer.PutBool(false)
	buffer.PutUVarInt(2)
	buffer.PutInt32(-1)
	buffer.PutUVarInt(0)
}

func decodeBlockInfo(reader *proto.Reader) error {
	{
		if _, err := reader.UVarInt(); err != nil {
			return err
		}
		if _, err := reader.Bool(); err != nil {
			return err
		}
		if _, err := reader.UVarInt(); err != nil {
			return err
		}
		if _, err := reader.Int32(); err != nil {
			return err
		}
	}
	if _, err := reader.UVarInt(); err != nil {
		return err
	}
	return nil
}

type BlockError struct {
	Op         string
	Err        error
	ColumnName string
}

func (e *BlockError) Error() string {
	switch err := e.Err.(type) {
	case *column.Error:
		return fmt.Sprintf("clickhouse [%s]: (%s %s) %s", e.Op, e.ColumnName, err.ColumnType, err.Err)
	case *column.DateOverflowError:
		return fmt.Sprintf("clickhouse: dateTime overflow. %s must be between %s and %s", e.ColumnName, err.Min.Format(err.Format), err.Max.Format(err.Format))
	}
	return fmt.Sprintf("clickhouse [%s]: %s %s", e.Op, e.ColumnName, e.Err)
}
