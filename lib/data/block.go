package data

import (
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	wb "github.com/ClickHouse/clickhouse-go/lib/writebuffer"
)

type offset [][]int

const DefaultBatchColSize = 200

type Block struct {
	Values     [][]interface{}
	Columns    []column.Column
	NumRows    uint64
	NumColumns uint64
	offsets    []offset
	buffers    []*buffer
	info       blockInfo

	Wg           sync.WaitGroup
	ThreadSize   int
	AppendChan   []chan *Payload
	BatchColSize int
	ErrChan      chan error
	Err          error
	WgErr        sync.WaitGroup
	OpenFlag     atomic.Value // bool
}

type Payload struct {
	//Idx int
	Idx  []int
	Cols []column.Column
	Vals []driver.Value
}

func (block *Block) Copy() *Block {
	return &Block{
		Columns:    block.Columns,
		NumColumns: block.NumColumns,
		info:       block.info,
	}
}

func (block *Block) ColumnNames() []string {
	names := make([]string, 0, len(block.Columns))
	for _, column := range block.Columns {
		names = append(names, column.Name())
	}
	return names
}

func (block *Block) Read(serverInfo *ServerInfo, decoder *binary.Decoder) (err error) {
	if serverInfo.Revision > 0 {
		if err = block.info.read(decoder); err != nil {
			return err
		}
	}

	if block.NumColumns, err = decoder.Uvarint(); err != nil {
		return err
	}
	if block.NumRows, err = decoder.Uvarint(); err != nil {
		return err
	}
	block.Values = make([][]interface{}, block.NumColumns)
	if block.NumRows > 10 {
		for i := 0; i < int(block.NumColumns); i++ {
			block.Values[i] = make([]interface{}, 0, block.NumRows)
		}
	}
	for i := 0; i < int(block.NumColumns); i++ {
		var (
			value      interface{}
			columnName string
			columnType string
		)
		if columnName, err = decoder.String(); err != nil {
			return err
		}
		if columnType, err = decoder.String(); err != nil {
			return err
		}
		c, err := column.Factory(columnName, columnType, serverInfo.Timezone)
		if err != nil {
			return err
		}
		block.Columns = append(block.Columns, c)
		switch column := c.(type) {
		case *column.Array:
			if block.Values[i], err = column.ReadArray(decoder, int(block.NumRows)); err != nil {
				return err
			}
		case *column.Nullable:
			if block.Values[i], err = column.ReadNull(decoder, int(block.NumRows)); err != nil {
				return err
			}
		case *column.Tuple:
			if block.Values[i], err = column.ReadTuple(decoder, int(block.NumRows)); err != nil {
				return err
			}
		default:
			for row := 0; row < int(block.NumRows); row++ {
				if value, err = column.Read(decoder, false); err != nil {
					return err
				}
				block.Values[i] = append(block.Values[i], value)
			}
		}
	}
	return nil
}

func (block *Block) writeArray(column column.Column, value Value, num, level int) error {
	if level > column.Depth() {
		return column.Write(block.buffers[num].Column, value.Interface())
	}
	switch {
	case value.Kind() == reflect.Slice:
		if len(block.offsets[num]) < level {
			block.offsets[num] = append(block.offsets[num], []int{value.Len()})
		} else {
			block.offsets[num][level-1] = append(
				block.offsets[num][level-1],
				block.offsets[num][level-1][len(block.offsets[num][level-1])-1]+value.Len(),
			)
		}
		for i := 0; i < value.Len(); i++ {
			if err := block.writeArray(column, value.Index(i), num, level+1); err != nil {
				return err
			}
		}
	default:
		if err := column.Write(block.buffers[num].Column, value.Interface()); err != nil {
			return err
		}
	}
	return nil
}

func (block *Block) startAppendRow() {
	if open := block.OpenFlag.Load(); open != nil {
		if open.(bool) {
			log.Printf("error, block concurrent write is open now, openFlag: %v", open.(bool))
			return
		}
	}
	block.OpenFlag.Store(true)

	if block.BatchColSize <= 0 {
		block.BatchColSize = DefaultBatchColSize
	}
	colSize := len(block.Columns)
	if colSize%block.BatchColSize == 0 {
		block.ThreadSize = colSize / block.BatchColSize
	} else {
		block.ThreadSize = colSize/block.BatchColSize + 1
	}

	block.Err = nil
	block.ErrChan = make(chan error, 1000)
	block.AppendChan = make([]chan *Payload, 0, block.ThreadSize)
	for i := 0; i < block.ThreadSize; i++ {
		block.AppendChan = append(block.AppendChan, make(chan *Payload, 1000))
	}

	block.WgErr.Add(1)
	go func() {
		defer block.WgErr.Done()
		for e := range block.ErrChan {
			if e != nil {
				log.Println(e)
				block.Err = e
			}
		}
	}()

	for i := 0; i < block.ThreadSize; i++ {
		block.Wg.Add(1)
		go block.asyncAppendRow(block.AppendChan[i])
	}

}
func (block *Block) EndAppendRow() error {
	open := block.OpenFlag.Load()
	if open == nil {
		return nil
	}
	if !open.(bool) {
		//log.Printf("block concurrent write is not open, openFlag: %v", open.(bool))
		return nil
	}
	block.OpenFlag.Store(false)

	//首先关闭写入的chan
	for i := 0; i < block.ThreadSize; i++ {
		close(block.AppendChan[i])
	}
	block.Wg.Wait()

	//关闭接受错误的chan
	close(block.ErrChan)
	block.WgErr.Wait()
	if block.Err != nil {
		return block.Err
	}
	return nil
}

func (block *Block) asyncAppendRow(payloadChan chan *Payload) {
	defer func() {
		block.Wg.Done()
	}()
	for payload := range payloadChan {
		idx := payload.Idx
		for num, c := range payload.Cols {
			errCh := block.ErrChan
			switch column := c.(type) {
			case *column.Array:
				value := reflect.ValueOf(payload.Vals[num])
				if value.Kind() != reflect.Slice {
					errCh <- fmt.Errorf("unsupported Array(T) type [%T]", value.Interface())
				}
				if err := block.writeArray(c, newValue(value), idx[num], 1); err != nil {
					errCh <- err
				}
			case *column.Nullable:
				if err := column.WriteNull(block.buffers[idx[num]].Offset, block.buffers[idx[num]].Column, payload.Vals[num]); err != nil {
					errCh <- err
				}
			default:
				if err := column.Write(block.buffers[idx[num]].Column, payload.Vals[idx[num]]); err != nil {
					errCh <- err
				}
			}
		}
	}
}

func (block *Block) AppendRow(args []driver.Value) error {
	if len(block.Columns) != len(args) {
		return fmt.Errorf("block: expected %d arguments (columns: %s), got %d", len(block.Columns), strings.Join(block.ColumnNames(), ", "), len(args))
	}
	block.Reserve()
	{
		block.NumRows++
	}

	openFlag := block.OpenFlag.Load()
	if openFlag == nil {
		return fmt.Errorf("block concurrent write is not start, openFlag is nil")
	}
	if !openFlag.(bool) {
		return fmt.Errorf("block concurrent write is not start")
	}

	for i := 0; i < block.ThreadSize; i++ {
		sIdx := block.BatchColSize * i
		eIdx := block.BatchColSize * (i + 1)
		if eIdx > len(block.Columns) {
			eIdx = len(block.Columns)
		}
		var indexs []int
		for k := sIdx; k < eIdx; k++ {
			indexs = append(indexs, k)
		}
		payload := &Payload{
			Idx:  indexs,
			Cols: block.Columns[sIdx:eIdx],
			Vals: args[sIdx:eIdx],
		}
		block.AppendChan[i] <- payload
	}

	//for num, c := range block.Columns {
	//	switch column := c.(type) {
	//	case *column.Array:
	//		value := reflect.ValueOf(args[num])
	//		if value.Kind() != reflect.Slice {
	//			return fmt.Errorf("unsupported Array(T) type [%T]", value.Interface())
	//		}
	//		if err := block.writeArray(c, newValue(value), num, 1); err != nil {
	//			return err
	//		}
	//	case *column.Nullable:
	//		if err := column.WriteNull(block.buffers[num].Offset, block.buffers[num].Column, args[num]); err != nil {
	//			return err
	//		}
	//	default:
	//		if err := column.Write(block.buffers[num].Column, args[num]); err != nil {
	//			return err
	//		}
	//	}
	//}

	return nil
}

func (block *Block) Reserve() {
	if len(block.buffers) == 0 {
		block.buffers = make([]*buffer, len(block.Columns))
		block.offsets = make([]offset, len(block.Columns))
		for i := 0; i < len(block.Columns); i++ {
			var (
				offsetBuffer = wb.New(wb.InitialSize)
				columnBuffer = wb.New(wb.InitialSize)
			)
			block.buffers[i] = &buffer{
				Offset:       binary.NewEncoder(offsetBuffer),
				Column:       binary.NewEncoder(columnBuffer),
				offsetBuffer: offsetBuffer,
				columnBuffer: columnBuffer,
			}
		}
		block.startAppendRow()
	}
}

func (block *Block) Reset() {
	block.NumRows = 0
	block.NumColumns = 0
	block.Values = block.Values[:0]
	block.Columns = block.Columns[:0]
	block.info.reset()
	for _, buffer := range block.buffers {
		buffer.reset()
	}
	{
		block.offsets = nil
		block.buffers = nil
	}
}

func (block *Block) Write(serverInfo *ServerInfo, encoder *binary.Encoder) error {
	if serverInfo.Revision > 0 {
		if err := block.info.write(encoder); err != nil {
			return err
		}
	}
	if err := encoder.Uvarint(block.NumColumns); err != nil {
		return err
	}
	encoder.Uvarint(block.NumRows)
	defer func() {
		block.NumRows = 0
		for i := range block.offsets {
			block.offsets[i] = offset{}
		}
	}()
	for i, column := range block.Columns {
		encoder.String(column.Name())
		encoder.String(column.CHType())
		if len(block.buffers) == len(block.Columns) {
			for _, offsets := range block.offsets[i] {
				for _, offset := range offsets {
					if err := encoder.UInt64(uint64(offset)); err != nil {
						return err
					}
				}
			}
			if _, err := block.buffers[i].WriteTo(encoder); err != nil {
				return err
			}
		}
	}
	return nil
}

type blockInfo struct {
	num1        uint64
	isOverflows bool
	num2        uint64
	bucketNum   int32
	num3        uint64
}

func (info *blockInfo) reset() {
	info.num1 = 0
	info.isOverflows = false
	info.num2 = 0
	info.bucketNum = 0
	info.num3 = 0
}

func (info *blockInfo) read(decoder *binary.Decoder) error {
	var err error
	if info.num1, err = decoder.Uvarint(); err != nil {
		return err
	}
	if info.isOverflows, err = decoder.Bool(); err != nil {
		return err
	}
	if info.num2, err = decoder.Uvarint(); err != nil {
		return err
	}
	if info.bucketNum, err = decoder.Int32(); err != nil {
		return err
	}
	if info.num3, err = decoder.Uvarint(); err != nil {
		return err
	}
	return nil
}

func (info *blockInfo) write(encoder *binary.Encoder) error {
	if err := encoder.Uvarint(1); err != nil {
		return err
	}
	if err := encoder.Bool(info.isOverflows); err != nil {
		return err
	}
	if err := encoder.Uvarint(2); err != nil {
		return err
	}
	if info.bucketNum == 0 {
		info.bucketNum = -1
	}
	if err := encoder.Int32(info.bucketNum); err != nil {
		return err
	}
	if err := encoder.Uvarint(0); err != nil {
		return err
	}
	return nil
}

type buffer struct {
	Offset       *binary.Encoder
	Column       *binary.Encoder
	offsetBuffer *wb.WriteBuffer
	columnBuffer *wb.WriteBuffer
}

func (buf *buffer) WriteTo(w io.Writer) (int64, error) {
	var size int64
	{
		ln, err := buf.offsetBuffer.WriteTo(w)
		if err != nil {
			return size, err
		}
		size += ln
	}
	{
		ln, err := buf.columnBuffer.WriteTo(w)
		if err != nil {
			return size, err
		}
		size += ln
	}
	return size, nil
}

func (buf *buffer) reset() {
	buf.offsetBuffer.Reset()
	buf.columnBuffer.Reset()
}
