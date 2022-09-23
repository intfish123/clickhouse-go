package tests

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"testing"
)

func TestBlock(t *testing.T) {
	b := &proto.Block{}
	b.Columns = make([]column.Interface, 10)
	b.ColBatchSize = 60
	if err := b.OpenConcurrentWrite(); err != nil {
		t.Error(err)
	} else {
		t.Log("OpenConcurrentWrite")
		if err := b.Append(1, 2, 3, 4, 5, 6, 7, 8, 9, 10); err != nil {
			t.Error(err)
		} else {
			t.Log("Append")
		}

	}
}
