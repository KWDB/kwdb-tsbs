package query

import (
	"fmt"
	"sync"
)

type Kwdb struct {
	id               uint64
	HumanLabel       []byte
	HumanDescription []byte
	Hypertable       []byte
	SqlQuery         []byte
}

var KwdbPool = sync.Pool{
	New: func() interface{} {
		return &Kwdb{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Hypertable:       make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

func NewKWDB() *Kwdb {
	return KwdbPool.Get().(*Kwdb)
}

func (q *Kwdb) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Hypertable = q.Hypertable[:0]
	q.SqlQuery = q.SqlQuery[:0]
	KwdbPool.Put(q)
}

func (q *Kwdb) HumanLabelName() []byte {
	return q.HumanLabel
}

func (q *Kwdb) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *Kwdb) GetID() uint64 {
	return q.id
}

func (q *Kwdb) SetID(n uint64) {
	q.id = n
}

func (q *Kwdb) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Hypertable: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Hypertable, q.SqlQuery)
}
