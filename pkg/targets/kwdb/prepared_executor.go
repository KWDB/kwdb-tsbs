package kwdb

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/timescale/tsbs/pkg/targets/kwdb/commonpool"
)

type PreparedStatementSpec struct {
	Name                 string
	SQL                  string
	RowColumnCount       int
	ParameterFormatCodes []int16
}

type PreparedStatementHandle interface {
	preparedStatementHandle()
}

type standardPreparedHandle struct{}

func (*standardPreparedHandle) preparedStatementHandle() {}

type preparedBatchClient interface {
	Exec(context.Context, string) error
	PrepareStatement(context.Context, string, string) (*pgconn.StatementDescription, error)
	ExecPreparedStatement(context.Context, string, [][]byte, []int16, []int16) error
}

type PreparedBatchExecutor interface {
	Prepare(context.Context, preparedBatchClient, PreparedStatementSpec) (PreparedStatementHandle, error)
	Exec(context.Context, preparedBatchClient, PreparedStatementSpec, PreparedStatementHandle, [][]byte) error
}

type standardPrepareExecutor struct{}

func newStandardPrepareExecutor() PreparedBatchExecutor {
	return &standardPrepareExecutor{}
}

func (e *standardPrepareExecutor) Prepare(
	ctx context.Context, client preparedBatchClient, spec PreparedStatementSpec,
) (PreparedStatementHandle, error) {
	_, err := client.PrepareStatement(ctx, spec.Name, spec.SQL)
	if err != nil {
		return nil, err
	}

	return &standardPreparedHandle{}, nil
}

func (e *standardPrepareExecutor) Exec(
	ctx context.Context, client preparedBatchClient, spec PreparedStatementSpec, _ PreparedStatementHandle, args [][]byte,
) error {
	return client.ExecPreparedStatement(ctx, spec.Name, args, spec.ParameterFormatCodes, nil)
}

var _ preparedBatchClient = (*commonpool.Conn)(nil)
