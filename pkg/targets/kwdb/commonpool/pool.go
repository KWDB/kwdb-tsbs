package commonpool

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/silenceper/pool"
	"github.com/timescale/tsbs/pkg/targets/kwdb/thread"
	"sync"
)

type ConnectorPool struct {
	host     string
	user     string
	password string
	certdir  string
	port     int
	pool     pool.Pool
}

func NewConnectorPool(user, password, host, certdir string, port int) (*ConnectorPool, error) {
	a := &ConnectorPool{user: user, password: password, host: host, certdir: certdir, port: port}
	poolConfig := &pool.Config{
		InitialCap:  1,
		MaxCap:      10000,
		MaxIdle:     10000,
		Factory:     a.factory,
		Close:       a.close,
		IdleTimeout: -1,
	}
	p, err := pool.NewChannelPool(poolConfig)
	if err != nil {
		return nil, err
	}
	a.pool = p
	return a, nil
}

func (a *ConnectorPool) factory() (interface{}, error) {
	thread.Lock()
	defer thread.Unlock()

	url := ""
	if len(a.certdir) == 0 {
		url = fmt.Sprintf("dbname=defaultdb host=%s port=%d user=%s password=%s default_query_exec_mode=simple_protocol", a.host, a.port, a.user, a.password)
	} else {
		url = fmt.Sprintf("dbname=defaultdb host=%s port=%d user=%s password=%s sslmode=verify-ca sslcert=%s/client.root.crt sslkey=%s/client.root.key sslrootcert=%s/ca.crt default_query_exec_mode=simple_protocol",
			a.host, a.port, a.user, a.password, a.certdir, a.certdir, a.certdir)
	}

	return pgx.Connect(context.Background(), url)
}

func (a *ConnectorPool) close(v interface{}) error {
	if v != nil {
		thread.Lock()
		defer thread.Unlock()
		conn := v.(*pgx.Conn)
		a.Close(conn)
	}
	return nil
}

func (a *ConnectorPool) Get() (*pgx.Conn, error) {
	v, err := a.pool.Get()
	if err != nil {
		return nil, err
	}
	return v.(*pgx.Conn), nil
}

func (a *ConnectorPool) Put(c *pgx.Conn) error {
	return a.pool.Put(c)
}

func (a *ConnectorPool) Close(c *pgx.Conn) error {
	return a.pool.Close(c)
}

func (a *ConnectorPool) Release() {
	a.pool.Release()
}

func (a *ConnectorPool) verifyPassword(password string) bool {
	return password == a.password
}

var connectionMap = sync.Map{}

type Conn struct {
	Connection *pgx.Conn
	pool       *ConnectorPool
}

func (c *Conn) Put() error {
	return c.pool.Put(nil)
}

func GetConnection(user, password, host, certdir string, port int) (*Conn, error) {
	newPool, err := NewConnectorPool(user, password, host, certdir, port)
	if err != nil {
		return nil, err
	}
	connectionMap.Store(user, newPool)
	c, err := newPool.Get()
	if err != nil {
		return nil, err
	}
	return &Conn{
		Connection: c,
		pool:       newPool,
	}, nil
}
