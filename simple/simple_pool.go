package simple

import (
	"github.com/kataras/iris/core/errors"
	"sync"
	"time"
)

// 连接资源接口
type ConnRes interface {
	Close() error
}

// 工厂方法，用于创建连接
type Factory func() (ConnRes, error)

// 连接类型
type Conn struct {
	conn ConnRes   // 连接
	time time.Time // 连接时间
}

// 连接池
type ConnPool struct {
	mu      sync.Mutex    // 互斥锁，保证编程安全
	connsCh chan *Conn    // 通道，保存所有的连接
	factory Factory       // 工厂方法，创建连接
	closed  bool          // 连接池是否关闭
	timeout time.Duration // 超时时间
}

// 连接池构造函数
func NewConnPool(factory Factory, cap int, timeout time.Duration) (*ConnPool, error) {
	if cap <= 0 {
		return nil, errors.New("Capacity must not be less than or equal to 0")
	}
	if timeout <= 0 {
		return nil, errors.New("Timeout must not be less than or equal to 0")
	}
	cp := &ConnPool{
		mu:      sync.Mutex{},
		connsCh: make(chan *Conn, cap),
		factory: factory,
		closed:  false,
		timeout: timeout,
	}
	for i := 0; i < cap; i++ {
		cr, err := cp.factory()
		if err != nil {
			cp.Close()
			return nil, errors.New("factory error")
		}
		cp.connsCh <- &Conn{conn: cr, time: time.Now()}
	}
	return cp, nil
}

// 关闭连接池
func (cp *ConnPool) Close() {
	if cp.closed {
		return
	}
	cp.mu.Lock()
	cp.closed = true
	close(cp.connsCh)
	for c := range cp.connsCh {
		c.conn.Close()
	}
	cp.mu.Unlock()
}

// 返回连接池中连接个数
func (cp *ConnPool) Len() int {
	return len(cp.connsCh)
}

// 获得连接
func (cp *ConnPool) Get() (ConnRes, error) {
	if cp.closed {
		return nil, errors.New("ConnPool closed")
	}
	for {
		select {
		case cr, ok := <-cp.connsCh:
			if !ok {
				return nil, errors.New("ConnPool closed")
			}
			if time.Now().Sub(cr.time) > cp.timeout { // 超时，关闭连接，继续获取
				cr.conn.Close()
				continue
			}
			return cr.conn, nil
		default:
			cr, err := cp.factory()
			if err != nil {
				return nil, err
			}
			return cr, nil
		}
	}
}

// 资源放回池中
func (cp *ConnPool) Put(conn ConnRes) error {
	if cp.closed {
		return errors.New("ConnPool closed")
	}
	select {
	case cp.connsCh <- &Conn{conn: conn, time: time.Now()}:
		return nil
	default:
		conn.Close()
		return errors.New("ConnPool is full")
	}
}
