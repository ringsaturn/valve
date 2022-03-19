package valve

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrUnableAdd = errors.New("unable to add")
var ErrReceiveInternalQueue = errors.New("unable read internal queue")

type Core struct {
	Ticker     *time.Ticker       // 至少多久尝试输出一次
	BatchSize  int64              // 一次聚合多少
	inCounter  int64              // 输入队列元素数量，不是严格的 chan 的实际元素数量
	in         chan interface{}   // 输入队列
	outCounter int64              // 输出队列元素数量，不是严格的 chan 的实际元素数量
	out        chan []interface{} // 输出队列
}

func NewCore(ticker *time.Ticker, batchSize int64, inChanSize int, outChanSize int) (*Core, error) {
	c := &Core{
		Ticker:    ticker,
		BatchSize: batchSize,
		in:        make(chan interface{}, inChanSize),
		out:       make(chan []interface{}, outChanSize),
	}
	return c, nil
}

func (c *Core) GetInCounter() int64 {
	return atomic.LoadInt64(&c.inCounter)
}

func (c *Core) GetOutinCounter() int64 {
	return atomic.LoadInt64(&c.outCounter)
}

func (c *Core) DoneInCounter() {
	atomic.AddInt64(&c.outCounter, -1)
}

func (c *Core) Add(ctx context.Context, item interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.in <- item:
		atomic.AddInt64(&c.inCounter, 1)
		return nil
	default:
		return ErrUnableAdd
	}
}

// BAdd will block unitl tiem add to in chan
func (c *Core) BAdd(item interface{}) error {
	c.in <- item
	atomic.AddInt64(&c.inCounter, 1)
	return nil
}

func (c *Core) Receive() (chan []interface{}, error) {
	return c.out, nil
}

func (c *Core) Start(ctx context.Context) error {
	outItem := make([]interface{}, 0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.Ticker.C:
			if len(outItem) == 0 {
				continue
			}
			select {
			case c.out <- outItem:
				outItem = make([]interface{}, 0)
				atomic.AddInt64(&c.outCounter, 1)
			default:
				continue
			}
		case item := <-c.in:
			outItem = append(outItem, item)
			atomic.AddInt64(&c.inCounter, -1)
		}

		if len(outItem) >= int(c.BatchSize) {
			select {
			case c.out <- outItem:
				outItem = make([]interface{}, 0)
				atomic.AddInt64(&c.outCounter, 1)
			default:
				continue
			}
		}
	}
}
