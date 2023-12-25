package valve

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrUnableAdd = errors.New("unable to add")
var ErrReceiveInternalQueue = errors.New("unable read internal queue")

type Core[T any] struct {
	Ticker     *time.Ticker // 至少多久尝试输出一次
	BatchSize  int64        // 一次聚合多少
	inCounter  int64        // 输入队列元素数量，不是严格的 chan 的实际元素数量
	in         chan T       // 输入队列
	outCounter int64        // 输出队列元素数量，不是严格的 chan 的实际元素数量
	out        chan []T     // 输出队列
}

func NewCore[T any](ticker *time.Ticker, batchSize int64, inChanSize int, outChanSize int) (*Core[T], error) {
	c := &Core[T]{
		Ticker:    ticker,
		BatchSize: batchSize,
		in:        make(chan T, inChanSize),
		out:       make(chan []T, outChanSize),
	}
	return c, nil
}

func (c *Core[T]) GetInCounter() int64 {
	return atomic.LoadInt64(&c.inCounter)
}

func (c *Core[T]) GetOutinCounter() int64 {
	return atomic.LoadInt64(&c.outCounter)
}

func (c *Core[T]) DoneInCounter() {
	atomic.AddInt64(&c.outCounter, -1)
}

func (c *Core[T]) Add(ctx context.Context, item T) error {
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
func (c *Core[T]) BAdd(item T) error {
	c.in <- item
	atomic.AddInt64(&c.inCounter, 1)
	return nil
}

func (c *Core[T]) Receive() (chan []T, error) {
	return c.out, nil
}

func (c *Core[T]) Start(ctx context.Context) error {
	outItem := make([]T, 0)
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
				outItem = make([]T, 0)
				atomic.AddInt64(&c.outCounter, 1)
			default:
				continue
			}
		case item := <-c.in:
			outItem = append(outItem, item)
			atomic.AddInt64(&c.inCounter, -1)
		}

		if len(outItem) >= int(c.BatchSize) {
			c.out <- outItem
			outItem = make([]T, 0)
			atomic.AddInt64(&c.outCounter, 1)
		}
	}
}
