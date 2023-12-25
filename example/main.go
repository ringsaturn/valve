package main

import (
	"context"
	"log"
	"time"

	"github.com/ringsaturn/valve"
	"golang.org/x/sync/errgroup"
)

type Task struct {
	Time time.Time
}

func Producer(valveCore *valve.Core[*Task], producerFreq time.Duration, timeout time.Duration) error {
	ticker := time.NewTicker(producerFreq)
	for {
		select {
		case <-ticker.C:
			addFunc := func() error {
				ctx, cancel := context.WithTimeout(context.TODO(), timeout)
				defer cancel()
				return valveCore.Add(ctx, &Task{Time: time.Now()})
			}

			if err := addFunc(); err != nil {
				return err
			}
		}
	}
}

func Consumer(valveCore *valve.Core[*Task], ioTime time.Duration) error {
	out, err := valveCore.Receive()
	if err != nil {
		return err
	}
	for {
		select {
		case batchItem := <-out:
			valveCore.DoneInCounter()
			log.Println("batchItem", valveCore.GetInCounter(), batchItem)
			// mock IO
			time.Sleep(ioTime)
		}
		log.Println("lag", valveCore.GetInCounter(), valveCore.GetOutinCounter())
	}
}

func main() {
	initCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	valveCore, err := valve.NewCore[*Task](time.NewTicker(100*time.Millisecond), 100, 100, 2)
	if err != nil {
		panic(err)
	}

	group, groupCtx := errgroup.WithContext(initCtx)

	producerCount := 5
	startWorkerCount := 5
	consumerCount := 3

	for i := 0; i < producerCount; i++ {
		group.Go(func() error {
			return Producer(valveCore, time.Millisecond, 10*time.Millisecond)
		})
	}

	for i := 0; i < startWorkerCount; i++ {
		group.Go(func() error {
			return valveCore.Start(groupCtx)
		})
	}

	for i := 0; i < consumerCount; i++ {
		group.Go(func() error {
			return Consumer(valveCore, 100*time.Millisecond)
		})
	}

	panic(group.Wait())
}
