package fetcher

import (
	"context"
	"github.com/NYTimes/gizmo/pubsub"
	"github.com/lyft/flytestdlib/logger"
	"time"
)

type Processor interface {
	Process(ctx context.Context, m []byte) error
}

type WorkFetcher interface {
	RunUntil(ctx context.Context) error
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

type fetcher struct {
	subscriber   pubsub.Subscriber
	retryBackoff time.Duration
	proc         Processor
}

func (f *fetcher) RunUntil(ctx context.Context) error {
	// TODO handle graceful termination - wait for ctx.Done()
	for {
		logger.Infof(ctx, "Restarting Fetcher loop")
		err := f.Run(ctx)
		if err != nil {
			// Do nothing for now. maybe log
		}
		time.Sleep(f.retryBackoff)
	}
	return nil
}

func (f *fetcher) Run(ctx context.Context) error {
	for m := range f.subscriber.Start() {
		if err := f.proc.Process(ctx, m.Message()); err != nil {
			logger.Errorf(ctx, "Failed to process message, Reason: [%s]", err)
		} else {
			logger.Debugf(ctx, "Message processed successfully, Acking")
			if err := m.Done(); err != nil {
				logger.Debugf(ctx, "Failed to ack Message!")
			} else {
				logger.Debugf(ctx, "Message acked successfully!")
			}
		}
	}
	err := f.subscriber.Err()
	if err != nil {
		logger.Errorf(ctx, "Error when fetching from remote: [%v]", err)
		return err
	}
	logger.Infof(ctx, "Subscriber exiting")
	return nil
}

func (f *fetcher) Stop(ctx context.Context) error {
	return f.subscriber.Stop()
}

func NewFetcher(ctx context.Context, sub pubsub.Subscriber, proc Processor) WorkFetcher {
	return &fetcher{
		subscriber:   sub,
		retryBackoff: time.Second * 2,
		proc:         proc,
	}
}
