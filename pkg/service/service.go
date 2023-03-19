package service

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/kube-project/image-processor-service/pkg/providers"
)

// Dependencies are providers which this service operates with.
type Dependencies struct {
	Consumer  providers.ConsumerProvider
	Processor providers.ProcessorProvider
	Logger    zerolog.Logger
}

// Service interface defines a service which can Run something.
type Service interface {
	Run(ctx context.Context) error
}

// New creates a new service with all of its dependencies and configurations.
func New(deps Dependencies) Service {
	return &imageProcessor{
		deps: deps,
	}
}

// Service represents the service object of the receiver.
type imageProcessor struct {
	deps Dependencies
}

// Run starts the this service.
// TODO: Pass the context?
func (s *imageProcessor) Run(ctx context.Context) error {
	s.deps.Logger.Info().Msg("Starting service...")

	// Create the channel on which the consumer and the processor can communicate.
	// This should be buffered.
	mediator := make(chan int, 1)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := s.deps.Consumer.Consume(mediator); err != nil {
			return fmt.Errorf("failed to start consumer: %w", err)
		}

		return nil
	})
	g.Go(func() error {
		if err := s.deps.Processor.ProcessImages(ctx, mediator); err != nil {
			return fmt.Errorf("failed to start process images: %w", err)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to wait for consumer and process image: %w", err)
	}

	return nil
}
