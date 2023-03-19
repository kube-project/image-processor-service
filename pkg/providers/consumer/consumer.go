package consumer

import (
	"encoding/binary"
	"fmt"

	"github.com/nsqio/go-nsq"
	"github.com/rs/zerolog"

	"github.com/kube-project/image-processor-service/pkg/providers"
)

// Config configuration options for the consumer service.
type Config struct {
	NsqAddress string
}

// Dependencies of the consumer provider.
type Dependencies struct {
	Logger zerolog.Logger
}

type consumer struct {
	Config
	Dependencies
}

// NewConsumer creates a new consumer provider.
func NewConsumer(cfg Config, deps Dependencies) providers.ConsumerProvider {
	return &consumer{
		Config:       cfg,
		Dependencies: deps,
	}
}

// Consume consumes an entry from NSQ.
func (c *consumer) Consume(sendTo chan int) error {
	config := nsq.NewConfig()
	q, err := nsq.NewConsumer("images", "ch", config)
	if err != nil {
		// this is a hard error, without the queue this service is useless.
		c.Logger.Fatal().Err(err).Msg("failed to initiate new NSQ consumer")
		return fmt.Errorf("failed to initiate NSQ consumer: %w", err)
	}

	q.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		data := binary.LittleEndian.Uint64(message.Body)
		sendTo <- int(data)
		return nil
	}))

	if err := q.ConnectToNSQLookupd(c.NsqAddress); err != nil {
		// this is a hard error, without the queue this service is useless.
		c.Logger.Fatal().Err(err).Str("address", c.NsqAddress).Msg("failed to connect to NSQ")
		return fmt.Errorf("failed to connect to NSQ: %w", err)
	}

	return nil
}
