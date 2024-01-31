package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/etcd"
	"github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

func main() {
	db, err := sqlite.New("example", sqlite.Config{
		BusyTimeout: 5,
	})
	if err != nil {
		panic(err)
	}

	etcd, err := etcd.New("etcdb")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	stream := blockqueue.New(db, etcd)

	err = stream.Run(ctx)
	if err != nil {
		panic(err)
	}

	request := io.Topic{
		Name: "test",
		Subscribers: io.Subscribers{
			{
				Name: "test_subscriber",
			},
		},
	}

	topic := request.Topic()

	err = stream.AddJob(ctx, topic, request.Subscriber(topic.Id))
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			response, err := stream.Read(context.Background(), topic, "test_subscriber")
			if err != nil {
				log.Printf("error %v", err)
				continue
			}

			for _, r := range response {
				log.Println("Response Message : ", r.Message)
				err = stream.Ack(context.Background(), topic, "test_subscriber", r.Id)
				if err != nil {
					log.Printf("error ack message : %v", err)
					continue
				}

				log.Println("success ack message")
			}
		}
	}()

	for i := 0; i < 10; i++ {
		stream.Publish(ctx, topic, io.Publish{
			Message: fmt.Sprintf("Test %v", i),
		})
	}

	time.Sleep(5 * time.Second)
}
