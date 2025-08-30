package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/indeedhat/dotenv"
	_ "github.com/indeedhat/dotenv/autoload"
)

const (
	MqHost dotenv.String = "MQ_HOST"
	MqPort dotenv.String = "MQ_PORT"
	MqUser dotenv.String = "MQ_USER"
	MqPass dotenv.String = "MQ_PASS"
)

func main() {
	svcName, topics := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, subCh, pubCh, err := connect()
	if err != nil {
		log.Fatalf("failed to connect to broker: %s", err)
	}
	defer conn.Close()
	defer pubCh.Close()
	defer subCh.Close()

	if err := buildTopology(subCh, svcName, topics); err != nil {
		log.Fatalf("failed to build topology: %s", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	if err := consumeEvents(ctx, subCh, pubCh, svcName); err != nil {
		log.Fatal("failed to start consumer")
	}

	<-time.After(time.Second)

	for _, topic := range topics {
		publishEvent(
			ctx,
			pubCh,
			ExchangePublish,
			topic,
			fmt.Sprintf("test message: \nsvc(%s) \ntopic(%s)", svcName, topic),
		)
	}

	select {
	case <-ctx.Done():
		log.Print("context closed")
	case <-quit:
		log.Print("user canceled Ctrl+C")
	}
}

type flagList []string

// Set implements flag.Value.
func (f *flagList) Set(v string) error {
	log.Println("setting topic flag: ", v)
	*f = append(*f, v)
	return nil
}

// String implements flag.Value.
func (f *flagList) String() string {
	return strings.Join([]string(*f), ", ")
}

var _ flag.Value = (*flagList)(nil)

func parseFlags() (string, []string) {
	var topics flagList
	flag.Var(&topics, "t", "Topics to listen to")

	flag.Parse()

	log.Printf("%v %v", topics, flag.Args())

	if len(flag.Args()) < 1 {
		fmt.Print("error: Service name is required\n\n")
		goto printUsage
	} else if len(topics) < 1 {
		fmt.Print("error: at least one topic is required\n\n")
		goto printUsage
	}

	return flag.Arg(0), []string(topics)

printUsage:
	fmt.Println("Usage:\n\t./node -t [topic_name] [service_name]")
	flag.PrintDefaults()
	os.Exit(1)
	return "", nil
}
