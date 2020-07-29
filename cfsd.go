package main

import (
	"context"
	"github.com/nats-io/nats.go"
	"log"
	url "net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
	"errors"
)

const getFileTopic = "conthesis.cfs.get"
const putFileTopic = "conthesis.cfs.put"

func getRequiredEnv(env string) string {
	val := os.Getenv(env)
	if val == "" {
		log.Fatalf("`%s`, a required environment variable was not set", env)
	}
	return val
}

func connectNats() *nats.Conn {
	natsURL := getRequiredEnv("NATS_URL")
	nc, err := nats.Connect(natsURL)

	if err != nil {
		if err, ok := err.(*url.Error); ok {
			log.Fatalf("NATS_URL is of an incorrect format: %s", err.Error())
		}
		log.Fatalf("Failed to connect to NATS %T: %s", err, err)
	}
	return nc
}

type cfsd struct {
	nc      *nats.Conn
	mtab    *MTab
}

func (c *cfsd) getFile(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	path := string(m.Data)

	path_prefix, ent := c.mtab.Match(path)

	if ent == nil {
		log.Printf("No such filesystem matching path=%v", path)
		m.Respond([]byte(""))
	}

	switch e := ent.(type) {
	case *MTabSymlinks:
		sym_ent, ok_link := c.mtab.MatchValueOnly(e.LinkPath).(*MTabSink);
		if !ok_link {
			log.Printf("Symlink sink entry not found path=%v dest=%v", path, e.LinkPath)
			return
		}
		dst_ent, ok_dest := c.mtab.MatchValueOnly(e.DestPath).(*MTabSink);
		if !ok_dest {
			log.Printf("Destination sink entry not found path=%v dest=%v", path, e.DestPath)
			return
		}
		linked_path, err := performOperation(ctx, c.nc, sym_ent, "get", []byte(path_prefix))
		if err != nil {
			log.Printf("Failed operation path=%v: %v", path, err)
			return
		}
		err = delegateToUpstream(c.nc, dst_ent, "get", []byte(linked_path), m.Reply)
		if err != nil {
			log.Printf("Unable to delegate to upstream: %v", err)
			return
		}
	case *MTabSink:
		err := delegateToUpstream(c.nc, e, "get", []byte(path_prefix), m.Reply)
		if err != nil {
			log.Printf("Unable to delegate to upstream: %v", err)
			return
		}
	}
}



func delegateToUpstream(nc *nats.Conn, sink *MTabSink, operation string, argument []byte, reply string) error {
	topic := sink.topicFor(operation)
	if topic == "" {
		log.Printf("Topic for operation %v was not set", operation)
		return nc.Publish(reply, []byte(""))
	}
	log.Printf("topic=%v argument=%v", topic, string(argument))
	return nc.PublishRequest(topic, reply, argument)
}

func performOperation(ctx context.Context, nc *nats.Conn, sink *MTabSink, operation string, argument []byte) ([]byte, error) {
	topic := sink.topicFor(operation)

	if topic == "" {
		return nil, errors.New("Operation not set for sink")
	}

	msg, err := nc.RequestWithContext(ctx, topic, argument)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}


func waitForTerm() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	<-done
}

func (c *cfsd) setupSubscriptions() {
	_, err := c.nc.Subscribe(getFileTopic, c.getFile)
	if err != nil {
		log.Fatalf("Unable to subscribe to topic %s: %s", getFileTopic, err)
	}
}

func (c *cfsd) Close() {
	log.Printf("Shutting down...")
	c.nc.Drain()
}

func main() {
	nc := connectNats()
	mtab := NewMTab()
	err := mtab.LoadDefaultMTab()
	if err != nil {
		log.Fatalf("Unable to load mtab %v", err)
	}
	cfsd := cfsd{nc: nc, mtab: mtab}
	defer cfsd.Close()
	cfsd.setupSubscriptions()
	log.Printf("Connected to NATS")
	waitForTerm()
}
