package main

import (
	"context"
	"github.com/nats-io/nats.go"
	"log"
	url "net/url"
	"os"
	"time"
	"errors"
	"bytes"
	"go.uber.org/fx"
	"fmt"
)

const getFileTopic = "conthesis.cfs.get"
const putFileTopic = "conthesis.cfs.put"
const readLinkTopic = "conthesis.cfs.readlink"

func getRequiredEnv(env string) (string, error) {
	val := os.Getenv(env)
	if val == "" {
		return "", fmt.Errorf("`%s`, a required environment variable was not set", env)
	}
	return val, nil
}

func NewNats(lc fx.Lifecycle) (*nats.Conn, error) {
	natsURL, err := getRequiredEnv("NATS_URL")
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(natsURL)

	if err != nil {
		if err, ok := err.(*url.Error); ok {
			return nil, fmt.Errorf("NATS_URL is of an incorrect format: %w", err)
		}
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return nc.Drain()
		},
	})

	return nc, nil
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
		sym_ent, dst_ent, err := c.mtab.ExtractFromSym(e)
		if err != nil {
			log.Printf("Failed to extract targets for symlink: %v", err)
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


func (c *cfsd) putFile(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	buf := bytes.NewBuffer(m.Data)
	path, err := buf.ReadString('\n')
	if err != nil {
		log.Printf("Bad input format: %v", err)
		m.Respond([]byte("ERR"))
		return
	}
	path = path[:len(path) - 1] // Cut out the delimiter
	data := buf.Next(len(m.Data)) // Guaranteed to be the rest...

	path_prefix, ent := c.mtab.Match(path)

	if ent == nil {
		log.Printf("No such filesystem matching path=%v", path)
		m.Respond([]byte("ERR"))
	}
	switch e := ent.(type) {
	case *MTabSymlinks:
		sym_ent, dst_ent, err := c.mtab.ExtractFromSym(e)
		if err != nil {
			log.Printf("Failed to extract targets for symlink: %v", err)
			m.Respond([]byte("ERR"))
			return
		}

		linked_path, err := performOperation(ctx, c.nc, dst_ent, "post", data)
		if err != nil {
			log.Printf("Failed operation path=%v: %v", path, err)
			m.Respond([]byte("ERR"))
			return
		}
		payload := makePutPayload(path_prefix, &linked_path)
		err = delegateToUpstream(c.nc, sym_ent, "put", payload, m.Reply)
		if err != nil {
			log.Printf("Unable to delegate to upstream: %v", err)
			m.Respond([]byte("ERR"))
 			return
		}
	case *MTabSink:
		payload := makePutPayload(path_prefix, &data)
		err := delegateToUpstream(c.nc, e, "put", payload, m.Reply)
		if err != nil {
			log.Printf("Unable to delegate to upstream: %v", err)
			m.Respond([]byte("ERR"))
			return
		}
	}
}

func makePutPayload(path string, data *[]byte) []byte {
	buf := bytes.NewBufferString(path)
	buf.Grow(len(*data) + 1)
	buf.WriteRune('\n')
	buf.Write(*data)
	return buf.Bytes()
}

func (c *cfsd) readLink(m *nats.Msg) {
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
		sym_ent, _, err := c.mtab.ExtractFromSym(e)
		if err != nil {
			log.Printf("Failed to extract targets for symlink: %v", err)
			return
		}
		linked_path, err := performOperation(ctx, c.nc, sym_ent, "get", []byte(path_prefix))
		if err != nil {
			log.Printf("Failed operation path=%v: %v", path, err)
			return
		}

		var b bytes.Buffer
		b.Grow(len(e.DestPath) + 1 + len(linked_path))
		b.WriteString(e.DestPath)
		b.WriteRune('/')
		b.Write(linked_path)

		if err := m.Respond(b.Bytes()); err != nil {
			log.Printf("Failed to respond to message")
		}

	case *MTabSink:
		m.Respond(m.Data)
	}

}



func delegateToUpstream(nc *nats.Conn, sink *MTabSink, operation string, argument []byte, reply string) error {
	topic := sink.topicFor(operation)
	if topic == "" {
		log.Printf("Topic for operation %v was not set", operation)
		return nc.Publish(reply, []byte(""))
	}
	log.Printf("Delegating to topic=%v", topic)
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



func setupSubscriptions(c *cfsd) error {
	if _, err := c.nc.Subscribe(getFileTopic, c.getFile); err != nil {
		return err
	}

	if _, err := c.nc.Subscribe(putFileTopic, c.putFile); err != nil {
		return err
	}

	if _, err := c.nc.Subscribe(readLinkTopic, c.readLink); err != nil {
		return err
	}
	return nil
}

func NewCFSD(nc *nats.Conn, mtab *MTab) *cfsd {
	return &cfsd{nc: nc, mtab: mtab}
}

func main() {
	fx.New(
		fx.Provide(
			NewNats,
			NewMTab,
			NewCFSD,
		),
		fx.Invoke(setupSubscriptions),
	).Run()
}
