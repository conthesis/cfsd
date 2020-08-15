package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/fx"
)

const baseTopic = "conthesis.cfs."
const getFileTopic = baseTopic + "get"
const putFileTopic = baseTopic + "put"
const readLinkTopic = baseTopic + "readlink"
const listTopic = baseTopic + "list"

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
		return
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

type ListFilesRequest struct {
	Prefix string `json:"prefix"`
}

type ListFilesResponse struct {
	Success bool `json:"success"`
	Status string `json:"status,omitempty"`
	Contents []string `json:"contents"`
}

func (lfr ListFilesResponse) addPrefixToContents(prefix string) {
	for i := range lfr.Contents {
		lfr.Contents[i] = prefix + lfr.Contents[i]
	}
}

var InternalErrorResp ListFilesResponse = ListFilesResponse{Success: false, Status: "INTERNAL_ERROR"}

func listFilesRespond(m *nats.Msg, response ListFilesResponse) error {
	resp, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if err := m.Respond(resp); err != nil {
		return err
	}
	return nil
}

func (c *cfsd) listFiles(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	req := ListFilesRequest{}
	if err := json.Unmarshal(m.Data, &req); err != nil {
		log.Printf("List request had bad format %v", err)
		resp := ListFilesResponse{Success: false, Status: "BAD_FORMAT"}
		if err := listFilesRespond(m, resp); err != nil {
			log.Printf("Unable to send error %v", err)
		}
		return
	}

	pathPrefix, entry := c.mtab.Match(req.Prefix)

	if entry == nil {
		resp := ListFilesResponse{Success: true, Contents: c.mtab.FilesystemsMatching(req.Prefix)}
		if err := listFilesRespond(m, resp); err != nil {
			log.Printf("Unable to send file systems")
		}
		return
	}

	var sinkForListing *MTabSink = nil
	switch e := entry.(type) {
	case *MTabSink:
		sinkForListing = e
	case *MTabSymlinks:
		symEnt, _, err := c.mtab.ExtractFromSym(e)
		if err != nil {
			log.Printf("No filesystem matching symlinked fs %v", e)
		}
		sinkForListing = symEnt
	}

	if sinkForListing == nil {
		log.Printf("Filesystem match was of an unsupported type %v", entry)
		if err := listFilesRespond(m, InternalErrorResp); err != nil {
			log.Printf("Unable to send INTERNAL_ERROR")
		}
		return
	}

	if sinkForListing.topicFor("list") == "" {
		resp := ListFilesResponse{Success: true, Status: "NOT_LISTABLE"}
		if err := listFilesRespond(m, resp); err != nil {
			log.Printf("Unable to send empty list of files %v", err)
		}
	}

	result, err := performOperation(ctx, c.nc, sinkForListing, "list", []byte(pathPrefix))
	if err != nil {
		log.Printf("Error requesting from upstream %v", err)
		if err := listFilesRespond(m, InternalErrorResp); err != nil {
			log.Printf("Unable to send internal error %v", err)
		}
	}
	listResponse := ListFilesResponse{}
	if err := json.Unmarshal(result, &listResponse); err != nil {
		log.Printf("Unable to read upstream response: %v", err)
		if err := listFilesRespond(m, InternalErrorResp); err != nil {
			log.Printf("Unable to send internal error %v", err)
		}
	}

	// The original request minus what was sent to the upstream is the stripped away prefix.
	prefix := strings.TrimSuffix(req.Prefix, pathPrefix)
	listResponse.addPrefixToContents(prefix + "/")
	if err := listFilesRespond(m, listResponse); err != nil {
		log.Printf("Unable to send respone %v", err)
	}
}



func delegateToUpstream(nc *nats.Conn, sink *MTabSink, operation string, argument []byte, reply string) error {
	topic := sink.topicFor(operation)
	if topic == "" {
		log.Printf("Topic for operation %v was not set", operation)
		return nc.Publish(reply, []byte(""))
	}
	log.Printf("Delegating to %v", topic)
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
	subs := map[string]nats.MsgHandler{
		getFileTopic: c.getFile,
		putFileTopic: c.putFile,
		readLinkTopic: c.readLink,
		listTopic: c.listFiles,
	}
	for subj, fn := range subs {
		if _, err := c.nc.Subscribe(subj, fn); err != nil {
			return err
		}
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
