package main
import (
	"github.com/dghubble/trie"
	"log"
	"os"
	"strings"
	"gopkg.in/yaml.v2"
	"errors"
	"fmt"
)

var InvalidSymlinkError = errors.New("emit macho dwarf: elf header corrupted")

type MTabSink struct {
	TopicMap map[string]string
}

type MTabSymlinks struct {
	LinkPath string
	DestPath string
}

type MTabEntry interface {
	isMTabEntry()
}

func (s *MTabSink) topicFor(operation string) string {
	return s.TopicMap[operation]
}

func (s *MTabSink) isMTabEntry() {}
func (s *MTabSymlinks) isMTabEntry() {}

type MTab struct {
	trie *trie.PathTrie
}

type MTabFile struct {
	Symlinks map[string]*MTabSymlinks `yaml:omitempty`
	Sinks map[string]*MTabSink `yaml:omitempty`
}

func (mtf *MTabFile) FromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.SetStrict(true)
	return dec.Decode(mtf)
}

func findLongestPrefixMatch(trie *trie.PathTrie, path string) (string, interface{}) {
	var result interface{}
	var prefix string
	err := trie.WalkPath(path, func(k string, v interface{}) error {
		prefix = k
		result = v
		return nil
	})
	if (err != nil) {
		log.Panicf("Error finding trie entry, %s", err)
	}
	return prefix, result
}

func (mt *MTab) LoadDefaultMTab() error {
	f := &MTabFile{}
	err := f.FromFile("mtab.yaml")
	if err != nil {
		return err
	}

	for k, v := range f.Sinks {
		mt.trie.Put(k, v)
	}

	for k, v := range f.Symlinks {
		mt.trie.Put(k , v)
	}
	return nil
}

func NewMTab() (*MTab, error) {
	mt :=  &MTab{
		trie: trie.NewPathTrie(),
	}
	err := mt.LoadDefaultMTab()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MTab) Match(path string) (string, interface{}) {
	prefix, v := findLongestPrefixMatch(mt.trie, path)
	if v == nil {
		return "", nil
	}
	woPrefix := strings.TrimPrefix(path, prefix)
	return strings.TrimPrefix(woPrefix, "/"), v
}

func (mt *MTab) MatchValueOnly(path string) interface{} {
	_, v := mt.Match(path)
	return v
}

func (mt *MTab) ExtractFromSym(e *MTabSymlinks) (*MTabSink, *MTabSink, error) {
	sym_ent, ok_link := mt.MatchValueOnly(e.LinkPath).(*MTabSink);
	if !ok_link {
		err := fmt.Errorf("%w entry not found %v", InvalidSymlinkError, e.LinkPath)
		return nil, nil, err
	}
	dst_ent, ok_dest := mt.MatchValueOnly(e.DestPath).(*MTabSink);
	if !ok_dest {
		err := fmt.Errorf("%w Destination sink entry not found %v", InvalidSymlinkError, e.DestPath)
		return nil, nil, err
	}
	return sym_ent, dst_ent, nil
}

func (mt *MTab) FilesystemsMatching(prefix string) []string {
	ents := make([]string, 0)
	mt.trie.Walk(func(key string, _ interface{}) error {
		if strings.HasPrefix(key, prefix) {
			ents = append(ents, key[1:])
		}
		return nil
	})
	return ents
}