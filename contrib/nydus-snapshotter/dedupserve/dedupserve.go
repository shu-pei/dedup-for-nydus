package dedupserve

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ChunkInfo struct {
	BlobIndex        uint32 `json:"blob_index"`
	Index            uint32 `json:"index"`
	CompressOffset   uint64 `json:"compress_offset"`
	DecompressOffset uint64 `json:"decompress_offset"`
}

type BlobEntry struct {
	ChunkCount         uint32 `json:"chunk_count"`
	ReadaheadOffset    uint32 `json:"readahead_offset"`
	ReadaheadSize      uint32 `json:"readahead_size"`
	BlobID             string `json:"blob_id"`
	BlobIndex          uint32 `json:"blob_index"`
	BlobCacheSize      uint64 `json:"blob_cache_size"`
	CompressedBlobSize uint64 `json:"compressed_blob_size"`
}

type BlobTable struct {
	Entries []BlobEntry `json:"entries"`
}
type Inode struct {
	Id   string      `json:"id"`
	Data []ChunkInfo `json:"data"`
}

type dedupserve struct {
	root       string
	socketPath string

	dedupMu sync.RWMutex
	dedup   map[string]*Inode

	blobMu sync.RWMutex
	blob   map[string]*BlobTable
}

func Newdedupserve(root string) *dedupserve {
	socketPath := filepath.Join(root, "dedup.sock")
	return &dedupserve{
		root:       root,
		socketPath: socketPath,
		dedup:      make(map[string]*Inode),
		blob:       make(map[string]*BlobTable),
	}
}

func (m *dedupserve) PutInode(key string, val *Inode) {
	m.dedupMu.Lock()
	m.dedup[key] = val
	m.dedupMu.Unlock()
}

func (m *dedupserve) GetInode(key string) (*Inode, bool) {
	m.dedupMu.RLock()
	v, ok := m.dedup[key]
	m.dedupMu.RUnlock()
	return v, ok
}

func (m *dedupserve) DelInode(key string) {
	m.dedupMu.Lock()
	delete(m.dedup, key)
	m.dedupMu.Unlock()
}

func (m *dedupserve) PutBlob(key string, val *BlobTable) {
	m.blobMu.Lock()
	m.blob[key] = val
	m.blobMu.Unlock()
}

func (m *dedupserve) GetBlob(key string) (*BlobTable, bool) {
	m.blobMu.RLock()
	v, ok := m.blob[key]
	m.blobMu.RUnlock()
	return v, ok
}

func (m *dedupserve) DelBlob(key string) {
	m.blobMu.Lock()
	delete(m.blob, key)
	m.blobMu.Unlock()
}

type Request struct {
	// op: PUT / GET / DEL
	Op string `json:"op"`
	// kind: "inode" 或 "blob"
	Kind string `json:"kind"`

	// key is used by GET/PUT/DEL
	Key string `json:"key,omitempty"`

	// value is used for PUT; it's raw JSON which we'll unmarshal depending on kind
	Value json.RawMessage `json:"value,omitempty"`
}

type Response struct {
	Status  string      `json:"status"` // ok / error
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// length-prefixed framing: 4-byte big-endian length followed by JSON payload
func writeMessage(w io.Writer, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var lenbuf [4]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(b)))
	if _, err := w.Write(lenbuf[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readMessage(r *bufio.Reader) ([]byte, error) {
	// read 4 bytes length
	lenBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBytes); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBytes)
	if length == 0 {
		return nil, fmt.Errorf("zero length")
	}
	msg := make([]byte, length)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *dedupserve) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute)) // 防止闲置无限挂起
		msg, err := readMessage(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("read error: %v", err)
			return
		}

		var req Request
		if err := json.Unmarshal(msg, &req); err != nil {
			resp := Response{Status: "error", Message: "invalid request: " + err.Error()}
			_ = writeMessage(conn, resp)
			continue
		}

		resp := m.processRequest(&req)
		if err := writeMessage(conn, resp); err != nil {
			log.Printf("write error: %v", err)
			return
		}
	}
}

func (m *dedupserve) processRequest(req *Request) Response {
	switch req.Op {
	case "PUT":
		switch req.Kind {
		case "inode":
			var v Inode
			if err := json.Unmarshal(req.Value, &v); err != nil {
				return Response{"error", "invalid dedup value: " + err.Error(), nil}
			}
			m.PutInode(req.Key, &v)
			return Response{"ok", "", nil}
		case "blob":
			var v BlobTable
			if err := json.Unmarshal(req.Value, &v); err != nil {
				return Response{"error", "invalid blob value: " + err.Error(), nil}
			}
			m.PutBlob(req.Key, &v)
			return Response{"ok", "", nil}
		default:
			return Response{"error", "unknown kind: " + req.Kind, nil}
		}

	case "GET":
		switch req.Kind {
		case "inode":
			if v, ok := m.GetInode(req.Key); ok {
				return Response{"ok", "", v}
			} else {
				return Response{"ok", "not found", nil}
			}
		case "blob":
			if v, ok := m.GetBlob(req.Key); ok {
				return Response{"ok", "", v}
			} else {
				return Response{"ok", "not found", nil}
			}
		default:
			return Response{"error", "unknown kind: " + req.Kind, nil}
		}

	case "DEL":
		switch req.Kind {
		case "inode":
			m.DelInode(req.Key)
			return Response{"ok", "", nil}
		case "blob":
			m.DelBlob(req.Key)
			return Response{"ok", "", nil}
		default:
			return Response{"error", "unknown kind: " + req.Kind, nil}
		}

	default:
		return Response{"error", "unknown op: " + req.Op, nil}
	}
}

func (m *dedupserve) Serve(stop <-chan struct{}) error {
	os.Remove(m.socketPath)
	ln, err := net.Listen("unix", m.socketPath)
	if err != nil {
		return fmt.Errorf("listen error: %v", err)
	}
	defer os.Remove(m.socketPath)
	defer ln.Close()

	go func() {
		<-stop
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		go m.handleConn(conn)
	}
}
