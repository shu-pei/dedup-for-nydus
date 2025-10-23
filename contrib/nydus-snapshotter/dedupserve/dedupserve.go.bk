package dedupserve

import (
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/containerd/containerd/log"
	"github.com/pkg/errors"
)

type dedupserve struct {
	root       string
	socketPath string
	data       sync.Map
	db         *sql.DB
}

type Request struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
	Id   string `json:"id"`
	Ino  uint64 `json:"ino"`
}

type QueryResponse struct {
	Id  string `json:"id"`
	Ino uint64 `json:"ino"`
}

type BootstrapResponse struct {
	Bootstrap string `json:"bootstrap"`
}

func NewDedupServer(root string) (*dedupserve, error) {
	dbPath := filepath.Join(root, "dedup.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS dedup_data (
		key TEXT PRIMARY KEY,
		id TEXT NOT NULL,
		ino INTEGER NOT NULL
	)`)
	if err != nil {
		return nil, err
	}

	socketPath := filepath.Join(root, "dedup.sock")
	s := &dedupserve{root: root, socketPath: socketPath, db: db}
	if err := s.ensureSocketNotExists(); err != nil {
		return nil, err
	}

	rows, err := db.Query("SELECT key, id, ino FROM dedup_data")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var k, id string
		var ino uint64
		if err := rows.Scan(&k, &id, &ino); err != nil {
			return nil, err
		}
		s.data.Store(k, QueryResponse{Id: id, Ino: ino})
	}

	return s, nil
}

func (s *dedupserve) Serve(stop <-chan struct{}) error {
	l, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return err
	}
	defer l.Close()
	defer s.db.Close()

	go func() {
		<-stop
		l.Close()
		s.db.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
				return err
			}
		}
		go func() {
			if err := s.handleConn(conn); err != nil {
				log.L.WithError(err).Error("handleConn failed")
			}
		}()
	}
}

func (s *dedupserve) handleConn(conn net.Conn) error {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		var req Request
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.Wrap(err, "decode error")
		}

		switch req.Kind {
		case "query":
			if val, ok := s.data.Load(req.Key); ok {
				_ = encoder.Encode(val)
				continue
			}
			val := QueryResponse{Id: req.Id, Ino: req.Ino}
			s.data.Store(req.Key, val)

			// _, err := s.db.Exec(
			// 	"INSERT OR REPLACE INTO dedup_data (key, id, ino) VALUES (?, ?, ?)",
			// 	req.Key, req.Id, req.Ino,
			// )
			// if err != nil {
			// 	log.L.WithError(err).Error("failed to persist data")
			// }

			_ = encoder.Encode(val)

		case "getbs":
			path := filepath.Join(s.root, "boot", req.Id, "image.boot")
			resp := BootstrapResponse{Bootstrap: path}
			_ = encoder.Encode(resp)

		default:
			return errors.Errorf("unknown request type: %q", req.Kind)
		}
	}
}

func (s *dedupserve) ensureSocketNotExists() error {
	if err := os.MkdirAll(s.root, 0700); err != nil {
		return errors.Wrapf(err, "failed to create directory %q", s.root)
	}
	_, err := os.Stat(s.socketPath)
	// err is nil means listening socket path exists, remove before serve
	if err == nil {
		err := os.Remove(s.socketPath)
		if err != nil {
			return err
		}
	}
	return nil
}
