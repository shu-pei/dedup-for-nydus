/*
 * Copyright (c) 2020. Ant Group. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package snapshotter

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/snapshots"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type ServeOptions struct {
	ListeningSocketPath string
}

func Serve(ctx context.Context, rs snapshots.Snapshotter, options ServeOptions, stop <-chan struct{}) error {
	err := ensureSocketNotExists(options.ListeningSocketPath)
	if err != nil {
		return err
	}

	rpc := grpc.NewServer()
	snapshotsapi.RegisterSnapshotsServer(rpc, snapshotservice.FromSnapshotter(rs))
	l, err := net.Listen("unix", options.ListeningSocketPath)
	if err != nil {
		return errors.Wrapf(err, "error on listen socket %q", options.ListeningSocketPath)
	}
	go func() {
		sig := <-stop
		log.G(ctx).Infof("caught signal %s: shutting down", sig)
		err := l.Close()
		if err != nil {
			log.G(ctx).Errorf("failed to close listener %s, err: %v", options.ListeningSocketPath, err)
		}
	}()

	return rpc.Serve(l)
}

func ensureSocketNotExists(listeningSocketPath string) error {
	if err := os.MkdirAll(filepath.Dir(listeningSocketPath), 0700); err != nil {
		return errors.Wrapf(err, "failed to create directory %q", filepath.Dir(listeningSocketPath))
	}
	_, err := os.Stat(listeningSocketPath)
	// err is nil means listening socket path exists, remove before serve
	if err == nil {
		err := os.Remove(listeningSocketPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func startServer(socketPath string, stop <-chan struct{}) error {
	os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on unix socket: %w", err)
	}

	if err := os.Chmod(socketPath, 0666); err != nil {
		return fmt.Errorf("failed to chmod socket: %w", err)
	}

	go func() {
		<-stop
		fmt.Println("Shutting down server...")
		_ = listener.Close()
	}()

	data := make(map[string][2]uint64)
	var mu sync.RWMutex

	var counter uint64
	var counterMu sync.Mutex

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
			}
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			scanner := bufio.NewScanner(c)
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.SplitN(line, " ", 4)
				if len(parts) == 0 {
					c.Write([]byte("ERR empty command\n"))
					continue
				}
				cmd := strings.ToUpper(parts[0])

				switch cmd {
				case "GET":
					if len(parts) < 2 {
						c.Write([]byte("ERR missing key\n"))
						continue
					}
					key := parts[1]
					mu.RLock()
					val, ok := data[key]
					mu.RUnlock()
					if ok {
						c.Write([]byte(fmt.Sprintf("OK %d %d\n", val[0], val[1])))
					} else {
						c.Write([]byte("NOTFOUND\n"))
					}

				case "PUT":
					if len(parts) < 4 {
						c.Write([]byte("ERR missing key or values\n"))
						continue
					}
					key := parts[1]
					v1, err1 := strconv.ParseUint(parts[2], 10, 64)
					v2, err2 := strconv.ParseUint(parts[3], 10, 64)
					if err1 != nil || err2 != nil {
						c.Write([]byte("ERR invalid integer\n"))
						continue
					}
					mu.Lock()
					data[key] = [2]uint64{v1, v2}
					mu.Unlock()
					c.Write([]byte("OK\n"))

				case "GET_ID":
					counterMu.Lock()
					counter++
					id := counter
					counterMu.Unlock()
					c.Write([]byte(fmt.Sprintf("OK %d\n", id)))

				default:
					c.Write([]byte("ERR unknown command\n"))
				}
			}
		}(conn)
	}
}
