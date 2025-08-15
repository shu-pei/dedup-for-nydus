/*
 * Copyright (c) 2020. Ant Group. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package snapshotter

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/dragonflyoss/image-service/contrib/nydus-snapshotter/config"
	"github.com/dragonflyoss/image-service/contrib/nydus-snapshotter/pkg/utils/signals"
	"github.com/dragonflyoss/image-service/contrib/nydus-snapshotter/snapshot"
)

func Start(ctx context.Context, cfg config.Config) error {
	rs, err := snapshot.NewSnapshotter(ctx, &cfg)
	if err != nil {
		return errors.Wrap(err, "failed to initialize snapshotter")
	}

	stopSignal := signals.SetupSignalHandler()
	opt := ServeOptions{
		ListeningSocketPath: cfg.Address,
	}

	go func() {
		if err := startServer("/tmp/nydusmap.sock", stopSignal); err != nil {
			fmt.Println("Server stopped with error:", err)
		}
	}()

	return Serve(ctx, rs, opt, stopSignal)
}
