/*
 * Copyright (c) 2020. Ant Group. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package snapshotter

import (
	"context"

	"github.com/containerd/containerd/log"
	"github.com/pkg/errors"

	"github.com/dragonflyoss/image-service/contrib/nydus-snapshotter/config"
	"github.com/dragonflyoss/image-service/contrib/nydus-snapshotter/dedupserve"
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

	ds, err := dedupserve.NewDedupServer(cfg.RootDir)
	if err != nil {
		return errors.Wrap(err, "failed to initialize dedupserve")
	}
	go func() {
		if err := ds.Serve(stopSignal); err != nil {
			log.L.WithError(err).Error("dedup server stopped with error")
		}
	}()

	return Serve(ctx, rs, opt, stopSignal)
}
