/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.metastore.rocks;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.Checkpointed;

import io.grpc.Status;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Provides default implementations for checkpointing RocksDB databases.
 */
public interface RocksCheckpointed extends Checkpointed {
  /**
   * @return the {@link RocksStore} that will produce a checkpoint
   */
  RocksStore getRocksStore();

  @Override
  default CompletableFuture<Void> writeToCheckpoint(File directory,
                                                   ExecutorService executorService) {
    return CompletableFuture.runAsync(() -> {
      LOG.debug("taking {} snapshot started", getCheckpointName());
      try (RocksExclusiveLockHandle lock = getRocksStore().lockForCheckpoint()) {
        File subDir = new File(directory, getCheckpointName().toString());
        try {
          getRocksStore().writeToCheckpoint(subDir);
        } catch (RocksDBException e) {
          throw new AlluxioRuntimeException(Status.INTERNAL,
              String.format("Failed to take snapshot %s in dir %s", getCheckpointName(), directory),
              e, ErrorType.Internal, false);
        }
        LOG.debug("taking {} snapshot finished", getCheckpointName());
      }
    }, executorService);
  }

  @Override
  default void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    try (RocksExclusiveLockHandle lock = getRocksStore().lockForCheckpoint()) {
      getRocksStore().writeToCheckpoint(output);
    }
  }

  @Override
  default CompletableFuture<Void> restoreFromCheckpoint(File directory,
                                                       ExecutorService executorService) {
    return CompletableFuture.runAsync(() -> {
      LOG.debug("loading {} snapshot started", getCheckpointName());
      File subDir = new File(directory, getCheckpointName().toString());
      try (RocksExclusiveLockHandle lock = getRocksStore().lockForRewrite()) {
        getRocksStore().restoreFromCheckpoint(subDir);
      } catch (Exception e) {
        throw new AlluxioRuntimeException(Status.INTERNAL,
            String.format("Failed to restore snapshot %s", getCheckpointName()),
            e, ErrorType.Internal, false);
      }
      LOG.debug("loading {} snapshot finished", getCheckpointName());
    }, executorService);
  }

  @Override
  default void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    try (RocksExclusiveLockHandle lock = getRocksStore().lockForRewrite()) {
      getRocksStore().restoreFromCheckpoint(input);
    }
  }
}
