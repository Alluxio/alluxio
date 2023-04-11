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

package alluxio.master.journal.checkpoint;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;

import io.grpc.Status;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Base class for Alluxio classes which can be written to and read from metadata checkpoints.
 */
public interface Checkpointed {
  Logger LOG = LoggerFactory.getLogger(Checkpointed.class);
  /**
   * @return a name for this checkpointed class
   */
  CheckpointName getCheckpointName();

  /**
   * Writes a checkpoint to the specified directory asynchronously using the provided executor.
   *
   * @param directory       where the checkpoint will be written
   * @param executorService to use when running tasks asynchronously
   * @return a future that processes the computation
   */
  default CompletableFuture<Void> writeToCheckpoint(File directory,
                                                    ExecutorService executorService) {
    return CompletableFuture.runAsync(() -> {
      LOG.debug("taking {} snapshot started", getCheckpointName());
      File file = new File(directory, getCheckpointName().toString());
      try {
        MessageDigest md5 = MD5Hash.getDigester();
        try (OutputStream outputStream = new OptimizedCheckpointOutputStream(file, md5)) {
          writeToCheckpoint(outputStream);
        }
        MD5FileUtil.saveMD5File(file, new MD5Hash(md5.digest()));
      } catch (Exception e) {
        throw new AlluxioRuntimeException(Status.INTERNAL,
            String.format("Failed to take snapshot %s", getCheckpointName()),
            e, ErrorType.Internal, false);
      }
      LOG.debug("taking {} snapshot finished", getCheckpointName());
    }, executorService);
  }

  /**
   * Writes a checkpoint of all state to the given output stream.
   *
   * Implementations should make an effort to throw {@link InterruptedException} if they get
   * interrupted while running.
   *
   * @param output the output stream to write to
   */
  void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException;

  /**
   * Restores state from a checkpoint asynchronously.
   * @param directory where the checkpoint will be located
   * @param executorService to use when running asynchronous tasks
   * @return a future to track the progress
   */
  default CompletableFuture<Void> restoreFromCheckpoint(File directory,
                                                        ExecutorService executorService) {
    return CompletableFuture.runAsync(() -> {
      LOG.debug("loading {} snapshot started", getCheckpointName());
      File file = new File(directory, getCheckpointName().toString());
      try {
        MessageDigest md5 = MD5Hash.getDigester();
        try (CheckpointInputStream is = new OptimizedCheckpointInputStream(file, md5)) {
          restoreFromCheckpoint(is);
        }
        MD5FileUtil.verifySavedMD5(file, new MD5Hash(md5.digest()));
      } catch (Exception e) {
        throw new AlluxioRuntimeException(Status.INTERNAL,
            String.format("Failed to restore snapshot %s", getCheckpointName()),
            e, ErrorType.Internal, false);
      }
      LOG.debug("loading {} snapshot finished", getCheckpointName());
    }, executorService);
  }

  /**
   * Restores state from a checkpoint.
   *
   * @param input an input stream with checkpoint data
   */
  void restoreFromCheckpoint(CheckpointInputStream input) throws IOException;
}
