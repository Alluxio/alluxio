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

package alluxio.worker.task;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.grpc.DeletePOptions;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DeleteHandler is responsible for deleting files.
 */
public final class DeleteHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteHandler.class);
  private static final int RETRY_TIMES = 5;

  /**
   * This function deletes files/directories.
   * This is a temporary solution for PDDM from S3 storage to S3 storage.
   * There might be some issues with HDFS when it comes to removing directories.
   *
   * @param path     the path
   * @param fs       the source file system
   */
  public static void delete(AlluxioURI path, FileSystem fs) {
    RetryUtils.retryCallable(String.format("Starting delete file {}", path), () -> {
      DeletePOptions.Builder builder = DeletePOptions.newBuilder().setRecursive(false);
      fs.delete(path, builder.build());
      return null;
    }, new ExponentialBackoffRetry(100, 500, RETRY_TIMES));
  }
}
