/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import alluxio.util.CommonUtils;
import alluxio.worker.PrivateAccess;
import alluxio.worker.file.FileDataManager;
import alluxio.worker.file.FileSystemWorker;
import alluxio.worker.file.FileSystemWorkerPrivateAccess;

/**
 * Util methods for writing integration tests.
 */
public final class IntegrationTestUtils {

  /**
   * Convenience method for calling
   * {@link #waitForPersist(LocalAlluxioClusterResource, long, long)} with a timeout of 5
   * seconds.
   *
   * @param localAlluxioClusterResource the cluster for the worker that will persist the file
   * @param fileId the file id to wait to be persisted
   */
  public static void waitForPersist(LocalAlluxioClusterResource localAlluxioClusterResource,
                                    long fileId) {
    waitForPersist(localAlluxioClusterResource, fileId, 5 * Constants.SECOND_MS);
  }

  /**
   * Blocks until the specified file is persisted or a timeout occurs.
   *
   * @param localAlluxioClusterResource the cluster for the worker that will persist the file
   * @param fileId the file id to wait to be persisted
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public static void waitForPersist(LocalAlluxioClusterResource localAlluxioClusterResource,
                                    long fileId, int timeoutMs) {
    long start = System.currentTimeMillis();
    FileSystemWorker worker = PrivateAccess
        .getFileSystemWorker(localAlluxioClusterResource.get().getWorker());
    FileDataManager fileDataManager = FileSystemWorkerPrivateAccess.getFileDataManager(worker);
    while (!fileDataManager.isFilePersisted(fileId)) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        throw new RuntimeException("Timed out waiting for " + fileId + " to be persisted");
      }
      CommonUtils.sleepMs(20);
    }
  }

  private IntegrationTestUtils() {} // This is a utils class not intended for instantiation
}
