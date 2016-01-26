/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon;

import tachyon.util.CommonUtils;
import tachyon.worker.TachyonWorkerPrivateAccess;
import tachyon.worker.file.FileDataManager;
import tachyon.worker.file.FileSystemWorker;
import tachyon.worker.file.FileSystemWorkerPrivateAccess;

/**
 * Util methods for writing integration tests.
 */
public final class IntegrationTestUtils {

  /**
   * Convenience method for calling
   * {@link #waitForPersist(LocalTachyonClusterResource, long, long)} with a timeout of 5
   * seconds.
   *
   * @param localTachyonClusterResource the cluster for the worker that will persist the file
   * @param fileId the file id to wait to be persisted
   */
  public static void waitForPersist(LocalTachyonClusterResource localTachyonClusterResource,
      long fileId) {
    waitForPersist(localTachyonClusterResource, fileId, 5 * Constants.SECOND_MS);
  }

  /**
   * Blocks until the specified file is persisted or a timeout occurs.
   *
   * @param localTachyonClusterResource the cluster for the worker that will persist the file
   * @param fileId the file id to wait to be persisted
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public static void waitForPersist(LocalTachyonClusterResource localTachyonClusterResource,
      long fileId, int timeoutMs) {
    long start = System.currentTimeMillis();
    FileSystemWorker worker = TachyonWorkerPrivateAccess
        .getFileSystemWorker(localTachyonClusterResource.get().getWorker());
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
