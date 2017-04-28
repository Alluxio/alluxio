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

package alluxio.master.file.async;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.thrift.PersistFile;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The handler that handles the scheduled async persist requests on files, and decides which worker
 * to take the persistence task. The handler carries the strategy of when and where to persist the
 * file.
 */
public interface AsyncPersistHandler {

  /**
   * Factory for {@link AsyncPersistHandler}.
   */
  @ThreadSafe
  class Factory {
    public static final Logger LOG = LoggerFactory.getLogger(AsyncPersistHandler.Factory.class);

    private Factory() {} // prevent instantiation

    /**
     * Creates a new instance of {@link AsyncPersistHandler}.
     *
     * @param view {@link FileSystemMasterView} to pass to {@link AsyncPersistHandler}
     * @return the generated {@link AsyncPersistHandler}
     */
    public static AsyncPersistHandler create(FileSystemMasterView view) {
      try {
        return CommonUtils.createNewClassInstance(Configuration.<AsyncPersistHandler>getClass(
            PropertyKey.MASTER_FILE_ASYNC_PERSIST_HANDLER),
            new Class[] {FileSystemMasterView.class},
            new Object[] {view});
      } catch (Exception e) {
        LOG.error("Failed to instantiate the async handler of class "
            + PropertyKey.MASTER_FILE_ASYNC_PERSIST_HANDLER + ". Use the default handler instead");
        return new DefaultAsyncPersistHandler(view);
      }
    }
  }

  /**
   * Schedules a file for async persistence.
   *
   * @param path the path to the file
   */
  void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException;

  /**
   * Polls the files for persistence on the given worker.
   *
   * @param workerId the id of the worker for persistence
   * @return the list of files for persistence
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   * @throws AccessControlException if permission checking fails
   */
  List<PersistFile> pollFilesToPersist(long workerId)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;
}
