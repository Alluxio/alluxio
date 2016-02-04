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

package alluxio.client.lineage;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.exception.AlluxioException;

/**
 * Tachyon lineage file system client. This class provides lineage support in the file system
 * operations.
 */
@PublicApi
@ThreadSafe
public class LineageFileSystem extends BaseFileSystem {
  private LineageContext mLineageContext;

  /**
   * @return the current lineage file system for Tachyon
   */
  public static synchronized LineageFileSystem get() {
    return new LineageFileSystem();
  }

  protected LineageFileSystem() {
    super();
    mLineageContext = LineageContext.INSTANCE;
  }

  /**
   * A file is created when its lineage is added. This method reinitializes the created file. But
   * it's no-op if the file is already completed.
   *
   * @param path the path to the file
   * @param options the set of options specific to this operation
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise
   * @throws LineageDoesNotExistException if the lineage does not exist
   * @throws IOException if the recreation fails
   * @throws AlluxioException if an unexpected TachyonException occurs
   */
  private long reinitializeFile(AlluxioURI path, CreateFileOptions options)
      throws LineageDoesNotExistException, IOException, AlluxioException {
    LineageMasterClient masterClient = mLineageContext.acquireMasterClient();
    try {
      return masterClient.reinitializeFile(path.getPath(), options.getBlockSizeBytes(),
          options.getTtl());
    } finally {
      mLineageContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets the output stream for lineage job. If the file already exists on master, returns a dummy
   * output stream.
   *
   * @param path the Tachyon path of the file
   * @param options the set of options specific to this operation
   * @return an output stream to write the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws AlluxioException if an unexpected Tachyon exception is thrown
   */
  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFileOptions options)
      throws IOException, AlluxioException {
    long fileId;
    try {
      fileId = reinitializeFile(path, options);
    } catch (LineageDoesNotExistException e) {
      // not a lineage file
      return super.createFile(path, options);
    }
    if (fileId == -1) {
      return new DummyFileOutputStream(path, options.toOutStreamOptions());
    }
    return new LineageFileOutStream(path, options.toOutStreamOptions());
  }

  /**
   * Reports a file as lost.
   *
   * @param path the path to the lost file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AlluxioException if a Tachyon exception occurs
   */
  public void reportLostFile(AlluxioURI path)
      throws IOException, FileDoesNotExistException, AlluxioException {
    LineageMasterClient masterClient = mLineageContext.acquireMasterClient();
    try {
      masterClient.reportLostFile(path.getPath());
    } finally {
      mLineageContext.releaseMasterClient(masterClient);
    }
  }
}
