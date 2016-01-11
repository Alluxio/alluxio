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

package tachyon.client.lineage;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.exception.TachyonException;

/**
 * Tachyon lineage file system client. This class provides lineage support in the file system
 * operations.
 */
@PublicApi
public class TachyonLineageFileSystem extends TachyonFileSystem {
  private static TachyonLineageFileSystem sTachyonFileSystem;
  private LineageContext mLineageContext;

  /**
   * @return the current lineage file system for Tachyon
   */
  public static synchronized TachyonLineageFileSystem get() {
    if (sTachyonFileSystem == null) {
      sTachyonFileSystem = new TachyonLineageFileSystem();
    }
    return sTachyonFileSystem;
  }

  protected TachyonLineageFileSystem() {
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
   * @throws TachyonException if an unexpected TachyonException occurs
   */
  private long reinitializeFile(TachyonURI path, OutStreamOptions options)
      throws LineageDoesNotExistException, IOException, TachyonException {
    LineageMasterClient masterClient = mLineageContext.acquireMasterClient();
    try {
      long fileId = masterClient.reinitializeFile(path.getPath(), options.getBlockSizeBytes(),
          options.getTtl());
      return fileId;
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
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   */
  @Override
  public FileOutStream getOutStream(TachyonURI path, OutStreamOptions options)
      throws IOException, TachyonException {
    long fileId;
    try {
      fileId = reinitializeFile(path, options);
    } catch (LineageDoesNotExistException e) {
      // not a lineage file
      return super.getOutStream(path, options);
    }
    if (fileId == -1) {
      return new DummyFileOutputStream(fileId, options);
    }
    return new LineageFileOutStream(fileId, options);
  }

  /**
   * Reports a file as lost.
   *
   * @param path the path to the lost file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the file does not exist
   * @throws TachyonException if a Tachyon exception occurs
   */
  public void reportLostFile(TachyonURI path)
      throws IOException, FileDoesNotExistException, TachyonException {
    LineageMasterClient masterClient = mLineageContext.acquireMasterClient();
    try {
      masterClient.reportLostFile(path.getPath());
    } finally {
      mLineageContext.releaseMasterClient(masterClient);
    }
  }
}
