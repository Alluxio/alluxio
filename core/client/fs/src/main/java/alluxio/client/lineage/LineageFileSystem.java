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

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio lineage file system client. This class provides lineage support in the file system
 * operations.
 */
@PublicApi
@ThreadSafe
public final class LineageFileSystem extends BaseFileSystem {
  private LineageContext mLineageContext;

  /**
   * @param fileSystemContext file system context
   * @param lineageContext lineage context
   * @return the current lineage file system for Alluxio
   */
  public static synchronized LineageFileSystem get(FileSystemContext fileSystemContext,
      LineageContext lineageContext) {
    return new LineageFileSystem(fileSystemContext, lineageContext);
  }

  private LineageFileSystem(FileSystemContext fileSystemContext, LineageContext lineageContext) {
    super(fileSystemContext);
    mLineageContext = lineageContext;
  }

  /**
   * A file is created when its lineage is added. This method reinitializes the created file. But
   * it's no-op if the file is already completed.
   *
   * @param path the path to the file
   * @param options the set of options specific to this operation
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise
   * @throws LineageDoesNotExistException if the lineage does not exist
   */
  private long reinitializeFile(AlluxioURI path, CreateFileOptions options)
      throws LineageDoesNotExistException, IOException, AlluxioException {
    LineageMasterClient masterClient = mLineageContext.acquireMasterClient();
    try {
      return masterClient.reinitializeFile(path.getPath(), options.getBlockSizeBytes(),
          options.getTtl(), options.getTtlAction());
    } catch (NotFoundException e) {
      throw new LineageDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mLineageContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets the output stream for lineage job. If the file already exists on master, returns a dummy
   * output stream.
   *
   * @param path the Alluxio path of the file
   * @param options the set of options specific to this operation
   * @return an output stream to write the file
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
    return new LineageFileOutStream(mFileSystemContext, path, options.toOutStreamOptions());
  }

  /**
   * Reports a file as lost.
   *
   * @param path the path to the lost file
   * @throws FileDoesNotExistException if the file does not exist
   */
  public void reportLostFile(AlluxioURI path)
      throws IOException, FileDoesNotExistException, AlluxioException {
    LineageMasterClient masterClient = mLineageContext.acquireMasterClient();
    try {
      masterClient.reportLostFile(path.getPath());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mLineageContext.releaseMasterClient(masterClient);
    }
  }
}
