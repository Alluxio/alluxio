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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.block.UnderStoreBlockInStream.UnderStoreStreamFactory;
import alluxio.client.file.options.CloseUfsFileOptions;
import alluxio.client.file.options.OpenUfsFileOptions;
import alluxio.client.UnderFileSystemFileReader;
import alluxio.exception.AlluxioException;
import alluxio.underfs.UnderFileInputStream;
import alluxio.underfs.options.OpenOptions;

import java.io.IOException;

/**
 * Factory which creates input streams to a specified path in under storage. The streams are created
 * through the file system worker API.
 */
public final class DelegatedUnderStoreStreamFactory implements UnderStoreStreamFactory {
  private final FileSystemWorkerClient mClient;
  private final long mFileId;

  /**
   * @param context file system context
   * @param path the ufs path to create streams to
   * @throws IOException if an IO exception occurs
   */
  public DelegatedUnderStoreStreamFactory(FileSystemContext context, String path)
      throws IOException {
    mClient = context.createWorkerClient();
    try {
      mFileId = mClient.openUfsFile(new AlluxioURI(path), OpenUfsFileOptions.defaults());
    } catch (AlluxioException | IOException e) {
      mClient.close();
      throw new IOException(e);
    }
  }

  @Override
  public UnderFileInputStream create(OpenOptions options) {
    return new UnderFileSystemFileInStream(mClient.getWorkerDataServerAddress(),
        options.getOffset(), mFileId, UnderFileSystemFileReader.Factory.create());
  }

  @Override
  public void close() throws IOException {
    try {
      mClient.closeUfsFile(mFileId, CloseUfsFileOptions.defaults());
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mClient.close();
    }
  }
}
