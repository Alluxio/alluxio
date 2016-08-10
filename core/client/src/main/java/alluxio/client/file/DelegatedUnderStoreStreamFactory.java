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
import alluxio.client.netty.NettyUnderFileSystemFileReader;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.io.InputStream;

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
    mClient = FileSystemContext.INSTANCE.createWorkerClient();
    try {
      mFileId = mClient.openUfsFile(new AlluxioURI(path), OpenUfsFileOptions.defaults());
    } catch (AlluxioException | IOException e) {
      mClient.close();
      throw new IOException(e);
    }
  }

  @Override
  public InputStream create() {
    return new UnderFileSystemFileInStream(mClient.getWorkerDataServerAddress(), mFileId,
        new NettyUnderFileSystemFileReader());
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
