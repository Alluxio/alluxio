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

import alluxio.client.block.UnderStoreBlockInStream.UnderStoreStreamFactory;
import alluxio.underfs.UnderFileSystem;

import java.io.IOException;
import java.io.InputStream;

/**
 * Factory which creates input streams to a specified path in under storage. The streams are created
 * through the {@link UnderFileSystem} API.
 */
public final class DirectUnderStoreStreamFactory implements UnderStoreStreamFactory {
  private final String mPath;

  /**
   * @param path the path in under storage to read from
   */
  public DirectUnderStoreStreamFactory(String path) {
    mPath = path;
  }

  @Override
  public InputStream create() throws IOException {
    return UnderFileSystem.get(mPath).open(mPath);
  }

  @Override
  public void close() throws IOException {
    // Nothing needs to be closed.
  }
}
