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

package alluxio.client.fuse.dora.readonly;

import alluxio.client.file.options.FileSystemOptions;
import alluxio.conf.Configuration;
import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.AlluxioJniFuseFileSystem;
import alluxio.fuse.options.FuseOptions;
import alluxio.jnifuse.struct.FileStat;
import alluxio.util.io.BufferUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public abstract class AbstractFuseFileSystemTest extends AbstractFuseDoraReadOnlyTest {
  protected AlluxioJniFuseFileSystem mFuseFs;
  protected AlluxioFuseUtils.CloseableFuseFileInfo mFileInfo;
  protected FileStat mFileStat;

  @Override
  public void beforeActions() {
    mFuseFs = new AlluxioJniFuseFileSystem(mContext, mFileSystem,
        FuseOptions.create(Configuration.global(), FileSystemOptions.create(
            mContext.getClusterConf(), Optional.of(mUfsOptions)), false));
    mFileStat = FileStat.of(ByteBuffer.allocateDirect(256));
    mFileInfo = new AlluxioFuseUtils.CloseableFuseFileInfo();
  }

  @Override
  public void afterActions() throws IOException {
    BufferUtils.cleanDirectBuffer(mFileStat.getBuffer());
    mFileInfo.close();
  }

  protected void createEmptyFile(String path) throws IOException {
    new FileOutputStream(path).close();
  }

  protected void createFile(String path, int size) throws IOException {
    try (FileOutputStream stream = new FileOutputStream(path)) {
      stream.write(BufferUtils.getIncreasingByteArray(size));
    }
  }
}
