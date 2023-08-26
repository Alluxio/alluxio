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

package alluxio.fuse;

import static jnr.constants.platform.OpenFlags.O_ACCMODE;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.fuse.file.FuseFileInOrOutStream;
import alluxio.fuse.file.FuseFileInStream;
import alluxio.fuse.file.FuseFileOutStream;
import alluxio.fuse.file.FuseFileStream;
import alluxio.fuse.file.FusePositionReader;
import alluxio.fuse.lock.FuseReadWriteLockManager;

import jnr.constants.platform.OpenFlags;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for {@link FuseFileInStream}.
 */
@ThreadSafe
public class RandomAccessFuseStreamFactory {
  private final FuseReadWriteLockManager mLockManager = new FuseReadWriteLockManager();
  private final FileSystem mFileSystem;
  private final AuthPolicy mAuthPolicy;
  // TODO(lu) allow different threads reading from same file to share the same position reader
  private final boolean mPositionReadEnabled
      = Configuration.getBoolean(PropertyKey.FUSE_POSITION_READ_ENABLED);

  public RandomAccessFuseStreamFactory(FileSystem fileSystem, AuthPolicy authPolicy) {
    mFileSystem = fileSystem;
    mAuthPolicy = authPolicy;
  }

  public FuseFileStream create(
      AlluxioURI uri, int flags, long mode) {
    boolean exists;
    try {
      exists = mFileSystem.exists(uri);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    switch (OpenFlags.valueOf(flags & O_ACCMODE.intValue())) {
      case O_RDONLY:
        if (mPositionReadEnabled) {
          return FusePositionReader.create(mFileSystem, mLockManager, uri);
        }
        return FuseFileInStream.create(mFileSystem, mLockManager, uri);
      case O_WRONLY:
        if (!exists) {
          FuseFileOutStream fuseFileOutStream = FuseFileOutStream.create(mFileSystem, mAuthPolicy,
              mLockManager, uri, flags, mode);
          fuseFileOutStream.close();
        }
        return RandomAccessFuseFileStream.create(mFileSystem, mAuthPolicy, mLockManager, uri,
            flags, mode);
      default:
        if (!exists) {
          FuseFileInOrOutStream fuseFileInOrOutStream = FuseFileInOrOutStream.create(mFileSystem,
              mAuthPolicy, mLockManager, uri, flags, mode);
          fuseFileInOrOutStream.close();
        }
        return RandomAccessFuseFileStream.create(mFileSystem, mAuthPolicy, mLockManager, uri,
            flags, mode);
    }
  }
}
