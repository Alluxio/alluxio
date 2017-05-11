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

package alluxio.underfs;

import alluxio.underfs.options.MkdirsOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Extend {@link UnderFileSystemWithLogging} with logging directory operations.
 */
public class DirectoryUnderFileSystemWithLogging extends UnderFileSystemWithLogging
    implements DirectoryUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemWithLogging.class);

  private final DirectoryUnderFileSystem mUnderFileSystem;
  /**
   * Creates a new {@link DirectoryUnderFileSystemWithLogging} which forwards all calls to the
   * provided {@link DirectoryUnderFileSystem} implementation.
   *
   * @param ufs the implementation which will handle all the calls
   */
  DirectoryUnderFileSystemWithLogging(DirectoryUnderFileSystem ufs) {
    super(ufs);
    mUnderFileSystem = ufs;
  }

  @Override
  public boolean exists(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.exists(path);
      }

      @Override
      public String toString() {
        return String.format("Exists: path=%s", path);
      }
    });
  }

  @Override
  public boolean isDirectory(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.isDirectory(path);
      }

      @Override
      public String toString() {
        return String.format("IsDirectory: path=%s", path);
      }
    });
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.mkdirs(path);
      }

      @Override
      public String toString() {
        return String.format("Mkdirs: path=%s", path);
      }
    });
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.mkdirs(path, options);
      }

      @Override
      public String toString() {
        return String.format("Mkdirs: path=%s, options=%s", path, options);
      }
    });
  }
}
