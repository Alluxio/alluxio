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

package alluxio.job.plan.transform.format;

import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link Path} that has a cache for getting file system.
 */
public class JobPath extends Path {
  private static final long serialVersionUID = 1427341926575998813L;
  private static final ConcurrentHashMap<FileSystemKey, FileSystem> CACHE =
      new ConcurrentHashMap<>();

  /**
   * Calls {@link FileSystem}.get, created to make testing easier.
   * @param uri the uri
   * @param conf the conf
   * @return the file system
   * @throws IOException
   */
  @VisibleForTesting
  public static FileSystem fileSystemGet(URI uri, Configuration conf) throws IOException {
    return FileSystem.get(uri, conf);
  }

  private static class FileSystemKey {

    final String mScheme;
    final String mAuthority;
    final UserGroupInformation mUgi;
    final ReadType mReadType;
    final WriteType mWriteType;

    public FileSystemKey(JobPath path, Configuration conf) throws IOException {
      URI uri = path.toUri();
      mScheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
      mAuthority = uri.getAuthority() == null ? "" : uri.getAuthority().toLowerCase();
      mUgi = UserGroupInformation.getCurrentUser();
      mReadType = conf.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT.getName(),
          InstancedConfiguration.defaults().getEnum(
              PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class));
      mWriteType = conf.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT.getName(),
          InstancedConfiguration.defaults().getEnum(
              PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mScheme, mAuthority, mUgi, mReadType, mWriteType);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileSystemKey)) {
        return false;
      }

      FileSystemKey that = (FileSystemKey) o;
      return Objects.equal(mScheme, that.mScheme)
          && Objects.equal(mAuthority, that.mAuthority)
          && Objects.equal(mUgi, that.mUgi)
          && Objects.equal(mReadType, that.mReadType)
          && Objects.equal(mWriteType, that.mWriteType);
    }
  }

  /**
   * Copy of the constructor in {@link Path}.
   * @param scheme the scheme
   * @param authority the authority
   * @param path the path
   */
  public JobPath(String scheme, String authority, String path) {
    super(scheme, authority, path);
  }

  @Override
  public FileSystem getFileSystem(Configuration conf) throws IOException {
    try {
      return CACHE.computeIfAbsent(new FileSystemKey(this, conf), (key) -> {
        try {
          return fileSystemGet(this.toUri(), conf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw e;
    }
  }
}
