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

  private static final String ALLUXIO_HADOOP_FILESYSTEM_DISABLE_CACHE =
      "fs.alluxio.impl.disable.cache";

  private static final ConcurrentHashMap<FileSystemKey, FileSystem> CACHE =
      new ConcurrentHashMap<>();

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
    // Avoid caching at lower levels both because the key at this level has more properties and
    // this cache wants to close the file systems that don't make it into this cache.
    conf.setBoolean(ALLUXIO_HADOOP_FILESYSTEM_DISABLE_CACHE, true);

    FileSystemKey key = new FileSystemKey(this, conf);
    FileSystem fileSystem = CACHE.get(key);
    if (fileSystem != null) {
      return fileSystem;
    }

    fileSystem = super.getFileSystem(conf);

    synchronized (CACHE) {
      FileSystem oldFileSystem = CACHE.get(key);

      if (oldFileSystem != null) {
        fileSystem.close();
        return oldFileSystem;
      }

      CACHE.put(key, fileSystem);
      return fileSystem;
    }
  }
}
