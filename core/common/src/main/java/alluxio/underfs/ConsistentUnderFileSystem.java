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

import alluxio.AlluxioURI;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.OpenOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Represents a consistent under filesystem
 * which does not have eventual consistency issues.
 */
public abstract class ConsistentUnderFileSystem extends BaseUnderFileSystem {

  /**
   * Creates a new {@link ConsistentUnderFileSystem} for the given uri.
   *
   * @param uri path belonging to this under file system
   * @param ufsConf UFS configuration
   */
  public ConsistentUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
  }

  @Override
  public OutputStream createWithRetry(String path) throws IOException {
    return create(path);
  }

  @Override
  public OutputStream createWithRetry(String path, CreateOptions options) throws IOException {
    return create(path, options);
  }

  @Override
  public boolean deleteDirectoryWithRetry(String path) throws IOException {
    return deleteDirectory(path);
  }

  @Override
  public boolean deleteDirectoryWithRetry(String path, DeleteOptions options) throws IOException {
    return deleteDirectory(path, options);
  }

  @Override
  public boolean deleteFileWithRetry(String path) throws IOException {
    return deleteFile(path);
  }

  @Override
  public  UfsDirectoryStatus getDirectoryStatusWithRetry(String path) throws IOException {
    return getDirectoryStatus(path);
  }

  @Override
  public UfsFileStatus getFileStatusWithRetry(String path) throws IOException {
    return getFileStatus(path);
  }

  @Override
  public UfsStatus getStatusWithRetry(String path) throws IOException {
    return getStatus(path);
  }

  @Override
  public boolean isDirectoryWithRetry(String path) throws IOException {
    return isDirectory(path);
  }

  @Override
  public InputStream openWithRetry(String path) throws IOException {
    return open(path);
  }

  @Override
  public InputStream openWithRetry(String path, OpenOptions options) throws IOException {
    return open(path, options);
  }

  @Override
  public boolean renameDirectoryWithRetry(String src, String dst) throws IOException {
    return renameDirectory(src, dst);
  }

  @Override
  public boolean renameFileWithRetry(String src, String dst) throws IOException {
    return renameFile(src, dst);
  }
}
