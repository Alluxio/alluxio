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

package alluxio.testutils.underfs.sleeping;

import alluxio.AlluxioURI;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * An under file system for testing that sleeps a predefined amount of time before executing an
 * operation. The operation behavior will be equivalent to that of {@link LocalUnderFileSystem}.
 */
public class SleepingUnderFileSystem extends LocalUnderFileSystem {
  private static final String SLEEP_SCHEME = "sleep";
  private final SleepingUnderFileSystemOptions mOptions;

  /**
   * Creates a new {@link SleepingUnderFileSystem} for the given uri.
   *
   * @param uri path belonging to this under file system
   * @param ufsConf UFS configuration
   */
  public SleepingUnderFileSystem(AlluxioURI uri, SleepingUnderFileSystemOptions options,
      UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
    mOptions = options;
  }

  @Override
  public void cleanup() throws IOException {
    sleepIfNecessary(mOptions.getCleanupMs());
    super.cleanup();
  }

  @Override
  public void close() throws IOException {
    sleepIfNecessary(mOptions.getCloseMs());
    super.close();
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    sleepIfNecessary(mOptions.getConnectFromMasterMs());
    super.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    sleepIfNecessary(mOptions.getConnectFromWorkerMs());
    super.connectFromWorker(hostname);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    sleepIfNecessary(mOptions.getCreateMs());
    return super.create(cleanPath(path), options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    sleepIfNecessary(mOptions.getCreateMs());
    return super.createDirect(cleanPath(path), options);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    sleepIfNecessary(mOptions.getDeleteDirectoryMs());
    return super.deleteDirectory(cleanPath(path), options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    sleepIfNecessary(mOptions.getDeleteFileMs());
    return super.deleteFile(cleanPath(path));
  }

  @Override
  public boolean exists(String path) throws IOException {
    sleepIfNecessary(mOptions.getExistsMs());
    return super.exists(cleanPath(path));
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetBlockSizeByteMs());
    return super.getBlockSizeByte(cleanPath(path));
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetDirectoryStatusMs());
    return super.getDirectoryStatus(cleanPath(path));
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetFileLocationsMs());
    return super.getFileLocations(cleanPath(path));
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    sleepIfNecessary(mOptions.getGetFileLocationsMs());
    return super.getFileLocations(cleanPath(path), options);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetStatusMs());
    return super.getStatus(cleanPath(path));
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetFileStatusMs());
    return super.getFileStatus(cleanPath(path));
  }

  @Override
  public String getFingerprint(String path) {
    sleepIfNecessary(mOptions.getGetFingerprintMs());
    return super.getFingerprint(cleanPath(path));
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    sleepIfNecessary(mOptions.getGetSpaceMs());
    return super.getSpace(cleanPath(path), type);
  }

  @Override
  public String getUnderFSType() {
    sleepIfNecessary(mOptions.getGetUnderFSTypeMs());
    return super.getUnderFSType();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    sleepIfNecessary(mOptions.getIsDirectoryMs());
    return super.isDirectory(cleanPath(path));
  }

  @Override
  public boolean isFile(String path) throws IOException {
    sleepIfNecessary(mOptions.getIsFileMs());
    return super.isFile(cleanPath(path));
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    sleepIfNecessary(mOptions.getListStatusMs());
    return super.listStatus(cleanPath(path));
  }

  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    sleepIfNecessary(mOptions.getListStatusWithOptionsMs());
    return super.listStatus(cleanPath(path), options);
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    sleepIfNecessary(mOptions.getMkdirsMs());
    return super.mkdirs(cleanPath(path), options);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    sleepIfNecessary(mOptions.getOpenMs());
    return super.open(cleanPath(path), options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    sleepIfNecessary(mOptions.getRenameDirectoryMs());
    return super.renameDirectory(cleanPath(src), cleanPath(dst));
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    if (!PathUtils.isTemporaryFileName(src)) {
      sleepIfNecessary(mOptions.getRenameFileMs());
    } else {
      sleepIfNecessary(mOptions.getRenameTemporaryFileMs());
    }
    return super.renameFile(cleanPath(src), cleanPath(dst));
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    sleepIfNecessary(mOptions.getSetOwnerMs());
    super.setOwner(cleanPath(path), owner, group);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    sleepIfNecessary(mOptions.getSetModeMs());
    super.setMode(cleanPath(path), mode);
  }

  @Override
  public boolean supportsFlush() throws IOException {
    sleepIfNecessary(mOptions.getSupportsFlushMs());
    return super.supportsFlush();
  }

  /**
   * Removes the sleep scheme from the path if it exists.
   *
   * @param path the path to clean
   * @return a path without the sleep scheme
   */
  private String cleanPath(String path) {
    AlluxioURI uri = new AlluxioURI(path);
    if (SLEEP_SCHEME.equals(uri.getScheme())) {
      return uri.getPath();
    }
    return path;
  }

  /**
   * Waits for the specified duration if the duration is non-negative. A duration of 0 is
   * equivalent to calling Thread.sleep(0).
   *
   * @param duration time to sleep, negative if the thread should not sleep
   */
  private void sleepIfNecessary(long duration) {
    if (duration >= 0) {
      CommonUtils.sleepMs(duration);
    }
  }
}
