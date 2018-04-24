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

package alluxio.underfs.delegating;

import alluxio.AlluxioURI;
import alluxio.security.authorization.AccessControlList;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * UFS which delegates to another UFS. Extend this class to override method behavior.
 */
public class DelegatingUnderFileSystem implements UnderFileSystem {
  protected final UnderFileSystem mUfs;

  /**
   * @param ufs the underfilesystem to delegate to
   */
  public DelegatingUnderFileSystem(UnderFileSystem ufs) {
    mUfs = ufs;
  }

  @Override
  public void close() throws IOException {
    mUfs.close();
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    mUfs.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    mUfs.connectFromWorker(hostname);
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return mUfs.create(path);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return mUfs.create(path, options);
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    return mUfs.deleteDirectory(path);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return mUfs.deleteDirectory(path, options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return mUfs.deleteFile(path);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mUfs.exists(path);
  }

  @Override
  public AccessControlList getAcl(String path) throws IOException {
    return mUfs.getAcl(path);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return mUfs.getBlockSizeByte(path);
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return mUfs.getDirectoryStatus(path);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return mUfs.getFileLocations(path);
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return mUfs.getFileLocations(path, options);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return mUfs.getFileStatus(path);
  }

  @Override
  public String getFingerprint(String path) {
    return mUfs.getFingerprint(path);
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    return mUfs.getOperationMode(physicalUfsState);
  }

  @Override
  public List<String> getPhysicalStores() {
    return mUfs.getPhysicalStores();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return mUfs.getSpace(path, type);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return mUfs.getStatus(path);
  }

  @Override
  public String getUnderFSType() {
    return mUfs.getUnderFSType();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return mUfs.isDirectory(path);
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mUfs.isFile(path);
  }

  @Override
  public boolean isObjectStorage() {
    return mUfs.isObjectStorage();
  }

  @Override
  public boolean isSeekable() {
    return mUfs.isSeekable();
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    return mUfs.listStatus(path);
  }

  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    return mUfs.listStatus(path, options);
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return mUfs.mkdirs(path);
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return mUfs.mkdirs(path, options);
  }

  @Override
  public InputStream open(String path) throws IOException {
    return mUfs.open(path);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return mUfs.open(path, options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return mUfs.renameDirectory(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return mUfs.renameFile(src, dst);
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mUfs.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    mUfs.setOwner(path, owner, group);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    mUfs.setMode(path, mode);
  }

  @Override
  public boolean supportsFlush() {
    return mUfs.supportsFlush();
  }
}
