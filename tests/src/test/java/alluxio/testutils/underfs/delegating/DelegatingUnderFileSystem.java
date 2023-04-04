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

package alluxio.testutils.underfs.delegating;

import alluxio.dora.AlluxioURI;
import alluxio.dora.SyncInfo;
import alluxio.dora.collections.Pair;
import alluxio.dora.conf.AlluxioConfiguration;
import alluxio.dora.security.authorization.AccessControlList;
import alluxio.dora.security.authorization.AclEntry;
import alluxio.dora.security.authorization.DefaultAccessControlList;
import alluxio.dora.underfs.Fingerprint;
import alluxio.dora.underfs.UfsDirectoryStatus;
import alluxio.dora.underfs.UfsFileStatus;
import alluxio.dora.underfs.UfsMode;
import alluxio.dora.underfs.UfsStatus;
import alluxio.dora.underfs.UnderFileSystem;
import alluxio.dora.underfs.options.CreateOptions;
import alluxio.dora.underfs.options.DeleteOptions;
import alluxio.dora.underfs.options.FileLocationOptions;
import alluxio.dora.underfs.options.ListOptions;
import alluxio.dora.underfs.options.MkdirsOptions;
import alluxio.dora.underfs.options.OpenOptions;

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
  public void cleanup() throws IOException {
    mUfs.cleanup();
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
  public OutputStream createNonexistingFile(String path, CreateOptions options) throws IOException {
    return mUfs.createNonexistingFile(path, options);
  }

  @Override
  public OutputStream createNonexistingFile(String path) throws IOException {
    return mUfs.createNonexistingFile(path);
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
  public boolean deleteExistingDirectory(String path) throws IOException {
    return mUfs.deleteExistingDirectory(path);
  }

  @Override
  public boolean deleteExistingDirectory(String path, DeleteOptions options) throws IOException {
    return mUfs.deleteExistingDirectory(path, options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return mUfs.deleteFile(path);
  }

  @Override
  public boolean deleteExistingFile(String path) throws IOException {
    return mUfs.deleteExistingFile(path);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mUfs.exists(path);
  }

  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path)
      throws IOException {
    return mUfs.getAclPair(path);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return mUfs.getBlockSizeByte(path);
  }

  @Override
  public AlluxioConfiguration getConfiguration() throws IOException {
    return mUfs.getConfiguration();
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return mUfs.getDirectoryStatus(path);
  }

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(String path) throws IOException {
    return mUfs.getExistingDirectoryStatus(path);
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
  public UfsFileStatus getExistingFileStatus(String path) throws IOException {
    return mUfs.getExistingFileStatus(path);
  }

  @Override
  public String getFingerprint(String path) {
    return mUfs.getFingerprint(path);
  }

  @Override
  public Fingerprint getParsedFingerprint(String path) {
    return mUfs.getParsedFingerprint(path);
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
  public UfsStatus getExistingStatus(String path) throws IOException {
    return mUfs.getExistingStatus(path);
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
  public boolean isExistingDirectory(String path) throws IOException {
    return mUfs.isExistingDirectory(path);
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
  public InputStream openExistingFile(String path, OpenOptions options) throws IOException {
    return mUfs.openExistingFile(path, options);
  }

  @Override
  public InputStream openExistingFile(String path) throws IOException {
    return mUfs.openExistingFile(path);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return mUfs.renameDirectory(src, dst);
  }

  @Override
  public boolean renameRenamableDirectory(String src, String dst) throws IOException {
    return mUfs.renameRenamableDirectory(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return mUfs.renameFile(src, dst);
  }

  @Override
  public boolean renameRenamableFile(String src, String dst) throws IOException {
    return mUfs.renameRenamableFile(src, dst);
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mUfs.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    mUfs.setAclEntries(path, aclEntries);
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
  public boolean supportsFlush() throws IOException {
    return mUfs.supportsFlush();
  }

  @Override
  public boolean supportsActiveSync() {
    return mUfs.supportsActiveSync();
  }

  @Override
  public SyncInfo getActiveSyncInfo() throws IOException {
    return mUfs.getActiveSyncInfo();
  }

  @Override
  public void startSync(AlluxioURI uri) throws IOException {
    mUfs.startSync(uri);
  }

  @Override
  public void stopSync(AlluxioURI uri) throws IOException {
    mUfs.stopSync(uri);
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return mUfs.startActiveSyncPolling(txId);
  }

  @Override
  public boolean stopActiveSyncPolling() throws IOException {
    return mUfs.stopActiveSyncPolling();
  }
}
