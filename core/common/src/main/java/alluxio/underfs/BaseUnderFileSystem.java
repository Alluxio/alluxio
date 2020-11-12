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
import alluxio.Constants;
import alluxio.SyncInfo;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A base abstract {@link UnderFileSystem}.
 */
@ThreadSafe
public abstract class BaseUnderFileSystem implements UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BaseUnderFileSystem.class);

  /** The UFS {@link AlluxioURI} used to create this {@link BaseUnderFileSystem}. */
  protected final AlluxioURI mUri;

  /** UFS Configuration options. */
  protected final UnderFileSystemConfiguration mUfsConf;

  /**
   * Constructs an {@link BaseUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this ufs
   * @param ufsConf UFS configuration
   */
  protected BaseUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    mUri = Preconditions.checkNotNull(uri, "uri");
    mUfsConf = Preconditions.checkNotNull(ufsConf, "ufsConf");
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return create(path, CreateOptions.defaults(mUfsConf).setCreateParent(true));
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    return deleteDirectory(path, DeleteOptions.defaults());
  }

  @Override
  public boolean exists(String path) throws IOException {
    return isFile(path) || isDirectory(path);
  }

  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path)
      throws IOException {
    return new Pair<>(null, null);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    // Noop here by default
  }

  @Override
  public AlluxioConfiguration getConfiguration() {
    return mUfsConf;
  }

  @Override
  public String getFingerprint(String path) {
    // TODO(yuzhu): include default ACL in the fingerprint
    try {
      UfsStatus status = getStatus(path);
      Pair<AccessControlList, DefaultAccessControlList> aclPair = getAclPair(path);

      if (aclPair == null || aclPair.getFirst() == null || !aclPair.getFirst().hasExtended()) {
        return Fingerprint.create(getUnderFSType(), status).serialize();
      } else {
        return Fingerprint.create(getUnderFSType(), status, aclPair.getFirst()).serialize();
      }
    } catch (Exception e) {
      // In certain scenarios, it is expected that the UFS path does not exist.
      LOG.debug("Failed fingerprint. path: {} error: {}", path, e.toString());
      return Constants.INVALID_UFS_FINGERPRINT;
    }
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    UfsMode ufsMode = physicalUfsState.get(mUri.getRootPath());
    if (ufsMode != null) {
      return ufsMode;
    }
    return UfsMode.READ_WRITE;
  }

  @Override
  public List<String> getPhysicalStores() {
    return new ArrayList<>(Arrays.asList(mUri.getRootPath()));
  }

  @Override
  public boolean isObjectStorage() {
    return false;
  }

  @Override
  public boolean isSeekable() {
    return false;
  }

  @Override
  @Nullable
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    if (!options.isRecursive()) {
      return listStatus(path);
    }
    path = validatePath(path);
    List<UfsStatus> returnPaths = new ArrayList<>();
    // Each element is a pair of (full path, UfsStatus)
    Queue<Pair<String, UfsStatus>> pathsToProcess = new ArrayDeque<>();
    // We call list initially, so we can return null if the path doesn't denote a directory
    UfsStatus[] statuses = listStatus(path);
    if (statuses == null) {
      return null;
    } else {
      for (UfsStatus status : statuses) {
        pathsToProcess.add(new Pair<>(PathUtils.concatPath(path, status.getName()), status));
      }
    }
    while (!pathsToProcess.isEmpty()) {
      final Pair<String, UfsStatus> pathToProcessPair = pathsToProcess.remove();
      final String pathToProcess = pathToProcessPair.getFirst();
      UfsStatus pathStatus = pathToProcessPair.getSecond();
      int beginIndex = path.endsWith(AlluxioURI.SEPARATOR) ? path.length() : path.length() + 1;
      returnPaths.add(pathStatus.setName(pathToProcess.substring(beginIndex)));

      if (pathStatus.isDirectory()) {
        // Add all of its subpaths
        UfsStatus[] children = listStatus(pathToProcess);
        if (children != null) {
          for (UfsStatus child : children) {
            pathsToProcess.add(
                new Pair<>(PathUtils.concatPath(pathToProcess, child.getName()), child));
          }
        }
      }
    }
    return returnPaths.toArray(new UfsStatus[returnPaths.size()]);
  }

  @Override
  public InputStream open(String path) throws IOException {
    return open(path, OpenOptions.defaults());
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return mkdirs(path, MkdirsOptions.defaults(mUfsConf));
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri, PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath),
        false);
  }

  @Override
  public boolean supportsActiveSync() {
    return false;
  }

  @Override
  public SyncInfo getActiveSyncInfo() {
    return SyncInfo.emptyInfo();
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return false;
  }

  @Override
  public boolean stopActiveSyncPolling() {
    return false;
  }

  @Override
  public void startSync(AlluxioURI uri)  { }

  @Override
  public void stopSync(AlluxioURI uri) { }

  /**
   * Clean the path by creating a URI and turning it back to a string.
   *
   * @param path the path to validate
   * @return validated path
   */
  protected static String validatePath(String path) {
    return new AlluxioURI(path).toString();
  }
}
