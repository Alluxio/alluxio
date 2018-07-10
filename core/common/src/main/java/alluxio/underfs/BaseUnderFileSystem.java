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
import alluxio.collections.Pair;
import alluxio.security.authorization.AccessControlList;
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
    return create(path, CreateOptions.defaults().setCreateParent(true));
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
  public AccessControlList getAcl(String path) throws IOException {
    return null;
  }

  @Override
  public String getFingerprint(String path) {
    try {
      UfsStatus status = getStatus(path);
      AccessControlList acl = getAcl(path);
      if (acl == null || !acl.hasExtended()) {
        return Fingerprint.create(getUnderFSType(), status).serialize();
      } else {
        return Fingerprint.create(getUnderFSType(), status, acl).serialize();
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
      returnPaths.add(pathStatus.setName(pathToProcess.substring(path.length() + 1)));

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
    return mkdirs(path, MkdirsOptions.defaults());
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri.getScheme(), ufsBaseUri.getAuthority(),
        PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath), ufsBaseUri.getQueryMap());
  }

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
