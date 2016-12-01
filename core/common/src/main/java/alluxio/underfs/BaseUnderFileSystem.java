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
import alluxio.collections.Pair;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A base abstract {@link UnderFileSystem}.
 */
@ThreadSafe
public abstract class BaseUnderFileSystem implements UnderFileSystem {
  /** The UFS {@link AlluxioURI} used to create this {@link BaseUnderFileSystem}. */
  protected final AlluxioURI mUri;

  /** A map of property names to values. */
  protected HashMap<String, String> mProperties = new HashMap<>();

  /**
   * Constructs an {@link BaseUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this ufs
   */
  protected BaseUnderFileSystem(AlluxioURI uri) {
    mUri = Preconditions.checkNotNull(uri);
  }

  @Override
  public void configureProperties() throws IOException {
    // Default implementation does not update any properties.
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return create(path, CreateOptions.defaults());
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    return deleteDirectory(path, DeleteOptions.defaults());
  }

  @Override
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(mProperties);
  }

  @Override
  public UnderFileStatus[] listStatus(String path, ListOptions options) throws IOException {
    if (!options.isRecursive()) {
      return listStatus(path);
    }
    path = validatePath(path);
    List<UnderFileStatus> returnPaths = new ArrayList<>();
    // Each element is a pair of (full path, UnderFileStatus)
    Queue<Pair<String, UnderFileStatus>> pathsToProcess = new ArrayDeque<>();
    // We call list initially, so we can return null if the path doesn't denote a directory
    UnderFileStatus[] subpaths = listStatus(path);
    if (subpaths == null) {
      return null;
    } else {
      for (UnderFileStatus subp : subpaths) {
        pathsToProcess.add(new Pair<>(PathUtils.concatPath(path, subp.getName()), subp));
      }
    }
    while (!pathsToProcess.isEmpty()) {
      final Pair<String, UnderFileStatus> pathToProcessPair = pathsToProcess.remove();
      final String pathToProcess = pathToProcessPair.getFirst();
      final UnderFileStatus pathStatus = pathToProcessPair.getSecond();
      returnPaths.add(new UnderFileStatus(pathToProcess.substring(path.length() + 1),
          pathStatus.isDirectory()));

      if (pathStatus.isDirectory()) {
        // Add all of its subpaths
        subpaths = listStatus(pathStatus.getName());
        if (subpaths != null) {
          for (UnderFileStatus subp : subpaths) {
            pathsToProcess
                .add(new Pair<>(PathUtils.concatPath(pathToProcess, subp.getName()), subp));
          }
        }
      }
    }
    return returnPaths.toArray(new UnderFileStatus[returnPaths.size()]);
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

  @Override
  public void setProperties(Map<String, String> properties) {
    mProperties.clear();
    mProperties.putAll(properties);
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
