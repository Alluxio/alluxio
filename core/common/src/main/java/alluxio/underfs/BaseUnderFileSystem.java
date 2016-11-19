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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.options.CreateOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
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

  /** Maximum length for a single listing query. */
  private static final int MAX_LISTING_LENGTH = 1000;

  /** Length of each list request. */
  protected static final int LISTING_LENGTH =
      Configuration.getInt(PropertyKey.UNDERFS_LISTING_LENGTH) > MAX_LISTING_LENGTH
          ? MAX_LISTING_LENGTH : Configuration.getInt(PropertyKey.UNDERFS_LISTING_LENGTH);

  @Override
  public abstract String getUnderFSType();

  /**
   * Constructs an {@link BaseUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this ufs
   */
  protected BaseUnderFileSystem(AlluxioURI uri) {
    Preconditions.checkNotNull(uri);
    mUri = uri;
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
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return new AtomicFileOutputStream(path, options, this);
  }

  @Override
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(mProperties);
  }

  @Override
  public String[] listRecursive(String path) throws IOException {
    // Clean the path by creating a URI and turning it back to a string
    AlluxioURI uri = new AlluxioURI(path);
    path = uri.toString();
    List<String> returnPaths = new ArrayList<>();
    Queue<String> pathsToProcess = new ArrayDeque<>();
    // We call list initially, so we can return null if the path doesn't denote a directory
    String[] subpaths = list(path);
    if (subpaths == null) {
      return null;
    } else {
      for (String subp : subpaths) {
        pathsToProcess.add(PathUtils.concatPath(path, subp));
      }
    }
    while (!pathsToProcess.isEmpty()) {
      String p = pathsToProcess.remove();
      returnPaths.add(p.substring(path.length() + 1));
      // Add all of its subpaths
      subpaths = list(p);
      if (subpaths != null) {
        for (String subp : subpaths) {
          pathsToProcess.add(PathUtils.concatPath(p, subp));
        }
      }
    }
    return returnPaths.toArray(new String[returnPaths.size()]);
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
}
