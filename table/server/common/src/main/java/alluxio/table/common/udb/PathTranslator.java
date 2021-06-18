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

package alluxio.table.common.udb;

import alluxio.AlluxioURI;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utilities to convert to and from ufs paths and alluxio paths.
 */
public class PathTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(PathTranslator.class);

  private final BiMap<AlluxioURI, AlluxioURI> mPathMap;

  /**
   * Construct a path translator.
   */
  public PathTranslator() {
    mPathMap = HashBiMap.create();
  }

  /**
   * Add a mapping to the path translator.
   *
   * @param alluxioPath the alluxio path
   * @param ufsPath the corresponding ufs path
   *
   * @return PathTranslator object
   */
  public PathTranslator addMapping(String alluxioPath, String ufsPath) {
    mPathMap.put(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath));
    return this;
  }

  /**
   * Returns the corresponding alluxio path, for the specified ufs path.
   *
   * @param ufsPath the ufs path to translate
   * @return the corresponding alluxio path
   * @throws IOException if the ufs path is not mounted
   */
  public String toAlluxioPath(String ufsPath) throws IOException {
    String suffix = ufsPath.endsWith("/") ? "/" : "";
    AlluxioURI ufsUri = new AlluxioURI(ufsPath);
    // first look for an exact match
    if (mPathMap.inverse().containsKey(ufsUri)) {
      return mPathMap.inverse().get(ufsUri).toString() + suffix;
    }
    // otherwise match by longest prefix
    BiMap.Entry<AlluxioURI, AlluxioURI> longestPrefix = null;
    int longestPrefixDepth = -1;
    for (BiMap.Entry<AlluxioURI, AlluxioURI> entry : mPathMap.entrySet()) {
      try {
        AlluxioURI valueUri = entry.getValue();
        if (valueUri.isAncestorOf(ufsUri) && valueUri.getDepth() > longestPrefixDepth) {
          longestPrefix = entry;
          longestPrefixDepth = valueUri.getDepth();
        }
      } catch (InvalidPathException e) {
        throw new IOException(e);
      }
    }
    if (longestPrefix == null) {
      // TODO(yuzhu): instead of throwing an exception, mount the path?
      throw new IOException(String
          .format("Failed to translate ufs path (%s). Mapping missing from translator", ufsPath));
    }
    if (longestPrefix.getKey().equals(longestPrefix.getValue())) {
      // return ufsPath if set the key and value to be same when bypass path.
      return ufsPath;
    }
    try {
      String difference = PathUtils.subtractPaths(ufsUri.getPath(),
          longestPrefix.getValue().getPath());
      AlluxioURI mappedUri = longestPrefix.getKey().join(difference);
      if (!mappedUri.hasScheme() || !mappedUri.hasAuthority()) {
        // scheme/authority are missing, so prefix with the scheme and authority
        AlluxioURI baseUri = new AlluxioURI(
            ConfigurationUtils.getSchemeAuthority(ServerConfiguration.global()) + "/");
        mappedUri = new AlluxioURI(baseUri, mappedUri.getPath(), false);
      }
      return mappedUri.toString() + suffix;
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }
  }
}
