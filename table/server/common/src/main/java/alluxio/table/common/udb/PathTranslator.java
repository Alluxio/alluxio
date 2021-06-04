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

import alluxio.conf.ServerConfiguration;
import alluxio.util.ConfigurationUtils;

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

  private BiMap<String, String> mPathMap;

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
    while (alluxioPath.endsWith("/")) {
      // strip trailing slashes
      alluxioPath = alluxioPath.substring(0, alluxioPath.length() - 1);
    }

    while (ufsPath.endsWith("/")) {
      // strip trailing slashes
      ufsPath = ufsPath.substring(0, ufsPath.length() - 1);
    }
    mPathMap.put(alluxioPath, ufsPath);
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
    for (BiMap.Entry<String, String> entry : mPathMap.entrySet()) {
      if (ufsPath.startsWith(entry.getValue())) {
        // return ufsPath if set the key and value to be same when bypass path.
        if (entry.getKey().equals(entry.getValue())) {
          return ufsPath;
        }
        String alluxioPath = entry.getKey() + ufsPath.substring(entry.getValue().length());
        if (alluxioPath.startsWith("/")) {
          // scheme/authority are missing, so prefix with the scheme and authority
          alluxioPath =
              ConfigurationUtils.getSchemeAuthority(ServerConfiguration.global()) + alluxioPath;
        }
        return alluxioPath;
      }
    }
    // TODO(yuzhu): instead of throwing an exception, mount the path?
    throw new IOException(String
        .format("Failed to translate ufs path (%s). Mapping missing from translator", ufsPath));
  }
}
