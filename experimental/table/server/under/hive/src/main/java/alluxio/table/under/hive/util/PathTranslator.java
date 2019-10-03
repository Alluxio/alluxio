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

package alluxio.table.under.hive.util;

import alluxio.AlluxioURI;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities to convert to and from ufs paths and alluxio paths.
 */
public class PathTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(PathTranslator.class);

  private Map<AlluxioURI, AlluxioURI> mPathMap;

  /**
   * Construct a path translator.
   */
  public PathTranslator() {
    mPathMap = new HashMap<>();
  }

  /**
   * Add a mapping to the path translator.
   *
   * @param alluxioPath the alluxio path
   * @param ufsPath the corresponding ufs path
   *
   * @return PathTranslator object
   */
  public PathTranslator addMapping(AlluxioURI alluxioPath, AlluxioURI ufsPath) {
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
  public AlluxioURI toAlluxioPath(AlluxioURI ufsPath) throws IOException {
    for (Map.Entry<AlluxioURI, AlluxioURI> entry : mPathMap.entrySet()) {
      try {
        if (entry.getValue().isAncestorOf(ufsPath)) {
          String alluxioPath = entry.getKey().getPath()
              + ufsPath.getPath().substring(entry.getValue().getPath().length());
          if (alluxioPath.startsWith("/")) {
            // scheme/authority are missing, so prefix with the scheme and authority
            alluxioPath =
                ConfigurationUtils.getSchemeAuthority(ServerConfiguration.global()) + alluxioPath;
          }
          return new AlluxioURI(alluxioPath);
        }
      } catch (InvalidPathException e) {
        LOG.debug("Invalid path encountered in ");
      }
    }
    // TODO(yuzhu): instead of throwing an exception, mount the path?
    throw new IOException(String
        .format("Failed to translate ufs path (%s). Mapping missing from translator", ufsPath));
  }
}
