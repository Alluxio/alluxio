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

package alluxio.underfs.stocator;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.PropertyKey;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link StocatorUnderFileSystem}.
 */
@ThreadSafe
public class StocatorUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new {@link StocatorUnderFileSystemFactory}.
   */
  public StocatorUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, Object unusedConf) {
    Preconditions.checkNotNull(path);
    try {
      return new StocatorUnderFileSystem(new AlluxioURI(path));
    } catch (Exception e) {
      LOG.error("Failed to create SwiftUnderFileSystem.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean supportsPath(String path) {
    LOG.debug("Support path {}", path);
    if (path == null) {
      return false;
    }
    String stocatorSupported = System.getProperty(PropertyKey.STOCATOR_SCHEME_LIST.toString());
    stocatorSupported = Configuration.get(PropertyKey.STOCATOR_SCHEME_LIST);
    if (stocatorSupported != null) {
      String[] schemes = stocatorSupported.split(",");
      for (String scheme : schemes) {
        if (path.startsWith(scheme)) {
          return true;
        }
      }
    }
    return false;
  }
}
