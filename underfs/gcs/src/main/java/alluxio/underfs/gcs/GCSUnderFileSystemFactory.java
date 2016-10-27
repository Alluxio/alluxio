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

package alluxio.underfs.gcs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.jets3t.service.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link GCSUnderFileSystem}. It will ensure Google credentials are present
 * before returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public final class GCSUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new {@link GCSUnderFileSystemFactory}.
   */
  public GCSUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, Object unusedConf) {
    Preconditions.checkNotNull(path);

    if (addAndCheckGoogleCredentials()) {
      try {
        return GCSUnderFileSystem.createInstance(new AlluxioURI(path));
      } catch (ServiceException e) {
        LOG.error("Failed to create GCSUnderFileSystem.", e);
        throw Throwables.propagate(e);
      }
    }

    String err = "Google Credentials not available, cannot create GCS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(Constants.HEADER_GCS);
  }

  /**
   * Adds Google credentials from system properties to the Alluxio configuration if they are not
   * already present.
   *
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean addAndCheckGoogleCredentials() {
    return Configuration.containsKey(PropertyKey.GCS_ACCESS_KEY)
        && Configuration.containsKey(PropertyKey.GCS_SECRET_KEY);
  }
}
