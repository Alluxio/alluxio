/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.s3;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
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
 * Factory for creating {@link S3UnderFileSystem}. It will ensure AWS credentials are present before
 * returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public class S3UnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public UnderFileSystem create(String path, Configuration configuration, Object unusedConf) {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(configuration);

    if (addAndCheckAWSCredentials(configuration)) {
      try {
        return new S3UnderFileSystem(new AlluxioURI(path), configuration);
      } catch (ServiceException e) {
        LOG.error("Failed to create S3UnderFileSystem.", e);
        throw Throwables.propagate(e);
      }
    }

    String err = "AWS Credentials not available, cannot create S3 Under File System.";
    LOG.error(err);
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path, Configuration configuration) {
    return path != null && path.startsWith(Constants.HEADER_S3N);
  }

  /**
   * Adds AWS credentials from system properties to the Alluxio configuration if they are not
   * already present.
   *
   * @param configuration the Alluxio configuration to check and add credentials to
   * @return true if both access and secret key are present, false otherwise
   */
  private boolean addAndCheckAWSCredentials(Configuration configuration) {
    String accessKeyConf = Constants.S3_ACCESS_KEY;
    if (System.getProperty(accessKeyConf) != null && configuration.get(accessKeyConf) == null) {
      configuration.set(accessKeyConf, System.getProperty(accessKeyConf));
    }
    String secretKeyConf = Constants.S3_SECRET_KEY;
    if (System.getProperty(secretKeyConf) != null && configuration.get(secretKeyConf) == null) {
      configuration.set(secretKeyConf, System.getProperty(secretKeyConf));
    }
    return configuration.get(accessKeyConf) != null
        && configuration.get(secretKeyConf) != null;
  }
}
