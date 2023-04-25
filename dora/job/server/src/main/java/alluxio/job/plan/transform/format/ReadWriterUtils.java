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

package alluxio.job.plan.transform.format;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.uri.NoAuthority;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

/**
 * Utilities for implementing {@link TableReader} and {@link TableWriter}.
 */
public final class ReadWriterUtils {
  private static final String ALLUXIO_HADOOP_FILESYSTEM_DISABLE_CACHE =
      "fs.alluxio.impl.disable.cache";

  /**
   * Checks preconditions of uri.
   *
   * @param uri the URI to check
   */
  public static void checkUri(AlluxioURI uri) {
    Preconditions.checkArgument(uri.getScheme() != null && !uri.getScheme().isEmpty(),
        ExceptionMessage.TRANSFORM_TABLE_URI_LACKS_SCHEME.getMessage(uri));
    if (uri.getScheme().equals(Constants.SCHEME)) {
      Preconditions.checkArgument(uri.getAuthority() != null
              && !uri.getAuthority().equals(NoAuthority.INSTANCE),
          ExceptionMessage.TRANSFORM_TABLE_URI_LACKS_AUTHORITY.getMessage(uri));
    }
  }

  /**
   * @return a new Hadoop conf with alluxio read type set to no cache
   */
  public static Configuration readNoCacheConf() {
    Configuration conf = new Configuration();
    conf.setEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT.getName(), ReadType.NO_CACHE);
    // The cached filesystem might not be configured with the above read type.
    conf.setBoolean(ALLUXIO_HADOOP_FILESYSTEM_DISABLE_CACHE, true);
    return conf;
  }

  /**
   * @return a new Hadoop conf with alluxio write type set to through
   */
  public static Configuration writeThroughConf() {
    Configuration conf = new Configuration();
    conf.setEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT.getName(), WriteType.THROUGH);
    // The cached filesystem might not be configured with the above write type.
    conf.setBoolean(ALLUXIO_HADOOP_FILESYSTEM_DISABLE_CACHE, true);
    return conf;
  }

  private ReadWriterUtils() {} // Prevent initialization
}
