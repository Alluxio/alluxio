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

package alluxio.underfs.hdfs;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystem;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for the HDFS implementation of the {@link UnderFileSystem}.
 */
@ThreadSafe
public final class HdfsUnderFileSystemUtils {

  private HdfsUnderFileSystemUtils() {} // prevent instantiation

  /**
   * Replaces default key with user provided Alluxio property key.
   *
   * @param hadoopConf configuration to replace the key in
   * @param key the key to replace
   */
  public static void addKey(org.apache.hadoop.conf.Configuration hadoopConf, PropertyKey key) {
    if (Configuration.containsKey(key)) {
      hadoopConf.set(key.toString(), Configuration.get(key));
    }
  }

  /**
   * Adds S3 keys to the given {@code conf} object if the user has specified them using system
   * properties, and they're not already set.
   *
   * @param conf the Hadoop configuration
   */
  public static void addS3Credentials(org.apache.hadoop.conf.Configuration conf) {
    String accessKeyConf = PropertyKey.S3N_ACCESS_KEY.toString();
    if (System.getProperty(accessKeyConf) != null && conf.get(accessKeyConf) == null) {
      conf.set(accessKeyConf, System.getProperty(accessKeyConf));
    }
    String secretKeyConf = PropertyKey.S3N_SECRET_KEY.toString();
    if (System.getProperty(secretKeyConf) != null && conf.get(secretKeyConf) == null) {
      conf.set(secretKeyConf, System.getProperty(secretKeyConf));
    }
  }

}
