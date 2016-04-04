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

package alluxio.underfs.hdfs;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for the HDFS implementation of the {@link UnderFileSystem}.
 */
@ThreadSafe
public class HdfsUnderFileSystemUtils {
  /**
   * Replaces default key with user provided key.
   *
   * @param hadoopConf configuration to replace the key in
   * @param conf Alluxio configuration with the key
   * @param key the key to replace
   */
  public static void addKey(org.apache.hadoop.conf.Configuration hadoopConf, Configuration conf,
      String key) {
    if (System.getProperty(key) != null) {
      hadoopConf.set(key, System.getProperty(key));
    } else if (conf.get(key) != null) {
      hadoopConf.set(key, conf.get(key));
    }
  }

  /**
   * Adds S3 keys to the given {@code conf} object if the user has specified them using system
   * properties, and they're not already set.
   *
   * @param conf the Hadoop configuration
   */
  public static void addS3Credentials(org.apache.hadoop.conf.Configuration conf) {
    String accessKeyConf = Constants.S3_ACCESS_KEY;
    if (System.getProperty(accessKeyConf) != null && conf.get(accessKeyConf) == null) {
      conf.set(accessKeyConf, System.getProperty(accessKeyConf));
    }
    String secretKeyConf = Constants.S3_SECRET_KEY;
    if (System.getProperty(secretKeyConf) != null && conf.get(secretKeyConf) == null) {
      conf.set(secretKeyConf, System.getProperty(secretKeyConf));
    }
  }

}
