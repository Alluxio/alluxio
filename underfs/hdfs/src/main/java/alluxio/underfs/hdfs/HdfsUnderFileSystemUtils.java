/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.underfs.hdfs;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.Constants;
import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;

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
