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

package alluxio.hadoop;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.apache.hadoop.io.DefaultStringifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class for merging Alluxio {@link Configuration} with Hadoop's Configuration class.
 */
@ThreadSafe
public final class HadoopConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopConfigurationUtils.class);

  private HadoopConfigurationUtils() {} // Prevent instantiation.

  /**
   * Stores the Alluxio {@link Configuration} to the target
   * Hadoop {@link org.apache.hadoop.conf.Configuration} object.
   *
   * @param target the {@link org.apache.hadoop.conf.Configuration} target
   */
  public static void storeToHadoopConfiguration(org.apache.hadoop.conf.Configuration target) {
  // Need to set io.serializations key to prevent NPE when trying to get SerializationFactory.
    target.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    Map<String, String> confProperties = Configuration.toMap();
    try {
      DefaultStringifier.store(target, confProperties, PropertyKey.SITE_CONF_DIR.toString());
    } catch (IOException ex) {
      LOG.error("Unable to store Alluxio configuration in Hadoop configuration", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Merges Hadoop {@link org.apache.hadoop.conf.Configuration} into the Alluxio configuration.
   *
   * @param source the {@link org.apache.hadoop.conf.Configuration} to merge
   */
  public static void mergeHadoopConfiguration(org.apache.hadoop.conf.Configuration source) {
    // Load Alluxio configuration if any and merge to the one in Alluxio file system
    // Push Alluxio configuration to the Job configuration
    Properties alluxioConfProperties = new Properties();
    // Load any Alluxio configuration parameters existing in the Hadoop configuration.
    for (Map.Entry<String, String> entry : source) {
      String propertyName = entry.getKey();
      if (PropertyKey.isValid(propertyName)) {
        alluxioConfProperties.put(propertyName, entry.getValue());
      }
    }
    LOG.info("Loading Alluxio properties from Hadoop configuration: {}", alluxioConfProperties);
    // Merge the relevant Hadoop configuration into Alluxio's configuration.
    // TODO(jiri): support multiple client configurations (ALLUXIO-2034)
    Configuration.merge(alluxioConfProperties);
  }
}
