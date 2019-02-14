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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class for merging {@link AlluxioConfiguration} with Hadoop's Configuration class.
 */
@ThreadSafe
public final class HadoopConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopConfigurationUtils.class);

  private HadoopConfigurationUtils() {} // Prevent instantiation.

  /**
   * Merges Hadoop {@link org.apache.hadoop.conf.Configuration} with Alluxio properties.
   *
   * @param hadoopConf the {@link org.apache.hadoop.conf.Configuration} to merge
   * @param alluxioProps the Alluxio properties to merge
   * @return a configuration with properties all merged
   */
  public static InstancedConfiguration mergeHadoopConfiguration(
      org.apache.hadoop.conf.Configuration hadoopConf, AlluxioProperties alluxioProps) {
    // Load Alluxio configuration if any and merge to the one in Alluxio file system
    // Push Alluxio configuration to the Job configuration
    Properties alluxioConfProperties = new Properties();
    // Load any Alluxio configuration parameters existing in the Hadoop configuration.
    for (Map.Entry<String, String> entry : hadoopConf) {
      String propertyName = entry.getKey();
      if (PropertyKey.isValid(propertyName)) {
        alluxioConfProperties.put(propertyName, entry.getValue());
      }
    }
    LOG.info("Loading Alluxio properties from Hadoop configuration: {}", alluxioConfProperties);
    // Merge the relevant Hadoop configuration into Alluxio's configuration.

    alluxioProps.merge(alluxioConfProperties, Source.RUNTIME);
    // Creting a new instanced configuration from an AlluxioProperties object isn't expensive.
    InstancedConfiguration mergedConf = new InstancedConfiguration(alluxioProps);
    mergedConf.validate();
    return mergedConf;
  }

  /**
   * Merges an {@link AlluxioConfiguration} into an hadoop
   * {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param source The source hadoop configuration
   * @param alluxioConf the alluxio configuration to merge
   * @return a hadoop configuration object with the properties from the {@link AlluxioConfiguration}
   */
  public static org.apache.hadoop.conf.Configuration mergeAlluxioConfiguration(
      org.apache.hadoop.conf.Configuration source, AlluxioConfiguration alluxioConf) {
    org.apache.hadoop.conf.Configuration mergedConf = new org.apache.hadoop.conf.Configuration();
    source.forEach((Map.Entry<String, String> e) -> mergedConf.set(e.getKey(), e.getValue()));
    alluxioConf.copyProperties().forEach((PropertyKey pk, String val) -> {
      if (val != null) {
        mergedConf.set(pk.getName(), val);
      }
    });
    return mergedConf;
  }
}
