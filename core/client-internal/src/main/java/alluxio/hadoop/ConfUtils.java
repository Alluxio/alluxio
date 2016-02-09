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

package alluxio.hadoop;

import alluxio.Configuration;
import alluxio.Constants;

import org.apache.hadoop.io.DefaultStringifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class for {@link Configuration}.
 */
@ThreadSafe
public final class ConfUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private ConfUtils() {} // Prevent instantiation.

  /**
   * Stores the source {@link Configuration} object to the target
   * Hadoop {@link org.apache.hadoop.conf.Configuration} object.
   *
   * @param source the {@link Configuration} to be stored
   * @param target the {@link org.apache.hadoop.conf.Configuration} target
   */
  public static void storeToHadoopConfiguration(Configuration source,
      org.apache.hadoop.conf.Configuration target) {
  // Need to set io.serializations key to prevent NPE when trying to get SerializationFactory.
    target.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    Properties confProperties = source.getInternalProperties();
    try {
      DefaultStringifier.store(target, confProperties, Constants.CONF_SITE);
    } catch (IOException ex) {
      LOG.error("Unable to store Alluxio configuration in Hadoop configuration", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Loads {@link Configuration} from Hadoop {@link org.apache.hadoop.conf.Configuration} source.
   *
   * @param source the {@link org.apache.hadoop.conf.Configuration} to load from
   * @return instance of {@link Configuration} to be loaded
   */
  public static Configuration loadFromHadoopConfiguration(
      org.apache.hadoop.conf.Configuration source) {
    // Load Alluxio configuration if any and merge to the one in Alluxio file system
    // Push Alluxio configuration to the Job configuration
    Properties alluxioConfProperties = null;
    if (source.get(Constants.CONF_SITE) != null) {
      LOG.info("Found Alluxio configuration site from Job configuration for Alluxio");
      try {
        alluxioConfProperties =
            DefaultStringifier.load(source, Constants.CONF_SITE, Properties.class);
      } catch (IOException e) {
        LOG.error("Unable to load Alluxio configuration from Hadoop configuration", e);
        throw new RuntimeException(e);
      }
    }
    if (alluxioConfProperties == null) {
      alluxioConfProperties = new Properties();
    }
    // Load any Alluxio configuration parameters existing in the Hadoop configuration.
    for (Map.Entry<String, String> entry : source) {
      String propertyName = entry.getKey();
      // TODO(gene): use a better way to enumerate every Alluxio configuration parameter
      if (propertyName.startsWith("alluxio.")
          || propertyName.equals(Constants.S3_ACCESS_KEY)
          || propertyName.equals(Constants.S3_SECRET_KEY)) {
        alluxioConfProperties.put(propertyName, entry.getValue());
      }
    }
    LOG.info("Loading Alluxio properties from Hadoop configuration: {}", alluxioConfProperties);
    return new Configuration(alluxioConfProperties);
  }
}
