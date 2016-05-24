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
    Map<String, String> confProperties = source.toMap();
    try {
      DefaultStringifier.store(target, confProperties, Constants.SITE_CONF_DIR);
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
    Properties alluxioConfProperties = new Properties();
    // Load any Alluxio configuration parameters existing in the Hadoop configuration.
    for (Map.Entry<String, String> entry : source) {
      String propertyName = entry.getKey();
      // TODO(gene): use a better way to enumerate every Alluxio configuration parameter
      if (propertyName.startsWith("alluxio.")
          || propertyName.equals(Constants.S3_ACCESS_KEY)
          || propertyName.equals(Constants.S3_SECRET_KEY)
          || propertyName.equals(Constants.SWIFT_API_KEY)
          || propertyName.equals(Constants.SWIFT_AUTH_METHOD_KEY)
          || propertyName.equals(Constants.SWIFT_AUTH_PORT_KEY)
          || propertyName.equals(Constants.SWIFT_AUTH_URL_KEY)
          || propertyName.equals(Constants.SWIFT_PASSWORD_KEY)
          || propertyName.equals(Constants.SWIFT_TENANT_KEY)
          || propertyName.equals(Constants.SWIFT_USE_PUBLIC_URI_KEY)
          || propertyName.equals(Constants.SWIFT_USER_KEY)) {
        alluxioConfProperties.put(propertyName, entry.getValue());
      }
    }
    LOG.info("Loading Alluxio properties from Hadoop configuration: {}", alluxioConfProperties);
    return Configuration.fromMap(alluxioConfProperties);
  }
}
