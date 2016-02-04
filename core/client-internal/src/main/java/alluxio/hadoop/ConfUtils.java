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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.io.DefaultStringifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Constants;
import alluxio.Configuration;

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
      DefaultStringifier.store(target, confProperties, Constants.TACHYON_CONF_SITE);
    } catch (IOException ex) {
      LOG.error("Unable to store TachyonConf in Hadoop configuration", ex);
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
    // Load TachyonConf if any and merge to the one in TachyonFS
    // Push TachyonConf to the Job conf
    Properties tachyonConfProperties = null;
    if (source.get(Constants.TACHYON_CONF_SITE) != null) {
      LOG.info("Found TachyonConf site from Job configuration for Tachyon");
      try {
        tachyonConfProperties = DefaultStringifier.load(source, Constants.TACHYON_CONF_SITE,
            Properties.class);
      } catch (IOException e) {
        LOG.error("Unable to load TachyonConf from Hadoop configuration", e);
        throw new RuntimeException(e);
      }
    }
    if (tachyonConfProperties == null) {
      tachyonConfProperties = new Properties();
    }
    // Load any Tachyon configuration parameters existing in the Hadoop configuration.
    for (Map.Entry<String, String> entry : source) {
      String propertyName = entry.getKey();
      // TODO(gene): use a better way to enumerate every Tachyon configuration parameter
      if (propertyName.startsWith("alluxio.")
          || propertyName.equals(Constants.S3_ACCESS_KEY)
          || propertyName.equals(Constants.S3_SECRET_KEY)) {
        tachyonConfProperties.put(propertyName, entry.getValue());
      }
    }
    LOG.info("Loading Tachyon properties from Hadoop configuration: {}", tachyonConfProperties);
    return new Configuration(tachyonConfProperties);
  }
}
