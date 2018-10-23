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

package alluxio.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Configurations used by the metrics system.
 */
@NotThreadSafe
public final class MetricsConfig {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);
  private Properties mProperties;

  /**
   * Creates a new {@code MetricsConfig} using the given config file.
   *
   * @param configFile config file to use
   */
  public MetricsConfig(String configFile) {
    mProperties = new Properties();
    if (Files.exists(Paths.get(configFile))) {
      loadConfigFile(configFile);
    }
    removeInstancePrefix();
  }

  /**
   * Creates a new {@code MetricsConfig} using the given {@link Properties}.
   *
   * @param properties properties to use
   */
  public MetricsConfig(Properties properties) {
    mProperties = new Properties();
    mProperties.putAll(properties);
    removeInstancePrefix();
  }

  /**
   * @return the properties
   */
  public Properties getProperties() {
    return mProperties;
  }

  /**
   * Uses regex to parse every original property key to a prefix and a suffix. Creates sub
   * properties that are grouped by the prefix.
   *
   * @param prop the original properties
   * @param regex prefix and suffix pattern
   * @return a {@code Map} from the prefix to its properties
   */
  public static Map<String, Properties> subProperties(Properties prop, String regex) {
    Map<String, Properties> subProperties = new HashMap<>();
    Pattern pattern = Pattern.compile(regex);

    for (Map.Entry<Object, Object> entry : prop.entrySet()) {
      Matcher m = pattern.matcher(entry.getKey().toString());
      if (m.find()) {
        String prefix = m.group(1);
        String suffix = m.group(2);
        if (!subProperties.containsKey(prefix)) {
          subProperties.put(prefix, new Properties());
        }
        subProperties.get(prefix).put(suffix, entry.getValue());
      }
    }
    return subProperties;
  }

  /**
   * Loads the metrics configuration file.
   *
   * @param configFile the metrics config file
   */
  private void loadConfigFile(String configFile) {
    try (InputStream is = new FileInputStream(configFile)) {
      mProperties.load(is);
    } catch (Exception e) {
      LOG.error("Error loading metrics configuration file.", e);
    }
  }

  /**
   * Removes the instance prefix in the properties. This is to make the configuration
   * parsing logic backward compatible with old configuration format.
   */
  private void removeInstancePrefix() {
    Properties newProperties = new Properties();
    for (Map.Entry<Object, Object> entry : mProperties.entrySet()) {
      String key = entry.getKey().toString();

      if (key.startsWith("*") || key.startsWith("worker") || key.startsWith("master")) {
        String newKey = key.substring(key.indexOf('.') + 1);
        newProperties.put(newKey, entry.getValue());
      } else {
        newProperties.put(key, entry.getValue());
      }
    }
    mProperties = newProperties;
  }

  @Override
  public String toString() {
    return mProperties.toString();
  }
}
