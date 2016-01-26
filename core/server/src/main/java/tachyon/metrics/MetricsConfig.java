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

package tachyon.metrics;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;

/**
 * Configurations used by the metrics system.
 */
public final class MetricsConfig {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final String DEFAULT_PREFIX = "*";
  private static final String INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)";
  private static final String METRICS_CONF = "metrics.properties";

  private String mConfigFile;
  private Properties mProperties;
  private Map<String, Properties> mPropertyCategories;

  /**
   * Creates a new {@code MetricsConfig} using the given config file.
   *
   * @param configFile config file to use
   */
  public MetricsConfig(String configFile) {
    mConfigFile = configFile;
    mProperties = new Properties();
    setDefaultProperties();
    loadConfigFile();
    parseConfiguration();
  }

  /**
   * Creates a new {@code MetricsConfig} using the given {@link Properties}.
   *
   * @param properties properties to use
   */
  public MetricsConfig(Properties properties) {
    mProperties = new Properties();
    setDefaultProperties();
    mProperties.putAll(properties);
    parseConfiguration();
  }

  private void addDefaultProperties(Properties prop, Properties defaultProp) {
    for (Map.Entry<Object, Object> entry : defaultProp.entrySet()) {
      String key = entry.getKey().toString();
      if (prop.getProperty(key) == null) {
        prop.setProperty(key, entry.getValue().toString());
      }
    }
  }

  /**
   * Gets properties for the given instance.
   *
   * @param inst the instance name. Currently there are only two instances: "master" and "worker"
   * @return the instance's properties if it is present, otherwise a default one is returned
   */
  public Properties getInstanceProperties(String inst) {
    Properties prop = mPropertyCategories.get(inst);
    if (prop == null) {
      prop = mPropertyCategories.get(DEFAULT_PREFIX);
      if (prop == null) {
        prop = new Properties();
      }
    }
    return prop;
  }

  /**
   * Gets the property categories, used by unit tests only.
   *
   * @return a {@code Map} that maps from instance name to its properties
   */
  public Map<String, Properties> getPropertyCategories() {
    return mPropertyCategories;
  }

  /**
   * Loads the metrics configuration file.
   */
  private void loadConfigFile() {
    InputStream is = null;
    try {
      if (mConfigFile != null) {
        is = new FileInputStream(mConfigFile);
      } else {
        is = getClass().getClassLoader().getResourceAsStream(METRICS_CONF);
      }
      if (is != null) {
        mProperties.load(is);
      }
    } catch (Exception e) {
      LOG.error("Error loading metrics configuration file.");
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Parses the configuration and maps the instance name to its properties.
   */
  private void parseConfiguration() {
    mPropertyCategories = subProperties(mProperties, INSTANCE_REGEX);
    if (mPropertyCategories.containsKey(DEFAULT_PREFIX)) {
      Properties defaultProperties = mPropertyCategories.get(DEFAULT_PREFIX);
      for (Map.Entry<String, Properties> entry : mPropertyCategories.entrySet()) {
        if (!entry.getKey().equals(DEFAULT_PREFIX)) {
          addDefaultProperties(entry.getValue(), defaultProperties);
        }
      }
    }
  }

  /**
   * Sets the default properties. The MetricsServlet is enabled and the path is /metrics/json
   * by default.
   */
  private void setDefaultProperties() {
    mProperties.setProperty("*.sink.servlet.class", "tachyon.metrics.sink.MetricsServlet");
    mProperties.setProperty("*.sink.servlet.path", "/metrics/json");
  }

  /**
   * Uses regex to parse every original property key to a prefix and a suffix. Creates sub
   * properties that are grouped by the prefix.
   *
   * @param prop the original properties
   * @param regex specifies the prefix and suffix pattern
   * @return a {@code Map} maps from the prefix to its properties
   */
  public Map<String, Properties> subProperties(Properties prop, String regex) {
    Map<String, Properties> subProperties = new HashMap<String, Properties>();
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
}
