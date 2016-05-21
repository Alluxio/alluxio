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
package alluxio.util;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Utilities to create Alluxio configurations.
 */
public final class ConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private ConfigurationUtils() {} // prevent instantiation

  /**
   * Loads properties from resource. This method will search Classpath for the properties file with
   * the given resourceName.
   *
   * @param resourceName filename of the properties file
   * @return a set of properties on success, or null if failed
   */
  public static Properties loadPropertiesFromResource(String resourceName) {
    Properties properties = new Properties();

    InputStream inputStream =
        Configuration.class.getClassLoader().getResourceAsStream(resourceName);
    if (inputStream == null) {
      return null;
    }

    try {
      properties.load(inputStream);
    } catch (IOException e) {
      LOG.error("Unable to load default Alluxio properties file {}", resourceName, e);
      return null;
    }
    return properties;
  }

  /**
   * Loads properties from the given file. This method will search Classpath for the properties
   * file.
   *
   * @param filePath the absolute path of the file to load properties
   * @return a set of properties on success, or null if failed
   */
  public static Properties loadPropertiesFromFile(String filePath) {
    Properties properties = new Properties();

    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      properties.load(fileInputStream);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      LOG.error("Unable to load properties file {}", filePath, e);
      return null;
    }
    return properties;
  }

  /**
   * Searches the given properties file from a list of paths as well as the classpath.
   *
   * @param propertiesFile the file to load properties
   * @param confPathList a list of paths to search the propertiesFile
   * @return loaded properties on success, or null if failed
   */
  public static Properties searchPropertiesFile(String propertiesFile,
      String[] confPathList) {
    if (propertiesFile == null || confPathList == null) {
      return null;
    }
    for (String path : confPathList) {
      String file = PathUtils.concatPath(path, propertiesFile);
      Properties properties = loadPropertiesFromFile(file);
      if (properties != null) {
        // If a site conf is successfully loaded, stop trying different paths
        LOG.info("Configuration file {} loaded.", file);
        return properties;
      }
    }
    return loadPropertiesFromResource(propertiesFile);
  }

  /**
   * Validates the configurations.
   *
   * @param configuration Configuration to validate
   * @return true if the validation succeeds, false otherwise
   */
  public static boolean validateConf(Configuration configuration) {
    Set<String> validProperties = new HashSet<>();
    try {
      // Iterate over the array of Field objects in alluxio.Constants by reflection
      for (Field field : Constants.class.getDeclaredFields()) {
        if (field.getType().isAssignableFrom(String.class)) {
          // all fields are static, so ignore the argument
          String name = (String) field.get(null);
          if (name.startsWith("alluxio.")) {
            validProperties.add(name.trim());
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    // Alluxio version is a valid conf entry but not defined in alluxio.Constants
    validProperties.add(Constants.VERSION);

    // There are three properties that are auto-generated in WorkerStorage based on corresponding
    // format strings defined in alluxio.Constants. Here we transform each format string to a regexp
    // to check if a property name follows the format. E.g.,
    // "alluxio.worker.tieredstore.level%d.alias" is transformed to
    // "alluxio\.worker\.tieredstore\.level\d+\.alias".
    Pattern masterAliasPattern =
        Pattern.compile(Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT
            .replace("%d", "\\d+").replace(".", "\\."));
    Pattern workerAliasPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.replace("%d", "\\d+")
            .replace(".", "\\."));
    Pattern dirsPathPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT
            .replace("%d", "\\d+").replace(".", "\\."));
    Pattern dirsQuotaPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.replace("%d",
            "\\d+").replace(".", "\\."));
    Pattern reservedRatioPattern =
        Pattern.compile(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT.replace("%d",
            "\\d+").replace(".", "\\."));
    boolean valid = true;
    for (Map.Entry<String, String> entry : configuration.toMap().entrySet()) {
      String propertyName = entry.getKey();
      if (masterAliasPattern.matcher(propertyName).matches()
          || workerAliasPattern.matcher(propertyName).matches()
          || dirsPathPattern.matcher(propertyName).matches()
          || dirsQuotaPattern.matcher(propertyName).matches()
          || reservedRatioPattern.matcher(propertyName).matches()) {
        continue;
      }
      if (propertyName.startsWith("alluxio.") && !validProperties.contains(propertyName)) {
        LOG.error("Unsupported or deprecated property " + propertyName);
        valid = false;
      }
    }
    return valid;
  }
}
