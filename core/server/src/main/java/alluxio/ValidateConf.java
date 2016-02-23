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

package alluxio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Validate the Alluxio configuration.
 *
 */
@ThreadSafe
public final class ValidateConf {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static boolean validate() {
    Set<String> validProperties = new HashSet<String>();
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
    validProperties.add("alluxio.version");

    // There are three properties that are auto-generated in WorkerStorage based on corresponding
    // format strings defined in alluxio.Constants. Here we transform each format string to a regexp
    // to check if a property name follows the format. E.g.,
    // "alluxio.worker.tieredstore.level%d.alias" is transformed to
    // "alluxio\.worker\.tieredstore\.level\d+\.alias".
    Pattern aliasPattern =
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
    Configuration configuration = new Configuration();
    boolean valid = true;
    for (Entry<String, String> entry : configuration.toMap().entrySet()) {
      String propertyName = entry.getKey();
      if (aliasPattern.matcher(propertyName).matches()
          || dirsPathPattern.matcher(propertyName).matches()
          || dirsQuotaPattern.matcher(propertyName).matches()
          || reservedRatioPattern.matcher(propertyName).matches()) {
        continue;
      }
      if (propertyName.startsWith("alluxio.") && !validProperties.contains(propertyName)) {
        System.err.println("Unsupported or deprecated property " + propertyName);
        valid = false;
      }
    }
    return valid;
  }

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   */
  public static void main(String[] args) {
    if (!validate()) {
      System.exit(-1);
    }
    System.out.println("All configuration entries are valid.");
    System.exit(0);
  }

  private ValidateConf() {} // prevent instantiation.
}
