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

package tachyon;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.conf.TachyonConf;

/**
 * Validate the Tachyon configuration.
 *
 */
@ThreadSafe
public final class ValidateConf {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static boolean validate() {
    Set<String> validProperties = new HashSet<String>();
    try {
      // Iterate over the array of Field objects in tachyon.Constants by reflection
      for (Field field : Constants.class.getDeclaredFields()) {
        if (field.getType().isAssignableFrom(String.class)) {
          // all fields are static, so ignore the argument
          String name = (String) field.get(null);
          if (name.startsWith("tachyon.")) {
            validProperties.add(name.trim());
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    // Tachyon version is a valid conf entry but not defined in tachyon.Constants
    validProperties.add("tachyon.version");

    // There are three properties that are auto-generated in WorkerStorage based on corresponding
    // format strings defined in tachyon.Constants. Here we transform each format string to a regexp
    // to check if a property name follows the format. E.g.,
    // "tachyon.worker.tieredstore.level%d.alias" is transformed to
    // "tachyon\.worker\.tieredstore\.level\d+\.alias".
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
    TachyonConf tachyonConf = new TachyonConf();
    boolean valid = true;
    for (Entry<String, String> entry : tachyonConf.toMap().entrySet()) {
      String propertyName = entry.getKey();
      if (aliasPattern.matcher(propertyName).matches()
          || dirsPathPattern.matcher(propertyName).matches()
          || dirsQuotaPattern.matcher(propertyName).matches()
          || reservedRatioPattern.matcher(propertyName).matches()) {
        continue;
      }
      if (propertyName.startsWith("tachyon.") && !validProperties.contains(propertyName)) {
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
