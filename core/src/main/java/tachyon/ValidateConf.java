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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.conf.TachyonConf;
import tachyon.Constants;

/**
 * Validate the TachyonConf object.
 *
 */
public class ValidateConf {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static boolean validate() {
    Constants constants = new Constants();
    Set<String> validProperties = new HashSet<String>();
    try {
      // Iterate over the array of Field objects in tachyon.Conatants by reflection
      for (Field field : constants.getClass().getDeclaredFields()) {
        if (field.getType().isAssignableFrom(String.class)) {
          String name = (String) field.get(constants);
          if (name.startsWith("tachyon.")) {
            validProperties.add(name.trim());
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    // There are two properties that are defined in tachyon-default.properties but not declared in
    // Tachyon.Constants.
    validProperties.add("tachyon.version");
    validProperties.add("tachyon.debug");

    // There are three properties that are auto-generated in WorkerStorage rather than defined in
    // Tachyon.Constants.
    Pattern aliasPattern = Pattern.compile("tachyon.worker.tieredstore.level\\d+.alias");
    Pattern dirsPathPattern = Pattern.compile("tachyon.worker.tieredstore.level\\d+.dirs.path");
    Pattern dirsQuotaPattern = Pattern.compile("tachyon.worker.tieredstore.level\\d+.dirs.quota");

    TachyonConf tachyonConf = new TachyonConf();
    boolean valid = true;
    for (Entry<String, String> entry : tachyonConf.toMap().entrySet()) {
      String propertyName = entry.getKey();
      if (aliasPattern.matcher(propertyName).matches()
          || dirsPathPattern.matcher(propertyName).matches()
          || dirsQuotaPattern.matcher(propertyName).matches()) {
        continue;
      }
      if (propertyName.startsWith("tachyon.") && !validProperties.contains(propertyName)) {
        System.out.println("Unsupported or deprecated property " + propertyName);
        valid = false;
      }
    }
    return valid;
  }

  public static void main(String[] args) {
    if (!validate()) {
      System.exit(-1);
    }
    System.exit(0);
  }

}
