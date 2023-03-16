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

package alluxio.cli.docgen;

import static alluxio.cli.docgen.ConfigurationDocGenerator.CSV_FILE_NAMES;
import static alluxio.cli.docgen.ConfigurationDocGenerator.YML_FILE_NAMES;

import alluxio.annotation.PublicApi;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.PathUtils;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility to validate existence of any necessary configuration docGen changes during pull request.
 */
@ThreadSafe
@PublicApi
public final class ConfigurationDocValidator extends DocValidator {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationDocValidator.class);
  public static final String CSV_FILE_HEADER = "propertyName,defaultValue";

  private ConfigurationDocValidator() {
  } // prevent instantiation

  /**
   * Validates configuration files.
   *
   * @return returns number of deltas
   */
  public static Integer validate() throws IOException {
    Collection<? extends PropertyKey> defaultKeys = PropertyKey.defaultKeys();
    defaultKeys.removeIf(PropertyKey::isHidden);

    // Sort defaultKeys
    List<PropertyKey> dfkeys = new ArrayList<>(defaultKeys);
    Collections.sort(dfkeys);

    Integer numDeltas = 0;
    numDeltas += validateFiles(dfkeys, CSV_FILE_DIR, CSV_FILE_NAMES);
    numDeltas += validateFiles(dfkeys, YML_FILE_DIR, YML_FILE_NAMES);

    return numDeltas;
  }

  private static Integer validateFiles(List<PropertyKey> dfkeys,
                                       String fileDir, List<String> fileNames)
          throws IOException {

    String filePath = PathUtils.concatPath(Configuration.getString(PropertyKey.HOME), fileDir);
    Integer numDeltas = 0;
    for (String fileName : fileNames) {
      String key = fileName.substring(0, fileName.indexOf("configuration") - 1);

      // based on key substring within each filename, create a regex to
      // filter for specific property keys.
      List<String> rPatterns = new ArrayList<String>();
      switch (key) {
        case "user":
          rPatterns.add("^alluxio\\.(user|user\\..*)");
          break;
        case "master":
          rPatterns.add("^alluxio\\.(master|master\\..*)");
          break;
        case "worker":
          rPatterns.add("^alluxio\\.(worker|worker\\..*)");
          break;
        case "security":
          rPatterns.add("^alluxio\\.(security|security\\..*)");
          break;
        case "cluster-management":
          rPatterns.add("^alluxio\\.(integration|integration\\..*)");
          break;
        case "common":
          rPatterns.add("^alluxio\\.(?!(user|master|worker|security|integration)\\b)\\w*[\\.]?.*");
          // additional regex pattern to retain non-system configuration records.
          rPatterns.add("^(fs|s3a)\\b\\..*");
          break;
        default:
          // ignore and skip any non-relevant files.
          LOG.warn("Ignoring unrecognized filename substring key: " + key);
          continue;
      }

      List<Pattern> compiledRegex = new ArrayList<Pattern>();
      for (String pattern : rPatterns) {
        compiledRegex.add(Pattern.compile(pattern));
      }

      List<String> revised = new ArrayList<String>();
      if (fileDir.equals(CSV_FILE_DIR)) {
        revised.add(CSV_FILE_HEADER);
      }
      for (PropertyKey propertyKey : dfkeys) {
        String pKey = propertyKey.toString();
        Boolean matched = false;
        for (Pattern pattern : compiledRegex) {
          if (pattern.matcher(pKey).matches()) {
            matched = true;
          }
        }
        if (matched) {
          String pKeyDesc;
          if (fileDir.equals(CSV_FILE_DIR)) {
            pKeyDesc = getPropertyDescriptionCSV(pKey, propertyKey);
          } else {
            pKeyDesc = StringEscapeUtils.escapeHtml4(getPropertyDescriptionYML(pKey, propertyKey));
          }
          // need to split multi-line strings with embedded newline character into separate
          // list records. This is needed to line up records from original files loaded from
          // the file system.
          revised.addAll(Arrays.asList(pKeyDesc.split("\n")));
        }
      }

      String fileFullPathName = PathUtils.concatPath(filePath, fileName);
      List<String> original = Files.readAllLines(Paths.get(fileFullPathName));

      numDeltas += generateDiff(original, revised, fileFullPathName);
    }
    return numDeltas;
  }

  private static String getPropertyDescriptionCSV(String pKey, PropertyKey propertyKey) {
    String defaultDescription;
    if (propertyKey.getDefaultSupplier().get() == null) {
      defaultDescription = "";
    } else {
      defaultDescription = propertyKey.getDefaultSupplier().getDescription();
    }
    // Quote the whole description to escape characters such as commas.
    defaultDescription = String.format("\"%s\"", defaultDescription);

    // Write property key and default value to CSV
    String keyValueStr = pKey + "," + defaultDescription;
    return keyValueStr;
  }

  private static String getPropertyDescriptionYML(String pKey, PropertyKey propertyKey) {
    // Puts descriptions in single quotes to avoid having to escaping reserved characters.
    // Still needs to escape single quotes with double single quotes.
    String description = propertyKey.getDescription().replace("'", "''");

    // Write property key and default value to yml files
    if (propertyKey.isIgnoredSiteProperty()) {
      description += " Note: overwriting this property will only work when it is passed as a "
              + "JVM system property (e.g., appending \"-D" + propertyKey + "\"=<NEW_VALUE>\" to "
              + "$ALLUXIO_JAVA_OPTS). Setting it in alluxio-site.properties will not work.";
    }
    String keyValueStr = String.format("%s:%n  '%s'", pKey, description);
    return keyValueStr;
  }
}
