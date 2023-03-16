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

import alluxio.annotation.PublicApi;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.PathUtils;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility to validate existence of any necessary metrics docGen changes during pull request.
 */
@ThreadSafe
@PublicApi
public final class MetricsDocValidator extends DocValidator {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsDocValidator.class);
  private static final String CSV_FILE_HEADER = "metricName,metricType";

  private MetricsDocValidator() {
  } // prevent instantiation

  /**
   * Validates configuration files.
   *
   * @return returns number of deltas
   */
  public static Integer validate() throws IOException {
    // Gets and sorts the metric keys
    List<MetricKey> defaultKeys = new ArrayList<>(MetricKey.allMetricKeys());
    Collections.sort(defaultKeys);

    Set<String> metricCategories = defaultKeys.stream()
            .map(key -> key.getName().split("\\.")[0].toLowerCase())
            .collect(Collectors.toSet());

    // Map from metric key prefix to metric category
    Map<String, String> metricTypeMap = new HashMap<>();
    for (MetricsSystem.InstanceType type : MetricsSystem.InstanceType.values()) {
      String typeStr = type.toString();
      String category = typeStr.toLowerCase();
      metricTypeMap.put(typeStr, category);
    }

    List<String> fileNamesCSV = new ArrayList<>();
    List<String> fileNamesYML = new ArrayList<>();

    for (String category : metricCategories) {
      fileNamesCSV.add(new String(category + "-metrics." + CSV_SUFFIX));
      fileNamesYML.add(new String(category + "-metrics." + YML_SUFFIX));
    }

    Integer numDeltas = 0;
    numDeltas += validateFiles(defaultKeys, CSV_FILE_DIR, fileNamesCSV, metricTypeMap);
    numDeltas += validateFiles(defaultKeys, YML_FILE_DIR, fileNamesYML, metricTypeMap);

    return numDeltas;
  }

  private static Integer validateFiles(List<MetricKey> defaultKeys, String fileDir,
                                       List<String> fileNames, Map<String, String> metricTypeMap)
          throws IOException {

    Integer numDeltas = 0;
    String filePath = PathUtils.concatPath(Configuration.getString(PropertyKey.HOME), fileDir);

    for (String fileName : fileNames) {
      String key = fileName.substring(0, fileName.indexOf("metrics") - 1);

      List<String> revised = new ArrayList<String>();
      if (fileDir.equals(CSV_FILE_DIR)) {
        revised.add(CSV_FILE_HEADER);
      }
      for (MetricKey metricKey : defaultKeys) {
        String metricKeyString = metricKey.toString();

        String[] components = metricKeyString.split("\\.");
        if (components.length < 2) {
          throw new IOException(String
                  .format("The given metric key %s doesn't have two or more components",
                          metricKeyString));
        }
        if (!metricTypeMap.containsKey(components[0])) {
          throw new IOException(String
                  .format("The metric key %s starts with invalid instance type %s", metricKeyString,
                          components[0]));
        }
        String category = metricTypeMap.get(components[0]);
        String line;
        if (key.equals(category)) {
          if (fileDir.equals(CSV_FILE_DIR)) {
            line = String.format("%s,%s", metricKeyString, metricKey.getMetricType().toString());
          } else {
            line = String.format("%s:%n  '%s'", metricKeyString,
                    StringEscapeUtils.escapeHtml4(metricKey.getDescription().replace("'", "''")));
          }
          revised.addAll(Arrays.asList(line.split("\n")));
        }
      }
      numDeltas = 0;

      // Load original file into memory for comparison
      String fileFullPathName = PathUtils.concatPath(filePath, fileName);
      List<String> original = Files.readAllLines(Paths.get(fileFullPathName));

      numDeltas += generateDiff(original, revised, fileFullPathName);
    }
    return numDeltas;
  }
}
