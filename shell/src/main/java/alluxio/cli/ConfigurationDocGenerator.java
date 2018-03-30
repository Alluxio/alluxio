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

package alluxio.cli;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * CSV_FILE_DIR utility to generate property keys to csv files.
 */
@ThreadSafe
public final class ConfigurationDocGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationDocGenerator.class);
  private static final String CSV_FILE_DIR = "docs/_data/table/";
  private static final String YML_FILE_DIR = "docs/_data/table/en/";
  public static final String CSV_FILE_HEADER = "propertyName,defaultValue";

  private ConfigurationDocGenerator() {} // prevent instantiation

  /**
   * Writes property key to csv files.
   *
   * @param defaultKeys Collection which is from PropertyKey DEFAULT_KEYS_MAP.values()
   * @param filePath    path for csv files
   */
  static void writeCSVFile(Collection<? extends PropertyKey> defaultKeys, String filePath)
      throws IOException {
    if (defaultKeys.size() == 0) {
      return;
    }

    FileWriter fileWriter;
    Closer closer = Closer.create();
    String[] fileNames = {"user-configuration.csv", "master-configuration.csv",
        "worker-configuration.csv", "security-configuration.csv",
        "key-value-configuration.csv", "common-configuration.csv",
        "cluster-management-configuration.csv"};

    try {
      // HashMap for FileWriter per each category
      Map<String, FileWriter> fileWriterMap = new HashMap<>();
      for (String fileName : fileNames) {
        fileWriter = new FileWriter(PathUtils.concatPath(filePath, fileName));
        // Write the CSV file header and line separator after the header
        fileWriter.append(CSV_FILE_HEADER + "\n");
        //put fileWriter
        String key = fileName.substring(0, fileName.indexOf("configuration") - 1);
        fileWriterMap.put(key, fileWriter);
        //register file writer
        closer.register(fileWriter);
      }

      // Sort defaultKeys
      List<PropertyKey> dfkeys = new ArrayList<>(defaultKeys);
      Collections.sort(dfkeys);

      for (PropertyKey iteratorPK : dfkeys) {
        String pKey = iteratorPK.toString();
        String value;
        if (iteratorPK.getDefaultValue() == null) {
          value = "";
        } else {
          value = iteratorPK.getDefaultValue();
        }

        // Write property key and default value to CSV
        String keyValueStr = pKey + "," + value + "\n";
        if (pKey.startsWith("alluxio.user.")) {
          fileWriter = fileWriterMap.get("user");
        } else if (pKey.startsWith("alluxio.master.")) {
          fileWriter = fileWriterMap.get("master");
        } else if (pKey.startsWith("alluxio.worker.")) {
          fileWriter = fileWriterMap.get("worker");
        } else if (pKey.startsWith("alluxio.security.")) {
          fileWriter = fileWriterMap.get("security");
        } else if (pKey.startsWith("alluxio.keyvalue.")) {
          fileWriter = fileWriterMap.get("key-value");
        } else if (pKey.startsWith("alluxio.integration")) {
          fileWriter = fileWriterMap.get("cluster-management");
        } else {
          fileWriter = fileWriterMap.get("common");
        }
        fileWriter.append(keyValueStr);
      }

      LOG.info("Property Key CSV files were created successfully.");
    } catch (Exception e) {
      throw closer.rethrow(e);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Error while flushing/closing Property Key CSV FileWriter", e);
      }
    }
  }

  /**
   * Writes description of property key to yml files.
   *
   * @param defaultKeys Collection which is from PropertyKey DEFAULT_KEYS_MAP.values()
   * @param filePath path for csv files
   */
  static void writeYMLFile(Collection<? extends PropertyKey> defaultKeys, String filePath)
      throws IOException {
    if (defaultKeys.size() == 0) {
      return;
    }

    FileWriter fileWriter;
    Closer closer = Closer.create();
    String[] fileNames = {"user-configuration.yml", "master-configuration.yml",
        "worker-configuration.yml", "security-configuration.yml",
        "key-value-configuration.yml", "common-configuration.yml",
        "cluster-management-configuration.yml"
    };

    try {
      // HashMap for FileWriter per each category
      Map<String, FileWriter> fileWriterMap = new HashMap<>();
      for (String fileName : fileNames) {
        fileWriter = new FileWriter(PathUtils.concatPath(filePath, fileName));
        //put fileWriter
        String key = fileName.substring(0, fileName.indexOf("configuration") - 1);
        fileWriterMap.put(key, fileWriter);
        //register file writer
        closer.register(fileWriter);
      }

      // Sort defaultKeys
      List<PropertyKey> dfkeys = new ArrayList<>(defaultKeys);
      Collections.sort(dfkeys);

      for (PropertyKey iteratorPK : dfkeys) {
        String pKey = iteratorPK.toString();

        // Puts descriptions in single quotes to avoid having to escaping reserved characters.
        // Still needs to escape single quotes with double single quotes.
        String description = iteratorPK.getDescription().replace("'", "''");

        // Write property key and default value to yml files
        if (iteratorPK.isIgnoredSiteProperty()) {
          description += " Note: This property must be specified as a JVM property; "
              + "it is not accepted in alluxio-site.properties.";
        }
        String keyValueStr = pKey + ":\n  '" + description + "'\n";
        if (pKey.startsWith("alluxio.user.")) {
          fileWriter = fileWriterMap.get("user");
        } else if (pKey.startsWith("alluxio.master.")) {
          fileWriter = fileWriterMap.get("master");
        } else if (pKey.startsWith("alluxio.worker.")) {
          fileWriter = fileWriterMap.get("worker");
        } else if (pKey.startsWith("alluxio.security.")) {
          fileWriter = fileWriterMap.get("security");
        } else if (pKey.startsWith("alluxio.keyvalue.")) {
          fileWriter = fileWriterMap.get("key-value");
        } else if (pKey.startsWith("alluxio.integration.")) {
          fileWriter = fileWriterMap.get("cluster-management");
        } else {
          fileWriter = fileWriterMap.get("common");
        }
        fileWriter.append(keyValueStr);
      }

      LOG.info("YML files for description of Property Keys were created successfully.");
    } catch (Exception e) {
      throw closer.rethrow(e);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Error while flushing/closing YML files for description of Property Keys "
            + "FileWriter", e);
      }
    }
  }

  /**
   * Main entry for this util class.
   *
   * @param args arguments for command line
   */
  public static void main(String[] args) throws IOException {
    // Trigger classloading of Configuration so that PropertyKey defaults are filled out.
    // TODO(adit): Remove this when we stop adding default property keys from Configuration.
    Configuration.containsKey(PropertyKey.CONF_DIR);
    Set<PropertyKey> defaultKeys = new HashSet<>(PropertyKey.defaultKeys());
    defaultKeys.removeIf(key -> key.isHidden());
    String homeDir = Configuration.get(PropertyKey.HOME);
    // generate CSV files
    String filePath = PathUtils.concatPath(homeDir, CSV_FILE_DIR);
    writeCSVFile(defaultKeys, filePath);
    // generate YML files
    filePath = PathUtils.concatPath(homeDir, YML_FILE_DIR);
    writeYMLFile(defaultKeys, filePath);
  }
}
