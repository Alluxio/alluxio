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

import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
//import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Comparator;

/**
 * A utility to generate property keys to csv files.
 */
@ThreadSafe
public final class ConfigurationDocGenerator {
  public static final String CSV_FILE_HEADER = "propertyName,defaultValue";
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationDocGenerator.class);

  private ConfigurationDocGenerator() {
  } // prevent instantiation

  /**
   * Writes property key to csv files.
   *
   * @param defaultKeys Collection which is from PropertyKey DEFAULT_KEYS_MAP.values()
   * @param filePath    path for csv files
   */
  public static void writeCSVFile(Collection<? extends PropertyKey> defaultKeys, String filePath)
      throws IOException {
    if (defaultKeys.size() == 0) {
      return;
    }

    FileWriter fileWriter;

    // HashMap for FileWriter per each category
    Map<String, FileWriter> fileWriterMap = new HashMap<>();
    fileWriterMap.put("user", new FileWriter(PathUtils.concatPath(filePath,
        "user-configuration.csv")));
    fileWriterMap.put("master", new FileWriter(PathUtils.concatPath(filePath,
        "master-configuration.csv")));
    fileWriterMap.put("worker", new FileWriter(PathUtils.concatPath(filePath,
        "worker-configuration.csv")));
    fileWriterMap.put("security", new FileWriter(PathUtils.concatPath(filePath,
        "security-configuration.csv")));
    fileWriterMap.put("keyvalue", new FileWriter(PathUtils.concatPath(filePath,
        "key-value-configuration.csv")));
    fileWriterMap.put("common", new FileWriter(PathUtils.concatPath(filePath,
        "common-configuration.csv")));

    Closer closer = Closer.create();
    try {
      for (Map.Entry<String, FileWriter> entry : fileWriterMap.entrySet()) {
        // Write the CSV file header
        entry.getValue().append(CSV_FILE_HEADER);
        // Add a new line separator after the header
        entry.getValue().append("\n");
        // register file writer
        closer.register(entry.getValue());
      }

      // Sort defaultKeys
      Comparator<PropertyKey> pC = new PropertyKeyComparator();
      List<PropertyKey> dfkeys = new ArrayList<>(defaultKeys);
      Collections.sort(dfkeys, pC);

      for (PropertyKey iteratorPK : dfkeys) {
        String pKey = iteratorPK.toString();
        String value;
        PropertyKey pk = PropertyKey.fromString(pKey);
        if (pk.getDefaultValue() == null) {
          value = "";
        } else {
          value = pk.getDefaultValue();
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
          fileWriter = fileWriterMap.get("keyvalue");
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
  public static void writeYMLFile(Collection<? extends PropertyKey> defaultKeys, String filePath)
      throws IOException {
    if (defaultKeys.size() == 0) {
      return;
    }

    FileWriter fileWriter;

    // HashMap for FileWriter per each category
    Map<String, FileWriter> fileWriterMap = new HashMap<>();
    fileWriterMap.put("user", new FileWriter(PathUtils.concatPath(filePath,
        "user-configuration.yml")));
    fileWriterMap.put("master", new FileWriter(PathUtils.concatPath(filePath,
        "master-configuration.yml")));
    fileWriterMap.put("worker", new FileWriter(PathUtils.concatPath(filePath,
        "worker-configuration.yml")));
    fileWriterMap.put("security", new FileWriter(PathUtils.concatPath(filePath,
        "security-configuration.yml")));
    fileWriterMap.put("keyvalue", new FileWriter(PathUtils.concatPath(filePath,
        "key-value-configuration.yml")));
    fileWriterMap.put("common", new FileWriter(PathUtils.concatPath(filePath,
        "common-configuration.yml")));

    Closer closer = Closer.create();
    try {
      for (Map.Entry<String, FileWriter> entry : fileWriterMap.entrySet()) {
        // register file writer
        closer.register(entry.getValue());
      }

      // Sort defaultKeys
      Comparator<PropertyKey> pC = new PropertyKeyComparator();
      List<PropertyKey> dfkeys = new ArrayList<>(defaultKeys);
      Collections.sort(dfkeys, pC);

      for (PropertyKey iteratorPK : dfkeys) {
        String pKey = iteratorPK.toString();
        String description = iteratorPK.getDescription();

        // Write property key and default value to yml files
        String keyValueStr = pKey + ":\n  " + description + "\n";
        if (pKey.startsWith("alluxio.user.")) {
          fileWriter = fileWriterMap.get("user");
        } else if (pKey.startsWith("alluxio.master.")) {
          fileWriter = fileWriterMap.get("master");
        } else if (pKey.startsWith("alluxio.worker.")) {
          fileWriter = fileWriterMap.get("worker");
        } else if (pKey.startsWith("alluxio.security.")) {
          fileWriter = fileWriterMap.get("security");
        } else if (pKey.startsWith("alluxio.keyvalue.")) {
          fileWriter = fileWriterMap.get("keyvalue");
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
   * Function for main entry.
   *
   * @param args arguments for command line
   */
  public static void main(String[] args) throws IOException {
    Collection<? extends PropertyKey> defaultKeys = PropertyKey.defaultKeys();
    String userDir = System.getProperty("user.dir");
    String location = userDir.substring(0, userDir.indexOf("alluxio") + 7);
    String filePath = PathUtils.concatPath(location, "/docs/_data/table/");
    // generate CSV files
    writeCSVFile(defaultKeys, filePath);
    // generate YML files
    filePath = PathUtils.concatPath(filePath, "/en/");
    writeYMLFile(defaultKeys, filePath);
  }

  /**
   * PropertyKey Comparator inner class.
   */
  private static final class PropertyKeyComparator implements Comparator<PropertyKey> {
    private PropertyKeyComparator() {
    } // prevent instantiation

    /**
     * Compare two PropertyKeys.
     *
     * @param pk1 PropertyKey object
     * @param pk2 PropertyKey object
     */
    @Override
    public int compare(PropertyKey pk1, PropertyKey pk2) {
      String pk1Name = pk1.toString();
      String pk2Name = pk2.toString();
      return pk1Name.compareTo(pk2Name);
    }
  }
}
