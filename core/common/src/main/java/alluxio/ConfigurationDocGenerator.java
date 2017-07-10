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

package alluxio;

import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.io.Closer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A utility to generate property keys to csv files.
 */
@ThreadSafe
public final class ConfigurationDocGenerator {
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

    //CSV file header
    String fileHeader = "propertyName,defaultValue";
    FileWriter fileWriter;

    //HashMap for CSV file names
    Map<String, String> fileNames = new HashMap<>();
    fileNames.put("user", PathUtils.concatPath(filePath, "user-configuration.csv"));
    fileNames.put("master", PathUtils.concatPath(filePath, "master-configuration.csv"));
    fileNames.put("worker", PathUtils.concatPath(filePath, "worker-configuration.csv"));
    fileNames.put("security", PathUtils.concatPath(filePath, "security-configuration.csv"));
    fileNames.put("keyvalue", PathUtils.concatPath(filePath, "key-value-configuration.csv"));
    fileNames.put("common", PathUtils.concatPath(filePath, "common-configuration.csv"));

    //HashMap for FileWriter per each category
    Map<String, FileWriter> fileWriterMap = new HashMap<>();
    Closer closer = Closer.create();
    try {
      for (Map.Entry<String, String> entry : fileNames.entrySet()) {
        fileWriter = new FileWriter(entry.getValue());
        //Write the CSV file header
        fileWriter.append(fileHeader);
        //Add a new line separator after the header
        fileWriter.append("\n");
        //put fileWriter
        fileWriterMap.put(entry.getKey(), fileWriter);
        //register file writer
        closer.register(fileWriter);
      }

      for (PropertyKey iteratorPK : defaultKeys) {
        String pKey = iteratorPK.toString();
        String value;
        PropertyKey pk = new PropertyKey(pKey);
        if (pk.getDefaultValue() == null) {
          value = "";
        } else {
          value = pk.getDefaultValue();
        }

        //Persist property key and default value to CSV
        String keyValueStr = pKey + "," + value + "\n";
        if (pKey.contains(".user.")) {
          fileWriter = fileWriterMap.get("user");
        } else if (pKey.contains(".master.")) {
          fileWriter = fileWriterMap.get("master");
        } else if (pKey.contains(".worker.")) {
          fileWriter = fileWriterMap.get("worker");
        } else if (pKey.contains(".security.")) {
          fileWriter = fileWriterMap.get("security");
        } else if (pKey.contains(".keyvalue.")) {
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
        for (Map.Entry<String, FileWriter> entry : fileWriterMap.entrySet()) {
          entry.getValue().flush();
          entry.getValue().close();
        }
        closer.close();
      } catch (IOException e) {
        LOG.error("Error while flushing/closing Property Key CSV FileWriter", e);
      }
    }
  }

  /**
   * Function for main entry.
   *
   * @param args arguments for command line
   */
  public static void main(String[] args) throws IllegalAccessException, IOException {
    Collection<? extends PropertyKey> defaultKeys = PropertyKey.defaultKeys();
    String userDir = System.getProperty("user.dir");
    String location = userDir.substring(0, userDir.indexOf("alluxio") + 7);
    String filePath = PathUtils.concatPath(location, "/docs/_data/table/");
    writeCSVFile(defaultKeys, filePath);
  }
}
