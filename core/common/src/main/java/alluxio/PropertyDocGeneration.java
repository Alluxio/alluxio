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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility to generate property keys to csv files.
 */
@ThreadSafe
public final class PropertyDocGeneration {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyDocGeneration.class);

  private PropertyDocGeneration() {
  }

  /**
   * Writes property key to csv files.
   *
   * @param defaultVals DEFAULT_VALUES HashMap which is from PropertyKey
   * @param filePath file file for csv files
   */
  public static void writeCSVFile(HashMap<PropertyKey, Object> defaultVals, String filePath) {
    if (defaultVals.size() == 0) {
      return;
    }

    //CSV file header
    String fileHeader = "propertyName,defaultValue";
    FileWriter fileWriter;

    //HashMap for CSV file names
    Map<String, String> fileNames = new HashMap<>();
    fileNames.put("user", filePath + "user-configuration.csv");
    fileNames.put("master", filePath + "master-configuration.csv");
    fileNames.put("worker", filePath + "worker-configuration.csv");
    fileNames.put("security", filePath + "security-configuration.csv");
    fileNames.put("keyvalue", filePath + "key-value-configuration.csv");
    fileNames.put("common", filePath + "common-configuration.csv");

    //HashMap for FileWriter per each category
    Map<String, FileWriter> fileWriterMap = new HashMap<>();
    try {
      for (Map.Entry<String, String> entry : fileNames.entrySet()) {
        fileWriter = new FileWriter(entry.getValue());
        //Write the CSV file header
        fileWriter.append(fileHeader);
        //Add a new line separator after the header
        fileWriter.append("\n");
        //put fileWriter
        fileWriterMap.put(entry.getKey(), fileWriter);
      }

      for (Map.Entry<PropertyKey, Object> entry : defaultVals.entrySet()) {
        String pKey = entry.getKey().toString();
        String value;
        if (entry.getValue() == null) {
          value = "";
        } else {
          value = entry.getValue().toString();
        }

        //Persist property key and default value to CSV
        String keyValueStr = pKey + "," + value + "\n";
        if (pKey.contains(".user.")) {
          fileWriter = fileWriterMap.get("user");
          fileWriter.append(keyValueStr);
        } else if (pKey.contains(".master.")) {
          fileWriter = fileWriterMap.get("master");
          fileWriter.append(keyValueStr);
        } else if (pKey.contains(".worker.")) {
          fileWriter = fileWriterMap.get("worker");
          fileWriter.append(keyValueStr);
        } else if (pKey.contains(".security.")) {
          fileWriter = fileWriterMap.get("security");
          fileWriter.append(keyValueStr);
        } else if (pKey.contains(".keyvalue.")) {
          fileWriter = fileWriterMap.get("keyvalue");
          fileWriter.append(keyValueStr);
        } else {
          fileWriter = fileWriterMap.get("common");
          fileWriter.append(keyValueStr);
        }
      }

      LOG.info("Property Key CSV files were created successfully.");
    } catch (Exception e) {
      LOG.error("Error in Property Key CSV FileWriter", e);
    } finally {
      try {
        for (Map.Entry<String, FileWriter> entry : fileWriterMap.entrySet()) {
          entry.getValue().flush();
          entry.getValue().close();
        }
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
  public static void main(String[] args) throws IllegalAccessException, NoSuchFieldException {
    PropertyKey propertyKey = new PropertyKey("");
    Class pk = propertyKey.getClass();
    try {
      Field defaultValues = pk.getDeclaredField("DEFAULT_VALUES");
      defaultValues.setAccessible(true);
      String userDir = System.getProperty("user.dir");
      String location = userDir.substring(0, userDir.indexOf("alluxio") + 7);
      String filePath = location + "/docs/_data/table/";
      writeCSVFile((HashMap<PropertyKey, Object>) defaultValues.get(propertyKey), filePath);
    } catch (Exception e) {
      LOG.error("No Such Field!", e);
    }
  }
}
