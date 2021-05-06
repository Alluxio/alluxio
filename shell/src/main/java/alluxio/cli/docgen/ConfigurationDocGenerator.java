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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * CSV_FILE_DIR utility to generate property keys to csv files.
 */
@ThreadSafe
@PublicApi
public final class ConfigurationDocGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationDocGenerator.class);
  private static final String CSV_FILE_DIR = "docs/_data/table/";
  private static final String YML_FILE_DIR = "docs/_data/table/en/";
  public static final String CSV_FILE_HEADER = "propertyName,defaultValue";
  private static final String TEMP_PREFIX = "temp-";
  private static final String CSV_SUFFIX = "csv";
  private static final String YML_SUFFIX = "yml";

  private static final String[] CSV_FILE_NAMES = {
      "cluster-management-configuration.csv",
      "common-configuration.csv",
      "master-configuration.csv",
      "security-configuration.csv",
      "user-configuration.csv",
      "worker-configuration.csv"
  };

  private static final String[] YML_FILE_NAMES = {
      "cluster-management-configuration.yml",
      "common-configuration.yml",
      "master-configuration.yml",
      "security-configuration.yml",
      "user-configuration.yml",
      "worker-configuration.yml"
  };

  private ConfigurationDocGenerator() {} // prevent instantiation

  /**
   * Create the FileWriter object based on if validate flag is set to true.
   *
   * @param filePath path for the csv/yml file
   * @param fileName name of the csv/yml file
   * @param validate the validate flag of the command
   * @return a FileWriter associated with the file
   */
  public static FileWriter createFileWriter(String filePath, String fileName, boolean validate)
      throws IOException {
    if (validate) {
      String fileNameTemp = TEMP_PREFIX.concat(fileName);
      return new FileWriter(PathUtils.concatPath(filePath, fileNameTemp));
    } else {
      return new FileWriter(PathUtils.concatPath(filePath, fileName));
    }
  }

  /**
   * Writes property key to csv files.
   *
   * @param defaultKeys Collection which is from PropertyKey DEFAULT_KEYS_MAP.values()
   * @param filePath    path for csv files
   * @param validate    validate flag
   */
  @VisibleForTesting
  public static void handleCSVFile(Collection<? extends PropertyKey> defaultKeys,
                                  String filePath, boolean validate) throws IOException {
    if (defaultKeys.size() == 0) {
      return;
    }

    FileWriter fileWriter;
    Closer closer = Closer.create();
    try {
      // HashMap for FileWriter per each category
      Map<String, FileWriter> fileWriterMap = new HashMap<>();
      for (String fileName : CSV_FILE_NAMES) {
        fileWriter = createFileWriter(filePath, fileName, validate);
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

      for (PropertyKey propertyKey : dfkeys) {
        String pKey = propertyKey.toString();
        String defaultDescription;
        if (propertyKey.getDefaultSupplier().get() == null) {
          defaultDescription = "";
        } else {
          defaultDescription = propertyKey.getDefaultSupplier().getDescription();
        }
        // Quote the whole description to escape characters such as commas.
        defaultDescription = String.format("\"%s\"", defaultDescription);

        // Write property key and default value to CSV
        String keyValueStr = pKey + "," + defaultDescription + "\n";
        if (pKey.startsWith("alluxio.user.")) {
          fileWriter = fileWriterMap.get("user");
        } else if (pKey.startsWith("alluxio.master.")) {
          fileWriter = fileWriterMap.get("master");
        } else if (pKey.startsWith("alluxio.worker.")) {
          fileWriter = fileWriterMap.get("worker");
        } else if (pKey.startsWith("alluxio.security.")) {
          fileWriter = fileWriterMap.get("security");
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
        if (validate) {
          compareFiles(CSV_FILE_NAMES, filePath, CSV_SUFFIX);
        }
      } catch (IOException e) {
        LOG.error("Error while flushing/closing Property Key CSV FileWriter " +
            "or compiling generated files to the existing files if validate flag is set", e);
      }
      if (validate) {
        for (String fileName : CSV_FILE_NAMES) {
          String fileNameTemp = TEMP_PREFIX.concat(fileName);
          File tempFile = new File(PathUtils.concatPath(filePath, fileNameTemp));
          if (tempFile.exists()) {
            tempFile.delete();
          }
        }
      }
    }
  }

  /**
   * Writes description of property key to yml files.
   *
   * @param defaultKeys Collection which is from PropertyKey DEFAULT_KEYS_MAP.values()
   * @param filePath path for csv files
   * @param validate validate flag
   */
  @VisibleForTesting
  public static void handleYMLFile(Collection<? extends PropertyKey> defaultKeys,
                                  String filePath, boolean validate)
      throws IOException {
    if (defaultKeys.size() == 0) {
      return;
    }

    FileWriter fileWriter;
    Closer closer = Closer.create();

    try {
      // HashMap for FileWriter per each category
      Map<String, FileWriter> fileWriterMap = new HashMap<>();
      for (String fileName : YML_FILE_NAMES) {
        fileWriter = createFileWriter(filePath, fileName, validate);
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
          description += " Note: overwriting this property will only work when it is passed as a "
              + "JVM system property (e.g., appending \"-D" + iteratorPK + "\"=<NEW_VALUE>\" to "
              + "$ALLUXIO_JAVA_OPTS). Setting it in alluxio-site.properties will not work.";
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
        } else if (pKey.startsWith("alluxio.integration.")) {
          fileWriter = fileWriterMap.get("cluster-management");
        } else {
          fileWriter = fileWriterMap.get("common");
        }
        fileWriter.append(StringEscapeUtils.escapeHtml4(keyValueStr));
      }

      LOG.info("YML files for description of Property Keys were created successfully.");
    } catch (Exception e) {
      throw closer.rethrow(e);
    } finally {
      try {
        closer.close();
        if (validate) {
          compareFiles(YML_FILE_NAMES, filePath, YML_SUFFIX);
        }
      } catch (IOException e) {
        LOG.error("Error while flushing/closing YML files for description of Property Keys "
            + "FileWriter or compiling generated files to the existing files if validate flag is set", e);
      }
      if (validate) {
        for (String fileName : YML_FILE_NAMES) {
          String fileNameTemp = TEMP_PREFIX.concat(fileName);
          File tempFile = new File(PathUtils.concatPath(filePath, fileNameTemp));
          if (tempFile.exists()) {
            tempFile.delete();
          }
        }
      }
    }
  }

  /**
   * Helper method that compare the temp file and the committed file when validate flag is used.
   *
   * @param fileNames the name of the file
   * @param filePath the path to the file
   * @param fileType the type of the file that we are comparing (CSV/YML)
   */
  public static void compareFiles(String[] fileNames, String filePath, String fileType)
      throws IOException {
    boolean hasDiff = false;

    for (String fileName : fileNames) {
      String fileNameTemp = TEMP_PREFIX.concat(fileName);
      File committedFile = new File(filePath, fileName);
      File tempFile = new File(filePath, fileNameTemp);
      if (!committedFile.exists()) {
        throw new IOException("Committed file does not exists");
      }
      if (!tempFile.exists()) {
        throw new IOException("Temporary generated file does not exists");
      }
      if (!FileUtils.contentEquals(committedFile, tempFile)) {
        hasDiff = true;
        System.out.printf("Config file %s changed.%n", fileName);
      }
    }
    if (!hasDiff) {
      System.out.println("No change in config " + fileType + " files detected.");
    }
  }

  /**
   * Generates the configuration docs.
   * @param validate validate flag
   */
  public static void generate(boolean validate) throws IOException {
    Collection<? extends PropertyKey> defaultKeys = PropertyKey.defaultKeys();
    defaultKeys.removeIf(key -> key.isHidden());
    String homeDir = new InstancedConfiguration(ConfigurationUtils.defaults())
        .get(PropertyKey.HOME);
    // generate CSV files
    String filePath = PathUtils.concatPath(homeDir, CSV_FILE_DIR);
    handleCSVFile(defaultKeys, filePath, validate);

    // generate YML files
    filePath = PathUtils.concatPath(homeDir, YML_FILE_DIR);
    handleYMLFile(defaultKeys, filePath, validate);
  }
}
