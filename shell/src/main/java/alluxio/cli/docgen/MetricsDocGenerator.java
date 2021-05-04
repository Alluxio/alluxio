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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Objects;
import com.google.common.io.Closer;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Generates metric key information in docs.
 */
@ThreadSafe
@PublicApi
public final class MetricsDocGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsDocGenerator.class);
  private static final String[] CATEGORIES =
      new String[]{"cluster", "master", "worker", "client", "fuse"};
  private static final String CSV_FILE_DIR = "docs/_data/table/";
  private static final String YML_FILE_DIR = "docs/_data/table/en/";
  private static final String CSV_SUFFIX = "csv";
  private static final String YML_SUFFIX = "yml";
  private static final String CSV_FILE_HEADER = "metricName,metricType";
  public static final String TEMP_PREFIX = "temp-";

  /**
   * Compare the temp files and the committed files if validate flag is set.
   *
   * @param folder the location of the files to be compared
   * @param suffix the file types (CSV/YML)
   * @param category the category in the file names
   * @param hasDiff a boolean indicating has there been changes
   *
   * @return a boolean indicating if there is anything difference
   * between the temp files and committed files
   */
  public static boolean compareFiles(String folder, String suffix, String category,
                                     boolean hasDiff) throws IOException {
    if (!FileUtils.contentEquals(new File(folder, (TEMP_PREFIX + category + "-metrics." + suffix)),
        new File(folder, (category + "-metrics." + suffix)))) {
      hasDiff = true;
      System.out.println("Metrics file " + category + "-metrics." + suffix + " changed.");
    }
    File tempFile = new File(folder, TEMP_PREFIX + category + "-metrics." + suffix);
    tempFile.delete();
    return hasDiff;
  }

  /**
   * Writes the supported files for metrics system docs.
   * @param validate validate flag
   */
  public static void generate(boolean validate) throws IOException {
    // Gets and sorts the metric keys
    List<MetricKey> defaultKeys = new ArrayList<>(MetricKey.allMetricKeys());
    Collections.sort(defaultKeys);

    String homeDir = new InstancedConfiguration(ConfigurationUtils.defaults())
        .get(PropertyKey.HOME);

    // Map from metric key prefix to metric category
    Map<String, String> metricTypeMap = new HashMap<>();
    for (MetricsSystem.InstanceType type : MetricsSystem.InstanceType.values()) {
      String typeStr = type.toString();
      String category = typeStr.toLowerCase();
      metricTypeMap.put(typeStr, category);
    }
    String csvFolder = PathUtils.concatPath(homeDir, CSV_FILE_DIR);
    String ymlFolder = PathUtils.concatPath(homeDir, YML_FILE_DIR);
    try (Closer closer = Closer.create()) {
      Map<FileWriterKey, FileWriter> fileWriterMap = new HashMap<>();
      FileWriter csvFileWriter;
      FileWriter ymlFileWriter;
      for (String category : CATEGORIES) {
        if (validate) {
          csvFileWriter = new FileWriter(PathUtils
              .concatPath(csvFolder, (TEMP_PREFIX + category + "-metrics." + CSV_SUFFIX)));
          csvFileWriter.append(CSV_FILE_HEADER + "\n");
          ymlFileWriter = new FileWriter(PathUtils
              .concatPath(ymlFolder, (TEMP_PREFIX + category + "-metrics." + YML_SUFFIX)));
        } else {
          csvFileWriter = new FileWriter(PathUtils
              .concatPath(csvFolder, (category + "-metrics." + CSV_SUFFIX)));
          csvFileWriter.append(CSV_FILE_HEADER + "\n");
          ymlFileWriter = new FileWriter(PathUtils
              .concatPath(ymlFolder, (category + "-metrics." + YML_SUFFIX)));
        }
        fileWriterMap.put(new FileWriterKey(category, CSV_SUFFIX), csvFileWriter);
        fileWriterMap.put(new FileWriterKey(category, YML_SUFFIX), ymlFileWriter);
        //register file writer
        closer.register(csvFileWriter);
        closer.register(ymlFileWriter);
      }

      for (MetricKey metricKey : defaultKeys) {
        String key = metricKey.toString();
        String[] components = key.split("\\.");
        if (components.length < 2) {
          throw new IOException(String
              .format("The given metric key %s doesn't have two or more components", key));
        }
        if (!metricTypeMap.containsKey(components[0])) {
          throw new IOException(String
              .format("The metric key %s starts with invalid instance type %s", key,
                  components[0]));
        }
        csvFileWriter = fileWriterMap.get(
            new FileWriterKey(metricTypeMap.get(components[0]), CSV_SUFFIX));
        ymlFileWriter = fileWriterMap.get(
            new FileWriterKey(metricTypeMap.get(components[0]), YML_SUFFIX));
        csvFileWriter.append(String.format("%s,%s%n", key, metricKey.getMetricType().toString()));
        ymlFileWriter.append(String.format("%s:%n  '%s'%n",
            key, StringEscapeUtils.escapeHtml4(metricKey.getDescription().replace("'", "''"))));
      }
    }
    if (validate) {
      boolean hasDiff = false;

      for (String category : CATEGORIES) {
        hasDiff = compareFiles(csvFolder, CSV_SUFFIX, category, hasDiff);
        hasDiff = compareFiles(ymlFolder, YML_SUFFIX, category, hasDiff);
      }
      if (!hasDiff) {
        System.out.println("No change in metric file detected.");
      }
    }
    LOG.info("Metrics CSV/YML files were created successfully.");
  }

  /**
   * The key for a file writer.
   */
  public static class FileWriterKey {
    String mCategory;
    String mFileType;

    /**
     * Constructs a {@link FileWriterKey}.
     *
     * @param category the metric key category
     * @param fileType the file type to write to
     */
    public FileWriterKey(String category, String fileType) {
      mCategory = category;
      mFileType = fileType;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileWriterKey)) {
        return false;
      }
      FileWriterKey that = (FileWriterKey) o;

      return Objects.equal(mCategory, that.mCategory)
          && Objects.equal(mFileType, that.mFileType);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mCategory, mFileType);
    }
  }

  private MetricsDocGenerator() {} // prevent instantiation
}
