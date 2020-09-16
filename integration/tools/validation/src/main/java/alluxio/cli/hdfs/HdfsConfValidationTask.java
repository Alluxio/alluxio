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

package alluxio.cli.hdfs;

import alluxio.cli.AbstractValidationTask;
import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.cli.ApplicableUfsType;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.util.io.PathUtils;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Class for validating HDFS-related configurations.
 * Validates accessibility and correctness of the configuration files passed to Alluxio.
 */
@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
public class HdfsConfValidationTask extends AbstractValidationTask {
  public static final String SEPARATOR = ":";
  protected final AlluxioConfiguration mConf;
  final String mPath;
  // loaded by loadHdfsConfig()
  Map<String, String> mCoreConf = null;
  Map<String, String> mHdfsConf = null;
  StringBuilder mMsg = new StringBuilder();
  StringBuilder mAdvice = new StringBuilder();

  /**
   * Creates a new instance of {@link HdfsConfValidationTask}
   * for validating HDFS configuration.
   *
   * @param path the UFS path
   * @param conf the UFS configuration
   */
  public HdfsConfValidationTask(String path, AlluxioConfiguration conf) {
    mPath = path;
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateHdfsConf";
  }

  protected ValidationTaskResult loadHdfsConfig() {
    Pair<String, String> clientConfFiles = getHdfsConfPaths();
    String coreConfPath = clientConfFiles.getFirst();
    String hdfsConfPath = clientConfFiles.getSecond();

    mCoreConf = accessAndParseConf("core-site.xml", coreConfPath);
    mHdfsConf = accessAndParseConf("hdfs-site.xml", hdfsConfPath);
    ValidationUtils.State state = (mCoreConf != null) && (mHdfsConf != null)
            ? ValidationUtils.State.OK : ValidationUtils.State.FAILED;
    return new ValidationTaskResult(state, getName(), mMsg.toString(), mAdvice.toString());
  }

  protected Pair<String, String> getHdfsConfPaths() {
    // If ServerConfiguration does not contain the key, then a {@link RuntimeException} will be
    // thrown before calling the {@link String#split} method.
    String[] clientHadoopConfFilePaths =
            mConf.get(PropertyKey.UNDERFS_HDFS_CONFIGURATION).split(SEPARATOR);
    String clientCoreSiteFilePath = null;
    String clientHdfsSiteFilePath = null;
    for (String path : clientHadoopConfFilePaths) {
      if (path.contains("core-site.xml")) {
        clientCoreSiteFilePath = path;
      } else if (path.contains("hdfs-site.xml")) {
        clientHdfsSiteFilePath = path;
      }
    }
    return new Pair<>(clientCoreSiteFilePath, clientHdfsSiteFilePath);
  }

  @Override
  public ValidationTaskResult validate(Map<String, String> optionsMap) {
    if (!ValidationUtils.isHdfsScheme(mPath)) {
      mMsg.append(String.format(
              "UFS path %s is not HDFS. Skipping validation for HDFS properties.%n", mPath));
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              mMsg.toString(), mAdvice.toString());
    }

    ValidationTaskResult loadConfig = loadHdfsConfig();
    if (loadConfig.getState() != ValidationUtils.State.OK) {
      // If failed to load config files, abort
      return loadConfig;
    }

    // no conflicts between these two
    return checkConflicts();
  }

  // Verify core-site.xml and hdfs.site.xml has no conflicts
  // mCoreConf and mHdfsConf are verified to be non-null as precondition
  protected ValidationTaskResult checkConflicts() {
    ValidationUtils.State state = ValidationUtils.State.OK;
    for (Map.Entry<String, String> entry : mCoreConf.entrySet()) {
      String k = entry.getKey();
      if (mHdfsConf.containsKey(k)) {
        String hdfsValue = mHdfsConf.get(k);
        String coreValue = mCoreConf.get(k);
        if (!hdfsValue.equals(coreValue)) {
          state = ValidationUtils.State.FAILED;
          mMsg.append(String.format("Property %s is %s in core-site.xml and %s in hdfs-site.xml",
                  k, coreValue, hdfsValue));
          mAdvice.append(String.format(
                  "Please fix the inconsistency for %s in core-site.xml and hdfs.xml.%n", k));
        }
      }
    }
    if (state == ValidationUtils.State.OK) {
      mMsg.append("core-site.xml and hdfs-site.xml are consistent.\n");
    }
    return new ValidationTaskResult(state, getName(), mMsg.toString(), mAdvice.toString());
  }

  @Nullable
  private Map<String, String> accessAndParseConf(String configName, String path) {
    if (path == null || path.isEmpty()) {
      mMsg.append(String.format("%s is not configured in Alluxio property %s%n", configName,
              PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      mAdvice.append(String.format("Please configure %s in %s%n", configName,
              PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      return null;
    }
    try {
      PathUtils.getPathComponents(path);
    } catch (InvalidPathException e) {
      mMsg.append(String.format("Invalid path %s in Alluxio property %s.%n", path,
              PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      mMsg.append(ValidationUtils.getErrorInfo(e));
      mAdvice.append(String.format("Please correct the path for %s in %s%n", configName,
              PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      return null;
    }
    HadoopConfigurationFileParser parser = new HadoopConfigurationFileParser();
    Map<String, String> properties = null;
    try {
      properties = parser.parseXmlConfiguration(path);
      mMsg.append(String.format("Successfully loaded %s. %n", path));
    } catch (ParserConfigurationException e) {
      mMsg.append(String.format("Failed to create instance of DocumentBuilder for file: %s. %s.%n",
              path, e.getMessage()));
      mMsg.append(ValidationUtils.getErrorInfo(e));
      mAdvice.append("Please check your configuration for javax.xml.parsers.DocumentBuilder.%n");
    } catch (IOException e) {
      mMsg.append(String.format("Failed to read %s. %s.%n", path, e.getMessage()));
      mMsg.append(ValidationUtils.getErrorInfo(e));
      mAdvice.append(String.format("Please check your %s.%n", path));
    } catch (SAXException e) {
      mMsg.append(String.format("Failed to parse %s. %s.%n", path, e.getMessage()));
      mMsg.append(ValidationUtils.getErrorInfo(e));
      mAdvice.append(String.format("Please check your %s.%n", path));
    }
    return properties;
  }
}
