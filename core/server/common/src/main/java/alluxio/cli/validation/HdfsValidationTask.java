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

package alluxio.cli.validation;

import alluxio.uri.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract class for validating HDFS-related configurations.
 */
public class HdfsValidationTask extends AbstractValidationTask {
  /** Name of the environment variable to store the path to Hadoop config directory. */
  protected static final String HADOOP_CONF_DIR_ENV_VAR = "HADOOP_CONF_DIR";

  protected static final Option HADOOP_CONF_DIR_OPTION =
      Option.builder("hadoopConfDir").required(false).hasArg(true)
      .desc("path to server-side hadoop conf dir").build();

  /**
   * Constructor of {@link HdfsValidationTask}.
   */
  public HdfsValidationTask() {}

  @Override
  public List<Option> getOptionList() {
    List<Option> opts = new ArrayList<>();
    opts.add(HADOOP_CONF_DIR_OPTION);
    return opts;
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    if (shouldSkip()) {
      return TaskResult.SKIPPED;
    }
    if (!validateHdfsSettingParity(optionsMap)) {
      return TaskResult.FAILED;
    }
    return TaskResult.OK;
  }

  protected boolean shouldSkip() {
    String scheme =
        new AlluxioURI(Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS)).getScheme();
    if (scheme == null || !scheme.startsWith("hdfs")) {
      System.out.format("Root underFS is not HDFS. Skipping validation for HDFS properties.%n");
      return true;
    }
    return false;
  }

  private boolean validateHdfsSettingParity(Map<String, String> optionsMap) {
    String serverHadoopConfDirPath;
    if (optionsMap.containsKey(HADOOP_CONF_DIR_OPTION.getOpt())) {
      serverHadoopConfDirPath = optionsMap.get(HADOOP_CONF_DIR_OPTION.getOpt());
    } else {
      serverHadoopConfDirPath = System.getenv(HADOOP_CONF_DIR_ENV_VAR);
    }
    if (serverHadoopConfDirPath == null) {
      System.out.println("Path to server-side hadoop configuration unspecified,"
          + " skipping validation for HDFS properties.");
      return true;
    }
    String serverCoreSiteFilePath = PathUtils.concatPath(serverHadoopConfDirPath, "/core-site.xml");
    String serverHdfsSiteFilePath = PathUtils.concatPath(serverHadoopConfDirPath, "/hdfs-site.xml");

    // If Configuration does not contain the key, then a {@link RuntimeException} will be thrown
    // before calling the {@link String#split} method.
    String[] clientHadoopConfFilePaths =
        Configuration.get(PropertyKey.UNDERFS_HDFS_CONFIGURATION).split(":");
    String clientCoreSiteFilePath = null;
    String clientHdfsSiteFilePath = null;
    for (String path : clientHadoopConfFilePaths) {
      try {
        String[] pathComponents = PathUtils.getPathComponents(path);
        if (pathComponents.length < 1) {
          continue;
        }
        if (pathComponents[pathComponents.length - 1].equals("core-site.xml")) {
          clientCoreSiteFilePath = path;
        } else if (pathComponents[pathComponents.length - 1].equals("hdfs-site.xml")) {
          clientHdfsSiteFilePath = path;
        }
      } catch (InvalidPathException e) {
        System.out.format("%s is an invalid path. Skip HDFS config parity check.%n", path);
        return true;
      }
    }
    if (clientCoreSiteFilePath == null || clientCoreSiteFilePath.isEmpty()) {
      System.out.println("Cannot locate the client-side core-site.xml,"
          + " skipping validation for HDFS properties.");
      return true;
    }
    if (clientHdfsSiteFilePath == null || clientHdfsSiteFilePath.isEmpty()) {
      System.out.println("Cannot locate the client-side hdfs-site.xml,"
          + " skipping validation for HDFS properties.");
      return true;
    }
    return compareConfigurations(clientCoreSiteFilePath, serverCoreSiteFilePath)
        && compareConfigurations(clientHdfsSiteFilePath, serverHdfsSiteFilePath);
  }

  private boolean compareConfigurations(String clientConfigFilePath, String serverConfigFilePath) {
    HadoopConfigurationFileParser parser = new HadoopConfigurationFileParser();
    Map<String, String> serverSiteProps = parser.parseXmlConfiguration(serverConfigFilePath);
    if (serverSiteProps == null) {
      System.err.format("Failed to parse server-side %s.%n", serverConfigFilePath);
      return false;
    }
    Map<String, String> clientSiteProps = parser.parseXmlConfiguration(clientConfigFilePath);
    if (clientSiteProps == null) {
      System.err.format("Failed to parse client-side %s.%n", clientConfigFilePath);
      return false;
    }
    boolean matches = true;
    for (Map.Entry<String, String> prop : clientSiteProps.entrySet()) {
      if (!serverSiteProps.containsKey(prop.getKey())) {
        matches = false;
        System.err.format("%s is configured in %s, but not configured in %s.%n",
            prop.getKey(), clientConfigFilePath, serverConfigFilePath);
      } else if (!prop.getValue().equals(serverSiteProps.get(prop.getKey()))) {
        matches = false;
        System.err.format("%s is set to %s in %s, but to %s in %s.%n",
            prop.getKey(), prop.getValue(), clientConfigFilePath,
            serverSiteProps.get(prop.getKey()), serverConfigFilePath);
      }
    }
    if (!matches) {
      return false;
    }
    for (Map.Entry<String, String> prop : serverSiteProps.entrySet()) {
      if (!clientSiteProps.containsKey(prop.getKey())) {
        matches = false;
        System.err.format("%s is configured in %s, but not configured in %s.%n",
            prop.getKey(), serverConfigFilePath, clientConfigFilePath);
      } else if (!prop.getValue().equals(clientSiteProps.get(prop.getKey()))) {
        matches = false;
        System.err.format("%s is set to %s in %s, but to %s in %s.%n",
            prop.getKey(), prop.getValue(), prop.getValue(),
            clientSiteProps.get(prop.getKey()), clientConfigFilePath);
      }
    }
    return matches;
  }
}
