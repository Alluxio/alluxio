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

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.cli.ApplicableUfsType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Compares HDFS configuration in Alluxio and in HDFS environment variables.
 * */
@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
public class HdfsConfParityValidationTask extends HdfsConfValidationTask {
  /** Name of the environment variable to store the path to Hadoop config directory. */
  protected static final String HADOOP_CONF_DIR_ENV_VAR = "HADOOP_CONF_DIR";

  protected static final Option HADOOP_CONF_DIR_OPTION =
          Option.builder("hadoopConfDir").required(false).hasArg(true)
                  .desc("path to server-side hadoop conf dir").build();

  /**
   * Constructor.
   *
   * @param path the UFS path
   * @param conf the UFS configuration
   * */
  public HdfsConfParityValidationTask(String path, AlluxioConfiguration conf) {
    super(path, conf);
  }

  @Override
  public String getName() {
    return "ValidateHdfsServerAndClientConf";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    if (!ValidationUtils.isHdfsScheme(mPath)) {
      mMsg.append(String.format("UFS path %s is not HDFS. "
              + "Skipping validation for HDFS properties.%n", mPath));
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              mMsg.toString(), mAdvice.toString());
    }

    return validateHdfsSettingParity(optionsMap);
  }

  @Override
  public List<Option> getOptionList() {
    List<Option> opts = new ArrayList<>();
    opts.add(HADOOP_CONF_DIR_OPTION);
    return opts;
  }

  private ValidationTaskResult validateHdfsSettingParity(Map<String, String> optionsMap) {
    String serverHadoopConfDirPath;
    if (optionsMap.containsKey(HADOOP_CONF_DIR_OPTION.getOpt())) {
      serverHadoopConfDirPath = optionsMap.get(HADOOP_CONF_DIR_OPTION.getOpt());
    } else {
      serverHadoopConfDirPath = System.getenv(HADOOP_CONF_DIR_ENV_VAR);
    }
    if (serverHadoopConfDirPath == null) {
      mMsg.append("Path to server-side hadoop configuration unspecified,"
              + " skipping validation for HDFS properties.");
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              mMsg.toString(), mAdvice.toString());
    }
    String serverCoreSiteFilePath = PathUtils.concatPath(serverHadoopConfDirPath,
            "/core-site.xml");
    String serverHdfsSiteFilePath = PathUtils.concatPath(serverHadoopConfDirPath,
            "/hdfs-site.xml");

    // Load client core-site and hdfs-site config
    ValidationTaskResult loadConfig = loadHdfsConfig();
    if (loadConfig.getState() != ValidationUtils.State.OK) {
      // If failed to load config files, abort
      return loadConfig;
    }

    boolean ok = compareConfigurations(serverCoreSiteFilePath,
             "core-site.xml", mCoreConf)
            && compareConfigurations(serverHdfsSiteFilePath,
             "hdfs-site.xml", mHdfsConf);
    return new ValidationTaskResult(ok ? ValidationUtils.State.OK
             : ValidationUtils.State.FAILED, getName(), mMsg.toString(), mAdvice.toString());
  }

  private boolean compareConfigurations(String serverConfigFilePath, String clientSiteName,
                                        Map<String, String> clientSiteProps) {
    HadoopConfigurationFileParser parser = new HadoopConfigurationFileParser();

    Map<String, String> serverSiteProps;
    try {
      serverSiteProps = parser.parseXmlConfiguration(serverConfigFilePath);
    } catch (Exception e) {
      mMsg.append(String.format("Failed to parse server-side %s.%n", serverConfigFilePath));
      mMsg.append(ValidationUtils.getErrorInfo(e));
      mAdvice.append(String.format("Please fix the parsing error in %s.%n", serverConfigFilePath));
      return false;
    }

    boolean matches = true;
    for (Map.Entry<String, String> prop : clientSiteProps.entrySet()) {
      if (!serverSiteProps.containsKey(prop.getKey())) {
        matches = false;
        mMsg.append(String.format("%s is configured in %s, but not configured in %s.%n",
                prop.getKey(), clientSiteName, serverConfigFilePath));
        mAdvice.append(String.format("Please configure property %s in %s.%n",
                prop.getKey(), serverConfigFilePath));
      } else if (!prop.getValue().equals(serverSiteProps.get(prop.getKey()))) {
        matches = false;
        mMsg.append(String.format("%s is set to %s in %s, but to %s in %s.%n",
                prop.getKey(), prop.getValue(), clientSiteName,
                serverSiteProps.get(prop.getKey()), serverConfigFilePath));
        mAdvice.append(String.format("Please fix the inconsistency on property %s.%n",
                prop.getKey()));
      }
    }
    if (!matches) {
      return false;
    }
    for (Map.Entry<String, String> prop : serverSiteProps.entrySet()) {
      if (!clientSiteProps.containsKey(prop.getKey())) {
        matches = false;
        mMsg.append(String.format("%s is configured in %s, but not configured in %s.%n",
                prop.getKey(), serverConfigFilePath, clientSiteName));
        mAdvice.append(String.format("Please configure %s in %s.%n",
                prop.getKey(), clientSiteName));
      } else if (!prop.getValue().equals(clientSiteProps.get(prop.getKey()))) {
        matches = false;
        mMsg.append(String.format("%s is set to %s in %s, but to %s in %s.%n",
                prop.getKey(), prop.getValue(), prop.getValue(),
                clientSiteProps.get(prop.getKey()), clientSiteName));
        mAdvice.append(String.format("Please fix the inconsistency on property %s.%n",
                prop.getKey()));
      }
    }
    return matches;
  }
}
