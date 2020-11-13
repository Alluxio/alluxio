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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ShellUtils;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validates if the HDFS version works with the UFS version specified in
 * alluxio configuration.
 * */
@ApplicableUfsType(ApplicableUfsType.Type.HDFS)
public class HdfsVersionValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;
  public static final String HADOOP_PREFIX = "hadoop-";
  public static final String CDH_PREFIX = "cdh-";

  // An example output from "hadoop version" command:
  //    Hadoop 2.7.2
  //    Subversion https://git-wip-us.apache.org/repos/asf/hadoop.git
  //      -r b165c4fe8a74265c792ce23f546c64604acf0e41
  //    Compiled by jenkins on 2016-01-26T00:08Z
  //    Compiled with protoc 2.5.0
  //    From source with checksum d0fda26633fa762bff87ec759ebe689c
  //    This command was run using /tmp/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar
  private static final Pattern HADOOP_PATTERN =
          Pattern.compile("Hadoop\\s+(?<version>([0-9]\\.)+[0-9]+)");
  // An example Hadoop version for CDH distribution is
  // Hadoop 2.6.0-cdh5.16.2
  private static final Pattern CDH_PATTERN =
          Pattern.compile("cdh(?<cdhVersion>([0-9]+\\.)+[0-9]+)");

  /**
   * Creates a new instance of {@link HdfsVersionValidationTask}
   * for validating HDFS version.
   * @param conf configuration
   */
  public HdfsVersionValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateHdfsVersion";
  }

  protected String parseVersion(String output) {
    Matcher cdhMatcher = CDH_PATTERN.matcher(output);
    // Use CDH version if it is CDH
    if (cdhMatcher.find()) {
      String cdhVersion = cdhMatcher.group("cdhVersion");
      return "cdh" + cdhVersion;
    }
    // Use Hadoop version otherwise
    String version = "";
    Matcher matcher = HADOOP_PATTERN.matcher(output);
    if (matcher.find()) {
      version = matcher.group("version");
    }
    return version;
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionMap)
          throws InterruptedException {
    String hadoopVersion;
    try {
      hadoopVersion = getHadoopVersion();
    } catch (IOException e) {
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              String.format("Failed to get hadoop version:%n%s.", ValidationUtils.getErrorInfo(e)),
              "Please check if hadoop is on your PATH.");
    }

    String version = mConf.get(PropertyKey.UNDERFS_VERSION);
    for (String prefix : new String[] {CDH_PREFIX, HADOOP_PREFIX}) {
      if (version.startsWith(prefix)) {
        version = version.substring(prefix.length());
        break;
      }
    }
    if (hadoopVersion.contains(version)) {
      return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
              String.format("Hadoop version %s contains UFS version defined in alluxio %s=%s.",
                      hadoopVersion, PropertyKey.UNDERFS_VERSION.toString(), version),
              "");
    }

    return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
            String.format("Hadoop version %s does not match %s=%s.",
                    hadoopVersion, PropertyKey.UNDERFS_VERSION.toString(), version),
            String.format("Please configure %s to match the HDFS version.",
                    PropertyKey.UNDERFS_VERSION.toString()));
  }

  protected String getHadoopVersion() throws IOException {
    String[] cmd = new String[]{"hadoop", "version"};
    String version = ShellUtils.execCommand(cmd);
    return parseVersion(version);
  }
}
