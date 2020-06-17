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

import alluxio.cli.ValidationTask;
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
public class HdfsVersionValidationTask implements ValidationTask {
  private final AlluxioConfiguration mConf;

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
    // An example output from "hadoop version" command:
    //    Hadoop 2.7.2
    //    Subversion https://git-wip-us.apache.org/repos/asf/hadoop.git
    //      -r b165c4fe8a74265c792ce23f546c64604acf0e41
    //    Compiled by jenkins on 2016-01-26T00:08Z
    //    Compiled with protoc 2.5.0
    //    From source with checksum d0fda26633fa762bff87ec759ebe689c
    //    This command was run using /tmp/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar
    String regex = "Hadoop\\s+(?<version>([0-9]\\.)+[0-9])";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(output);
    String version = "";
    if (matcher.find()) {
      version = matcher.group("version");
    }
    return version;
  }

  @Override
  public ValidationUtils.TaskResult validate(Map<String, String> optionMap)
          throws InterruptedException {
    String hadoopVersion;
    try {
      hadoopVersion = getHadoopVersion();
    } catch (IOException e) {
      return new ValidationUtils.TaskResult(ValidationUtils.State.FAILED, getName(),
              String.format("Failed to get hadoop version:%n%s.", ValidationUtils.getErrorInfo(e)),
              "Please check if hadoop is on your PATH.");
    }

    String version = mConf.get(PropertyKey.UNDERFS_VERSION);
    if (hadoopVersion.contains(version)) {
      return new ValidationUtils.TaskResult(ValidationUtils.State.OK, getName(),
              String.format("Hadoop version %s contains UFS version defined in alluxio %s=%s.",
                      hadoopVersion, PropertyKey.UNDERFS_VERSION.toString(), version),
              "");
    }

    return new ValidationUtils.TaskResult(ValidationUtils.State.FAILED, getName(),
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
