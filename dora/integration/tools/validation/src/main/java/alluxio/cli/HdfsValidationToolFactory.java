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

import alluxio.underfs.UnderFileSystemConfiguration;

import java.util.Map;

/**
 * Factory to create an HDFS validation tool implementation.
 */
public class HdfsValidationToolFactory implements ValidationToolFactory {
  @Override
  public String getType() {
    return ValidationConfig.HDFS_TOOL_TYPE;
  }

  @Override
  public ValidationTool create(Map<Object, Object> configMap) {
    String ufsPath = (String) configMap.get(ValidationConfig.UFS_PATH);
    UnderFileSystemConfiguration ufsConf =
            (UnderFileSystemConfiguration) configMap.get(ValidationConfig.UFS_CONFIG);
    return new HdfsValidationTool(ufsPath, ufsConf);
  }
}
