package alluxio.cli;

import alluxio.underfs.UnderFileSystemConfiguration;

import java.util.Map;

public class HdfsValidationToolFactory implements ValidationToolFactory {
  @Override
  public String getType() {
    return ValidationConfig.HDFS_TOOL_TYPE;
  }

  @Override
  public ValidationTool create(Map<Object, Object> configMap) {
    String ufsPath = (String) configMap.get("ufsPath");
    UnderFileSystemConfiguration ufsConf = (UnderFileSystemConfiguration) configMap.get("ufsConfig");
    return new HdfsValidationTool(ufsPath, ufsConf);
  }
}
