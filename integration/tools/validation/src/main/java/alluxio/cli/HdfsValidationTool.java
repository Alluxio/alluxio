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

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A tool to validate an HDFS mount, before the path is mounted to Alluxio.
 * */
public class HdfsValidationTool implements ValidationTool {

  private String mUfsPath;
  private UnderFileSystemConfiguration mUfsConf;

  /**
   * The constructor.
   *
   * @param ufsPath the ufs path
   * @param ufsConf the ufs configuration
   * */
  public HdfsValidationTool(String ufsPath, UnderFileSystemConfiguration ufsConf) {
    mUfsPath = PathUtils.normalizePath(ufsPath, AlluxioURI.SEPARATOR);
    mUfsConf = ufsConf;
  }

  @Override
  public Map<String, ValidationTask> getTasks() {
    ValidateEnv env = new ValidateEnv(mUfsPath, mUfsConf);
    Map<String, ValidationTask> tasks = new HashMap<>();
    for (Map.Entry<ValidationTask, String> entry : env.getTasks().entrySet()) {
      ValidationTask task = entry.getKey();
      String taskName = entry.getValue();
      Class clazz = task.getClass();
      if (clazz.isAnnotationPresent(ApplicableUfsType.class)) {
        ApplicableUfsType type = (ApplicableUfsType) clazz.getAnnotation(ApplicableUfsType.class);
        if (type.value() == ApplicableUfsType.Type.HDFS
            || type.value() == ApplicableUfsType.Type.ALL) {
          tasks.put(taskName, task);
        }
      }
    }
    return tasks;
  }

  protected List<ValidationTaskResult> validateUfs() throws InterruptedException {
    Map<String, String> validateOpts = ImmutableMap.of();
    Map<String, String> desc = new ValidateEnv(mUfsPath, mUfsConf).getDescription();
    List<ValidationTaskResult> results = new LinkedList<>();
    for (Map.Entry<String, ValidationTask> entry : getTasks().entrySet()) {
      ValidationTask task = entry.getValue();
      String taskName = entry.getKey();
      ValidationTaskResult result;
      try {
        result = task.validate(validateOpts);
      } catch (InterruptedException e) {
        result = new ValidationTaskResult(ValidationUtils.State.FAILED, task.getName(),
            "Task interrupted while running", "");
      }
      if (desc.containsKey(taskName)) {
        result.setDesc(desc.get(taskName));
      }
      results.add(result);
    }
    return results;
  }

  @Override
  public List<ValidationTaskResult> runAllTests() throws InterruptedException {
    return validateUfs();
  }
}
