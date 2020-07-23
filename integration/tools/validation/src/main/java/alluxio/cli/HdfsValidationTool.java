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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A tool to validate an HDFS mount, before the path is mounted to Alluxio.
 * */
public class HdfsValidationTool implements ValidationTool {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsValidationTool.class);

  private String mUfsPath;
  private UnderFileSystemConfiguration mUfsConf;

  /**
   * The constructor.
   *
   * @param ufsPath the ufs path
   * @param ufsConf the ufs configuration
   * */
  public HdfsValidationTool(String ufsPath, UnderFileSystemConfiguration ufsConf) {
    mUfsPath = ufsPath;
    mUfsConf = ufsConf;
  }

  private static List<ValidationUtils.TaskResult> validateUfs(
          String ufsPath, AlluxioConfiguration ufsConf) throws InterruptedException {
    Map<String, String> validateOpts = ImmutableMap.of();
    ValidateEnv tasks = new ValidateEnv(ufsPath, ufsConf);

    List<ValidationUtils.TaskResult> results = new ArrayList<>();
    Map<String, String> desc = tasks.getDescription();
    for (Map.Entry<ValidationTask, String> entry : tasks.getTasks().entrySet()) {
      ValidationTask task = entry.getKey();
      String taskName = entry.getValue();
      Class clazz = task.getClass();
      if (clazz.isAnnotationPresent(ApplicableUfsType.class)) {
        ApplicableUfsType type = (ApplicableUfsType) clazz.getAnnotation(ApplicableUfsType.class);
        if (type.value() == ApplicableUfsType.Type.HDFS
                || type.value() == ApplicableUfsType.Type.ALL) {
          ValidationUtils.TaskResult result = task.validate(validateOpts);
          if (desc.containsKey(taskName)) {
            result.setDesc(desc.get(taskName));
          }
          results.add(result);
        }
      }
    }
    return results;
  }

  private static ValidationUtils.TaskResult runUfsTests(String path, InstancedConfiguration conf) {
    try {
      UnderFileSystemContractTest test = new UnderFileSystemContractTest(path, conf);
      return test.runValidationTask();
    } catch (IOException e) {
      return new ValidationUtils.TaskResult(ValidationUtils.State.FAILED, "ufsTests",
              ValidationUtils.getErrorInfo(e), "");
    }
  }

  @Override
  public String runTests() throws InterruptedException {
    // Run validateEnv
    List<ValidationUtils.TaskResult> results = validateUfs(mUfsPath, mUfsConf);

    // group by state
    Map<ValidationUtils.State, List<ValidationUtils.TaskResult>> map = new HashMap<>();
    results.stream().forEach((r) -> {
      map.computeIfAbsent(r.getState(), (k) -> new ArrayList<>()).add(r);
    });

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(map);
  }
}
