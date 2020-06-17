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

import alluxio.Constants;
import alluxio.cli.hdfs.HdfsConfParityValidationTask;
import alluxio.cli.hdfs.HdfsConfValidationTask;
import alluxio.cli.hdfs.HdfsProxyUserValidationTask;
import alluxio.cli.hdfs.HdfsVersionValidationTask;
import alluxio.cli.hdfs.SecureHdfsValidationTask;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for checking Alluxio environment.
 */
// TODO(yanqin): decouple ValidationTask implementations for easier dependency management
public final class EnvValidationTool {
  private static final String ALLUXIO_MASTER_CLASS = "alluxio.master.AlluxioMaster";
  private static final String ALLUXIO_WORKER_CLASS = "alluxio.worker.AlluxioWorker";
  private static final String ALLUXIO_PROXY_CLASS = "alluxio.proxy.AlluxioProxy";

  private final Map<ValidationTask, String> mTasks = new HashMap<>();
  private final Map<String, String> mTaskDescriptions = new HashMap<>();
  private final List<ValidationTask> mCommonTasks = new ArrayList<>();
  private final List<ValidationTask> mClusterTasks = new ArrayList<>();
  private final List<ValidationTask> mMasterTasks = new ArrayList<>();
  private final List<ValidationTask> mWorkerTasks = new ArrayList<>();
  private final Map<String, Collection<ValidationTask>> mTargetTasks;

  private final AlluxioConfiguration mConf;
  private final String mPath;

  /**
   * Initializes from the target UFS path and configurations.
   *
   * @param path the UFS path
   * @param conf the UFS configurtions
   * */
  public EnvValidationTool(String path, AlluxioConfiguration conf) {
    mPath = path;
    mConf = conf;

    // HDFS configuration validations
    registerTask("ufs.hdfs.config.correctness", "validate HDFS configuration files",
            new HdfsConfValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.hdfs.config.parity",
            "validate HDFS-related configurations",
            new HdfsConfParityValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.hdfs.config.proxyuser",
            "validate proxyuser configuration in hdfs for alluxio",
            new HdfsProxyUserValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.hdfs.config.version",
            "validate version compatibility between alluxio and hdfs",
            new HdfsVersionValidationTask(mConf), mCommonTasks);

    // port availability validations
    registerTask("master.rpc.port.available",
            "validate master RPC port is available",
            new PortAvailabilityValidationTask(ServiceType.MASTER_RPC, ALLUXIO_MASTER_CLASS, mConf),
            mMasterTasks);
    registerTask("master.web.port.available",
            "validate master web port is available",
            new PortAvailabilityValidationTask(ServiceType.MASTER_WEB, ALLUXIO_MASTER_CLASS, mConf),
            mMasterTasks);
    registerTask("worker.rpc.port.available",
            "validate worker RPC port is available",
            new PortAvailabilityValidationTask(ServiceType.WORKER_RPC, ALLUXIO_WORKER_CLASS, mConf),
            mWorkerTasks);
    registerTask("worker.web.port.available",
            "validate worker web port is available",
            new PortAvailabilityValidationTask(ServiceType.WORKER_WEB, ALLUXIO_WORKER_CLASS, mConf),
            mWorkerTasks);
    registerTask("proxy.web.port.available",
            "validate proxy web port is available",
            new PortAvailabilityValidationTask(ServiceType.PROXY_WEB, ALLUXIO_PROXY_CLASS, mConf),
            mCommonTasks);

    // security configuration validations
    registerTask("master.ufs.hdfs.security.kerberos",
            "validate kerberos security configurations for masters",
            new SecureHdfsValidationTask("master", mPath, mConf), mMasterTasks);
    registerTask("worker.ufs.hdfs.security.kerberos",
            "validate kerberos security configurations for workers",
            new SecureHdfsValidationTask("worker", mPath, mConf), mWorkerTasks);

    // ssh validations
    registerTask("ssh.nodes.reachable",
            "validate SSH port on all Alluxio nodes are reachable",
            new SshValidationTask(mConf), mCommonTasks);

    // UFS validations
    registerTask("ufs.path.accessible",
            "validate the under file system location is accessible",
            new UfsDirectoryValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.path.superuser",
            "validate Alluxio has super user privilege on the under file system",
            new UfsSuperUserValidationTask(mPath, mConf), mCommonTasks);

    // RAM disk validations
    registerTask("worker.ramdisk.mount.privilege",
            "validate user has the correct privilege to mount ramdisk",
            new RamDiskMountPrivilegeValidationTask(mConf), mWorkerTasks);

    // User limit validations
    registerTask("ulimit.nofile",
            "validate ulimit for number of open files is set appropriately",
            UserLimitValidationTask.createOpenFilesLimitValidationTask(), mCommonTasks);
    registerTask("ulimit.nproc",
            "validate ulimit for number of processes is set appropriately",
            UserLimitValidationTask.createUserProcessesLimitValidationTask(), mCommonTasks);

    // space validations
    registerTask("worker.storage.space",
            "validate tiered storage locations have enough space",
            new StorageSpaceValidationTask(mConf), mWorkerTasks);
    registerTask("cluster.conf.consistent",
            "validate configuration consistency across the cluster",
            new ClusterConfConsistencyValidationTask(mConf), mClusterTasks);

    // java option validations
    registerTask("java.native.libs", "validate java native lib paths",
            new NativeLibValidationTask(), mCommonTasks);

    mTargetTasks = initializeTargetTasks();
  }

  private Map<String, Collection<ValidationTask>> initializeTargetTasks() {
    Map<String, Collection<ValidationTask>> targetMap = new TreeMap<>();
    List<ValidationTask> allMasterTasks = new ArrayList<>(mCommonTasks);
    allMasterTasks.addAll(mMasterTasks);
    targetMap.put("master", allMasterTasks);
    List<ValidationTask> allWorkerTasks = new ArrayList<>(mCommonTasks);
    allWorkerTasks.addAll(mWorkerTasks);
    targetMap.put("worker", allWorkerTasks);
    targetMap.put("local", mTasks.keySet());
    targetMap.put("cluster", new ArrayList<>(mClusterTasks));
    return targetMap;
  }

  private ValidationTask registerTask(String name, String description,
                   ValidationTask task, List<ValidationTask> tasks) {
    mTasks.put(task, name);
    mTaskDescriptions.put(name, description);
    tasks.add(task);
    return task;
  }

  /**
   * Get the tasks registered.
   *
   * @return a map of tasks mapping to their name
   * */
  public Map<ValidationTask, String> getTasks() {
    return Collections.unmodifiableMap(mTasks);
  }

  private boolean validateRemote(Collection<String> nodes, String target,
                                 String name, CommandLine cmd) throws InterruptedException {
    if (nodes == null) {
      return false;
    }

    boolean success = true;
    for (String node : nodes) {
      success &= validateRemote(node, target, name, cmd);
    }

    return success;
  }

  // validates environment on remote node
  private boolean validateRemote(String node, String target, String name,
                                 CommandLine cmd) throws InterruptedException {
    System.out.format("Validating %s environment on %s...%n", target, node);
    if (!CommonUtils.isAddressReachable(node, 22)) {
      System.err.format("Unable to reach ssh port 22 on node %s.%n", node);
      return false;
    }

    // args is not null.
    String argStr = String.join(" ", cmd.getArgs());
    String homeDir = mConf.get(PropertyKey.HOME);
    String remoteCommand = String.format(
        "%s/bin/alluxio validateEnv %s %s %s",
        homeDir, target, name == null ? "" : name, argStr);
    String localCommand = String.format(
        "ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt %s \"bash %s\"",
        node, remoteCommand);
    String[] command = {"bash", "-c", localCommand};
    try {
      ProcessBuilder builder = new ProcessBuilder(command);
      builder.redirectErrorStream(true);
      builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
      builder.redirectInput(ProcessBuilder.Redirect.INHERIT);
      Process process = builder.start();
      process.waitFor();
      return process.exitValue() == 0;
    } catch (IOException e) {
      System.err.format("Unable to validate on node %s: %s.%n", node, e.getMessage());
      return false;
    }
  }

  /**
   * Runs the validation tasks locally.
   *
   * @param target target task set
   * @param name task name prefix
   * @param cmd the command line
   * @return whether the validations passed
   * */
  public boolean validateLocal(String target, String name, CommandLine cmd)
      throws InterruptedException {
    int validationCount = 0;
    Map<ValidationUtils.State, Integer> results = new HashMap<>();
    Map<String, String> optionsMap = new HashMap<>();
    for (Option opt : cmd.getOptions()) {
      optionsMap.put(opt.getOpt(), opt.getValue());
    }
    Collection<ValidationTask> tasks = mTargetTasks.get(target);
    System.out.format("Validating %s environment...%n", target);
    for (ValidationTask task: tasks) {
      String taskName = mTasks.get(task);
      if (name != null && !taskName.startsWith(name)) {
        continue;
      }
      System.out.format("Validating %s...%n", taskName);
      ValidationUtils.TaskResult result = task.validate(optionsMap);
      results.put(result.mState, results.getOrDefault(result, 0) + 1);
      switch (result.mState) {
        case OK:
          System.out.print(Constants.ANSI_GREEN);
          break;
        case WARNING:
          System.out.print(Constants.ANSI_YELLOW);
          break;
        case FAILED:
          System.out.print(Constants.ANSI_RED);
          break;
        case SKIPPED:
          System.out.print(Constants.ANSI_PURPLE);
          break;
        default:
          break;
      }
      System.out.print(result.getName());
      System.out.println(Constants.ANSI_RESET);
      validationCount++;
    }
    if (results.containsKey(ValidationUtils.State.FAILED)) {
      System.err.format("%d failures ", results.get(ValidationUtils.State.FAILED));
    }
    if (results.containsKey(ValidationUtils.State.WARNING)) {
      System.err.format("%d warnings ", results.get(ValidationUtils.State.WARNING));
    }
    if (results.containsKey(ValidationUtils.State.SKIPPED)) {
      System.err.format("%d skipped ", results.get(ValidationUtils.State.SKIPPED));
    }
    System.err.println();
    if (validationCount == 0) {
      System.err.format("No validation task matched name \"%s\".%n", name);
      return false;
    }
    if (results.containsKey(ValidationUtils.State.FAILED)) {
      return false;
    }
    System.out.println("Validation succeeded.");
    return true;
  }

  /**
   * Runs the worker validation tasks.
   *
   * @param name task name prefix
   * @param cmd the command line
   * @return whether the validations passed
   * */
  public boolean validateWorkers(String name, CommandLine cmd) throws InterruptedException {
    return validateRemote(ConfigurationUtils.getWorkerHostnames(mConf), "worker", name, cmd);
  }

  /**
   * Runs the master validation tasks.
   *
   * @param name target task prefix
   * @param cmd the command line
   * @return whether the validations passed
   * */
  public boolean validateMasters(String name, CommandLine cmd) throws InterruptedException {
    return validateRemote(ConfigurationUtils.getMasterHostnames(mConf), "master", name, cmd);
  }

  private void printTasks(String target) {
    System.out.format("The following tasks are available to run on %s:%n", target);
    Collection<ValidationTask> tasks = mTargetTasks.get(target);
    for (ValidationTask task: tasks) {
      String taskName = mTasks.get(task);
      System.out.printf("%s: %s%n", taskName, mTaskDescriptions.get(taskName));
    }
    System.out.println();
  }

  /**
   * Prints all the tasks.
   * */
  public void printTasks() {
    printTasks("master");
    printTasks("worker");
    printTasks("cluster");
  }

  /**
   * Aggregates the options from each validation tasks.
   * Each {@link ValidationTask} defines the {@link Options} in a OPTIONS field
   *
   * @return all options
   * */
  public static Options getOptions() {
    Options options = new Options();
    Reflections reflections = new Reflections(ValidationTask.class.getPackage().getName());
    for (Class<? extends ValidationTask> cls : reflections.getSubTypesOf(ValidationTask.class)) {
      boolean hasOptions = Arrays.stream(cls.getFields())
              .anyMatch(f -> f.getName().equals("OPTIONS"));
      if (!hasOptions) {
        continue;
      }

      Field[] fields = cls.getFields();
      Options taskOptions = null;
      for (Field f : fields) {
        if (f.getName().equals("OPTIONS")) {
          try {
            taskOptions = (Options) f.get(cls);
            break;
          } catch (IllegalAccessException e) {
            // Cannot access field, skip it
            break;
          }
        }
      }
      if (taskOptions != null) {
        taskOptions.getOptions().forEach(options::addOption);
      }
    }
    return options;
  }
}
