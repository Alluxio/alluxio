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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
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
public final class ValidateEnv {
  private static final String USAGE = "validateEnv COMMAND [NAME] [OPTIONS]\n\n"
      + "Validate environment for Alluxio.\n\n"
      + "COMMAND can be one of the following values:\n"
      + "local:   run all validation tasks on local\n"
      + "master:  run master validation tasks on local\n"
      + "worker:  run worker validation tasks on local\n"
      + "all:     run corresponding validation tasks on all master nodes and worker nodes\n"
      + "masters: run master validation tasks on all master nodes\n"
      + "workers: run worker validation tasks on all worker nodes\n\n"
      + "list:    list all validation tasks\n\n"
      + "For all commands except list:\n"
      + "NAME can be any task full name or prefix.\n"
      + "When NAME is given, only tasks with name starts with the prefix will run.\n"
      + "For example, specifying NAME \"master\" or \"ma\" will run both tasks named "
      + "\"master.rpc.port.available\" and \"master.web.port.available\" but not "
      + "\"worker.rpc.port.available\".\n"
      + "If NAME is not given, all tasks for the given TARGET will run.\n\n"
      + "OPTIONS can be a list of command line options. Each option has the"
      + " format \"-<optionName> [optionValue]\"\n";

  private static final String ALLUXIO_MASTER_CLASS = "alluxio.master.AlluxioMaster";
  private static final String ALLUXIO_WORKER_CLASS = "alluxio.worker.AlluxioWorker";
  private static final String ALLUXIO_PROXY_CLASS = "alluxio.proxy.AlluxioProxy";
  private static final Options OPTIONS = new Options();

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
  public ValidateEnv(String path, AlluxioConfiguration conf) {
    mPath = path;
    mConf = conf;

    // HDFS configuration validations
    registerTask("ufs.hdfs.config.correctness",
            "This validates HDFS configuration files like core-site.xml and hdfs-site.xml.",
            new HdfsConfValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.hdfs.config.parity",
            "If a Hadoop config directory is specified, this compares the Hadoop config "
                    + "directory with the HDFS configuration paths given to Alluxio, "
                    + "and see if they are consistent.",
            new HdfsConfParityValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.hdfs.config.proxyuser",
            "This validates proxy user configuration in HDFS for Alluxio. "
                    + "This is needed to enable impersonation for Alluxio.",
            new HdfsProxyUserValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.hdfs.config.version",
            "This validates version compatibility between Alluxio and HDFS.",
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
             "This validates kerberos security configurations for Alluxio masters.",
            new SecureHdfsValidationTask("master", mPath, mConf), mMasterTasks);
    registerTask("worker.ufs.hdfs.security.kerberos",
             "This validates kerberos security configurations for Alluxio workers.",
            new SecureHdfsValidationTask("worker", mPath, mConf), mWorkerTasks);

    // ssh validations
    registerTask("ssh.nodes.reachable",
            "validate SSH port on all Alluxio nodes are reachable",
            new SshValidationTask(mConf), mCommonTasks);

    // UFS validations
    registerTask("ufs.version",
        "This validates the a configured UFS library version is available on the system.",
        new UfsVersionValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.path.accessible",
            "This validates the under file system location is accessible to Alluxio.",
            new UfsDirectoryValidationTask(mPath, mConf), mCommonTasks);
    registerTask("ufs.path.superuser",
            "This validates Alluxio has super user privilege on the under file system.",
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
    registerTask("java.native.libs",
            String.format("This validates if java native libraries defined at %s all exist.",
                    NativeLibValidationTask.NATIVE_LIB_PATH),
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
                   AbstractValidationTask task, List<ValidationTask> tasks) {
    mTasks.put(task, name);
    mTaskDescriptions.put(name, description);
    tasks.add(task);
    List<Option> optList = task.getOptionList();
    synchronized (ValidateEnv.class) {
      optList.forEach(opt -> OPTIONS.addOption(opt));
    }
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

  /**
   * Gets the task descriptions.
   *
   * @return a map of task names mapping to their descriptions
   * */
  public Map<String, String> getDescription() {
    return Collections.unmodifiableMap(mTaskDescriptions);
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
    if (!CommonUtils.isAddressReachable(node, 22, 30 * Constants.SECOND_MS)) {
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

  // runs validation tasks in local environment
  private boolean validateLocal(String target, String name, CommandLine cmd)
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
      ValidationTaskResult result = task.validate(optionsMap);
      results.put(result.mState, results.getOrDefault(result.mState, 0) + 1);
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

  private boolean validateWorkers(String name, CommandLine cmd) throws InterruptedException {
    return validateRemote(ConfigurationUtils.getWorkerHostnames(mConf), "worker", name, cmd);
  }

  private boolean validateMasters(String name, CommandLine cmd) throws InterruptedException {
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

  private void printTasks() {
    printTasks("master");
    printTasks("worker");
    printTasks("cluster");
  }

  private static int runTasks(String target, String name, CommandLine cmd)
      throws InterruptedException {
    // Validate against root path
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    String rootPath = conf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    ValidateEnv validate = new ValidateEnv(rootPath, conf);

    boolean success;
    switch (target) {
      case "local":
      case "worker":
      case "master":
        success = validate.validateLocal(target, name, cmd);
        break;
      case "all":
        success = validate.validateMasters(name, cmd);
        success = validate.validateWorkers(name, cmd) && success;
        success = validate.validateLocal("cluster", name, cmd) && success;
        break;
      case "workers":
        success = validate.validateWorkers(name, cmd);
        break;
      case "masters":
        success = validate.validateMasters(name, cmd);
        break;
      default:
        printHelp("Invalid target.");
        return -2;
    }
    return success ? 0 : -1;
  }

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   */
  public static void printHelp(String message) {
    System.err.println(message);
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(USAGE, OPTIONS, true);
  }

  /**
   * Validates environment.
   *
   * @param argv list of arguments
   * @return 0 on success, -1 on validation failures, -2 on invalid arguments
   */
  public static int validate(String... argv) throws InterruptedException {
    if (argv.length < 1) {
      printHelp("Target not specified.");
      return -2;
    }
    String command = argv[0];
    String name = null;
    String[] args;
    int argsLength = 0;
    // Find all non-option command line arguments.
    while (argsLength < argv.length && !argv[argsLength].startsWith("-")) {
      argsLength++;
    }
    if (argsLength > 1) {
      name = argv[1];
      args = Arrays.copyOfRange(argv, 2, argv.length);
    } else {
      args = Arrays.copyOfRange(argv, 1, argv.length);
    }

    CommandLine cmd;
    try {
      cmd = parseArgsAndOptions(OPTIONS, args);
    } catch (InvalidArgumentException e) {
      System.err.format("Invalid argument: %s.%n", e.getMessage());
      return -1;
    }
    if (command.equals("list")) {
      // Validate against root path
      AlluxioConfiguration conf = InstancedConfiguration.defaults();
      String rootPath = conf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
      ValidateEnv task = new ValidateEnv(rootPath, conf);
      task.printTasks();
      return 0;
    }
    return runTasks(command, name, cmd);
  }

  /**
   * Validates Alluxio environment.
   *
   * @param args the arguments to specify which validation tasks to run
   */
  public static void main(String[] args) throws InterruptedException {
    System.exit(validate(args));
  }

  /**
   * Parses the command line arguments and options in {@code args}.
   *
   * After successful execution of this method, command line arguments can be
   * retrieved by invoking {@link CommandLine#getArgs()}, and options can be
   * retrieved by calling {@link CommandLine#getOptions()}.
   *
   * @param args command line arguments to parse
   * @return {@link CommandLine} object representing the parsing result
   * @throws InvalidArgumentException if command line contains invalid argument(s)
   */
  private static CommandLine parseArgsAndOptions(Options options, String... args)
      throws InvalidArgumentException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      throw new InvalidArgumentException(
          "Failed to parse args for validateEnv", e);
    }
    return cmd;
  }
}
