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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.cli.validation.HdfsValidationTask;
import alluxio.cli.validation.PortAvailabilityValidationTask;
import alluxio.cli.validation.RamDiskMountPrivilegeValidationTask;
import alluxio.cli.validation.SecureHdfsValidationTask;
import alluxio.cli.validation.StorageSpaceValidationTask;
import alluxio.cli.validation.SshValidationTask;
import alluxio.cli.validation.UfsDirectoryValidationTask;
import alluxio.cli.validation.UserLimitValidationTask;
import alluxio.cli.validation.Utils;
import alluxio.cli.validation.ValidationTask;
import alluxio.exception.status.InvalidArgumentException;
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
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for checking Alluxio environment.
 */
// TODO(yanqin): decouple ValidationTask implementations for easier dependency management
public final class ValidateEnv {
  private static final String USAGE = "validateEnv TARGET [NAME] [OPTIONS]\n\n"
      + "Validate environment for Alluxio.\n\n"
      + "TARGET can be one of the following values:\n"
      + "local:   run all validation tasks on local\n"
      + "master:  run master validation tasks on local\n"
      + "worker:  run worker validation tasks on local\n"
      + "all:     run corresponding validation tasks on all master nodes and worker nodes\n"
      + "masters: run master validation tasks on all master nodes\n"
      + "workers: run worker validation tasks on all worker nodes\n\n"
      + "NAME can be any task full name or prefix.\n"
      + "When NAME is given, only tasks with name starts with the prefix will run.\n"
      + "For example, specifying NAME \"master\" or \"ma\" will run both tasks named "
      + "\"master.rpc.port.available\" and \"master.web.port.available\" but not "
      + "\"worker.rpc.port.available\".\n"
      + "If NAME is not given, all tasks for the given TARGET will run.\n\n"
      + "OPTIONS can be a list of command line options. Each option has the"
      + " format \"-<optionName> [optionValue]\"\n";

  private static final Options OPTIONS = new Options()
      .addOption("l", false,
          "List tasks to run for the given TARGET and NAME");

  private static final Map<ValidationTask, String> TASKS = new HashMap<>();

  private static final String ALLUXIO_MASTER_CLASS = "alluxio.master.AlluxioMaster";
  private static final String ALLUXIO_WORKER_CLASS = "alluxio.worker.AlluxioWorker";
  private static final String ALLUXIO_PROXY_CLASS = "alluxio.proxy.AlluxioProxy";

  private static final List<ValidationTask> COMMON_TASKS = new ArrayList<>();
  private static final List<ValidationTask> MASTER_TASKS = new ArrayList<>();
  private static final List<ValidationTask> WORKER_TASKS = new ArrayList<>();

  static {
    // HDFS configuration validations
    registerTask("ufs.hdfs.config.parity", new HdfsValidationTask(), COMMON_TASKS);

    // port availability validations
    registerTask("master.rpc.port.available",
        new PortAvailabilityValidationTask(ServiceType.MASTER_RPC, ALLUXIO_MASTER_CLASS),
        MASTER_TASKS);
    registerTask("master.web.port.available",
        new PortAvailabilityValidationTask(ServiceType.MASTER_WEB, ALLUXIO_MASTER_CLASS),
        MASTER_TASKS);
    registerTask("worker.data.port.available",
        new PortAvailabilityValidationTask(ServiceType.WORKER_DATA, ALLUXIO_WORKER_CLASS),
        WORKER_TASKS);
    registerTask("worker.rpc.port.available",
        new PortAvailabilityValidationTask(ServiceType.WORKER_RPC, ALLUXIO_WORKER_CLASS),
        WORKER_TASKS);
    registerTask("worker.web.port.available",
        new PortAvailabilityValidationTask(ServiceType.WORKER_WEB, ALLUXIO_WORKER_CLASS),
        WORKER_TASKS);
    registerTask("proxy.web.port.available",
        new PortAvailabilityValidationTask(ServiceType.PROXY_WEB, ALLUXIO_PROXY_CLASS),
        COMMON_TASKS);

    // security configuration validations
    registerTask("master.ufs.hdfs.security.kerberos",
        new SecureHdfsValidationTask("master"), MASTER_TASKS);
    registerTask("worker.ufs.hdfs.security.kerberos",
        new SecureHdfsValidationTask("worker"), WORKER_TASKS);

    // ssh validations
    registerTask("ssh.masters.reachable", new SshValidationTask("masters"), COMMON_TASKS);
    registerTask("ssh.workers.reachable", new SshValidationTask("workers"), COMMON_TASKS);

    // UFS validations
    registerTask("ufs.root.accessible", new UfsDirectoryValidationTask(), COMMON_TASKS);

    // RAM disk validations
    registerTask("worker.ramdisk.mount.privilege",
        new RamDiskMountPrivilegeValidationTask(), WORKER_TASKS);

    // User limit validations
    registerTask("ulimit.nofile",
        UserLimitValidationTask.createOpenFilesLimitValidationTask(), COMMON_TASKS);

    registerTask("ulimit.nproc",
        UserLimitValidationTask.createUserProcessesLimitValidationTask(), COMMON_TASKS);

    // space validations
    registerTask("worker.storage.space", new StorageSpaceValidationTask(), WORKER_TASKS);
  }

  private static final Map<String, Collection<ValidationTask>> TARGET_TASKS =
      initializeTargetTasks();

  private static Map<String, Collection<ValidationTask>> initializeTargetTasks() {
    Map<String, Collection<ValidationTask>> targetMap = new TreeMap<>();
    List<ValidationTask> allMasterTasks = new ArrayList<>(COMMON_TASKS);
    allMasterTasks.addAll(MASTER_TASKS);
    targetMap.put("master", allMasterTasks);
    List<ValidationTask> allWorkerTasks = new ArrayList<>(COMMON_TASKS);
    allWorkerTasks.addAll(WORKER_TASKS);
    targetMap.put("worker", allWorkerTasks);
    targetMap.put("local", TASKS.keySet());
    return targetMap;
  }

  private static ValidationTask registerTask(String name, ValidationTask task,
      List<ValidationTask> tasks) {
    TASKS.put(task, name);
    tasks.add(task);
    List<Option> optList = task.getOptionList();
    synchronized (ValidateEnv.class) {
      optList.forEach(opt -> OPTIONS.addOption(opt));
    }
    return task;
  }

  private static boolean validateRemote(List<String> nodes, String target, String name,
      CommandLine cmd) throws InterruptedException {
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
  private static boolean validateRemote(String node, String target, String name, CommandLine cmd)
      throws InterruptedException {
    System.out.format("Validating %s environment on %s...%n", target, node);
    if (!Utils.isAddressReachable(node, 22)) {
      System.err.format("Unable to reach ssh port 22 on node %s.%n", node);
      return false;
    }

    // args is not null.
    String argStr = String.join(" ", cmd.getArgs());
    String homeDir = Configuration.get(PropertyKey.HOME);
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
  private static boolean validateLocal(String target, String name, CommandLine cmd)
      throws InterruptedException {
    int validationCount = 0;
    int failureCount = 0;
    Map<String, String> optionsMap = new HashMap<>();
    for (Option opt : cmd.getOptions()) {
      optionsMap.put(opt.getOpt(), opt.getValue());
    }
    Collection<ValidationTask> tasks = TARGET_TASKS.get(target);
    for (ValidationTask task: tasks) {
      String taskName = TASKS.get(task);
      if (name != null && !taskName.startsWith(name)) {
        continue;
      }
      System.out.format("Validating %s...%n", taskName);
      if (task.shouldSkip()) {
        continue;
      }
      if (task.validate(optionsMap)) {
        System.out.println("OK");
      } else {
        System.err.println("Failed");
        failureCount++;
      }
      validationCount++;
    }
    if (failureCount > 0) {
      System.err.format("Validation failed. Total failures: %d.%n", failureCount);
      return false;
    }
    if (validationCount == 0) {
      System.err.format("No validation task matched name \"%s\".%n", name);
      return false;
    }
    System.out.println("Validation succeeded.");
    return true;
  }

  private static boolean validateWorkers(String name, CommandLine cmd) throws InterruptedException {
    return validateRemote(Utils.readNodeList("workers"), "worker", name, cmd);
  }

  private static boolean validateMasters(String name, CommandLine cmd) throws InterruptedException {
    return validateRemote(Utils.readNodeList("masters"), "master", name, cmd);
  }

  private static void printTasks(String location, String target, String name) {
    System.out.format("Will run the following tasks on %s:%n", location);
    Collection<ValidationTask> tasks = TARGET_TASKS.get(target);
    int count = 0;
    for (ValidationTask task: tasks) {
      String taskName = TASKS.get(task);
      if (name != null && !taskName.startsWith(name)) {
        continue;
      }
      System.out.println(taskName);
      count++;
    }
    if (count == 0) {
      System.err.format("No validation task matched name \"%s\".%n%n", name);
    } else {
      System.out.format("Total %d tasks to run on %s.%n%n", count, location);
    }
  }

  private static int printTasks(String target, String name) {
    switch (target) {
      case "local":
      case "worker":
      case "master":
        printTasks("localhost", target, name);
        break;
      case "all":
        printTasks("masters", "master", name);
        printTasks("workers", "worker", name);
        break;
      case "masters":
        printTasks("masters", "master", name);
        break;
      case "workers":
        printTasks("workers", "worker", name);
        break;
      default:
        printHelp("Invalid target.");
        return -2;
    }
    return 0;
  }

  private static int runTasks(String target, String name, CommandLine cmd)
      throws InterruptedException {
    boolean success;
    switch (target) {
      case "local":
      case "worker":
      case "master":
        success = validateLocal(target, name, cmd);
        break;
      case "all":
        success = validateMasters(name, cmd);
        success = validateWorkers(name, cmd) && success;
        break;
      case "workers":
        success = validateWorkers(name, cmd);
        break;
      case "masters":
        success = validateMasters(name, cmd);
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
    String target = argv[0];
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
    if (cmd.hasOption("l")) {
      return printTasks(target, name);
    }
    return runTasks(target, name, cmd);
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

  private ValidateEnv() {} // prevents instantiation
}
