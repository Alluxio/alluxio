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
import alluxio.master.AlluxioMaster;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.AlluxioWorker;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for checking Alluxio environment.
 */
public final class ValidateEnv {
  private static final String USAGE = "USAGE: ValidateEnv TARGET [NAME]\n\n"
      + "Validate environment for Alluxio.\n\n"
      + "TARGET can be one of the following values:\n"
      + "local: \trun all validation tasks on local\n"
      + "master: \trun master validation tasks on local\n"
      + "worker: \trun worker validation tasks on local\n"
      + "all: \trun corresponding validation tasks on all master nodes and worker nodes\n"
      + "masters: \trun master validation tasks on all master nodes\n"
      + "workers: \trun worker validation tasks on all worker nodes\n\n"
      + "NAME can be any task full name or prefix.\n"
      + "When NAME is given, only tasks with name starts with the prefix will run.\n";

  private static final Options OPTIONS = new Options();

  private static final Map<ValidationTask, String> TASK_NAMES = new HashMap<>();

  // port availability validations
  private static final ValidationTask MASTER_RPC_VALIDATION_TASK = create(
      "master.rpc.port.available",
      new PortAvailabilityValidationTask(ServiceType.MASTER_RPC, AlluxioMaster.class));
  private static final ValidationTask MASTER_WEB_VALIDATION_TASK = create(
      "master.web.port.available",
      new PortAvailabilityValidationTask(ServiceType.MASTER_WEB, AlluxioMaster.class));
  private static final ValidationTask WORKER_DATA_VALIDATION_TASK = create(
      "worker.data.port.available",
      new PortAvailabilityValidationTask(ServiceType.WORKER_DATA, AlluxioWorker.class));
  private static final ValidationTask WORKER_RPC_VALIDATION_TASK = create(
      "worker.rpc.port.available",
      new PortAvailabilityValidationTask(ServiceType.WORKER_RPC, AlluxioWorker.class));
  private static final ValidationTask WORKER_WEB_VALIDATION_TASK = create(
      "worker.web.port.available",
      new PortAvailabilityValidationTask(ServiceType.WORKER_WEB, AlluxioWorker.class));

  // ssh validations
  private static final ValidationTask MASTERS_SSH_VALIDATION_TASK = create(
      "masters.ssh.reachable",
      new SshValidationTask("masters"));
  private static final ValidationTask WORKERS_SSH_VALIDATION_TASK = create(
      "workers.ssh.reachable",
      new SshValidationTask("workers"));

  private static final Map<String, Collection<ValidationTask>> TARGET_TASKS =
      initializeTargetTasks();

  private static Map<String, Collection<ValidationTask>> initializeTargetTasks() {
    Map<String, Collection<ValidationTask>> targetMap = new TreeMap<>();
    targetMap.put("master", Arrays.asList(
        MASTER_RPC_VALIDATION_TASK,
        MASTER_WEB_VALIDATION_TASK,
        MASTERS_SSH_VALIDATION_TASK,
        WORKERS_SSH_VALIDATION_TASK
    ));
    targetMap.put("worker", Arrays.asList(
        WORKER_DATA_VALIDATION_TASK,
        WORKER_RPC_VALIDATION_TASK,
        WORKER_WEB_VALIDATION_TASK,
        MASTERS_SSH_VALIDATION_TASK,
        WORKERS_SSH_VALIDATION_TASK
    ));
    targetMap.put("local", TASK_NAMES.keySet());
    return targetMap;
  }

  private interface ValidationTask {
    boolean validate();
  }

  private static ValidationTask create(String name, ValidationTask task) {
    TASK_NAMES.put(task, name);
    return task;
  }

  private static boolean isLocalPortAvailable(int port) {
    try (ServerSocket socket = new ServerSocket(port)) {
      socket.setReuseAddress(true);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private static boolean isAddressReachable(String hostname, int port) {
    try (Socket socket = new Socket(hostname, port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private static boolean isAlluxioRunning(String className) {
    String[] command = {"bash", "-c",
                        "ps -Aww -o command | grep -i \"[j]ava\" | grep " + className};
    try {
      Process p = Runtime.getRuntime().exec(command);
      try (InputStreamReader input = new InputStreamReader(p.getInputStream())) {
        if (input.read() >= 0) {
          return true;
        }
      }
      return false;
    } catch (IOException e) {
      System.err.format("Unable to check Alluxio status: %s.%n", e.getMessage());
      return false;
    }
  }

  private static class PortAvailabilityValidationTask implements ValidationTask {
    private final ServiceType mServiceType;
    private final Class mOwner;

    public PortAvailabilityValidationTask(ServiceType serviceType, Class owner) {
      mServiceType = serviceType;
      mOwner = owner;
    }

    @Override
    public boolean validate() {
      if (isAlluxioRunning(mOwner.getCanonicalName())) {
        System.out.format("%s is already running. Skip validation.%n", mOwner.getSimpleName());
        return true;
      }
      int port = NetworkAddressUtils.getPort(mServiceType);
      if (!isLocalPortAvailable(port)) {
        System.err.format("%s port %d is not available.%n", mServiceType.getServiceName(), port);
        return false;
      }
      return true;
    }
  }

  private static class SshValidationTask implements ValidationTask {
    private final String mFileName;

    public SshValidationTask(String fileName) {
      mFileName = fileName;
    }

    @Override
    public boolean validate() {
      List<String> nodes = readNodeList(mFileName);
      if (nodes == null) {
        return false;
      }

      boolean hasUnreachableNodes = false;
      for (String nodeName : nodes) {
        if (!isAddressReachable(nodeName, 22)) {
          System.err.format("Unable to reach ssh port 22 on node %s.%n", nodeName);
          hasUnreachableNodes = true;
        }
      }
      return !hasUnreachableNodes;
    }
  }

  // read a list of nodes from given file name ignoring comments and empty lines
  private static List<String> readNodeList(String fileName) {
    String confDir = Configuration.get(PropertyKey.CONF_DIR);
    List<String> lines;
    try {
      lines = Files.readAllLines(Paths.get(confDir, fileName), StandardCharsets.UTF_8);
    } catch (IOException e) {
      System.err.format("Unable to read file %s/%s.%n", confDir, fileName);
      return null;
    }

    List<String> nodes = new ArrayList<>();
    for (String line : lines) {
      String node = line.trim();
      if (node.startsWith("#") || node.length() == 0) {
        continue;
      }
      nodes.add(node);
    }

    return nodes;
  }

  private static boolean validateRemote(List<String> nodes, String target, String name)
      throws InterruptedException {
    if (nodes == null) {
      return false;
    }

    boolean success = true;
    for (String node : nodes) {
      success &= validateRemote(node, target, name);
    }

    return success;
  }

  // validate environment on remote node
  private static boolean validateRemote(String node, String target, String name)
      throws InterruptedException {
    System.out.format("Validating %s environment on %s...%n", target, node);
    if (isAddressReachable(node, 22)) {
      String homeDir = Configuration.get(PropertyKey.HOME);
      String remoteCommand = String.format(
              "%s/bin/alluxio validateEnv %s %s", homeDir, target, name == null ? "" : name);
      String localCommand = String.format(
              "ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt %s \"bash %s\"",
              node, remoteCommand);
      String[] command = {"bash", "-c", localCommand};
      try {
        ProcessBuilder builder =
                new ProcessBuilder(command);
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

    } else {
      System.err.format("Unable to reach ssh port 22 on node %s.%n", node);
      return false;
    }
  }

  // run validation tasks in local environment
  private static boolean validateLocal(String target, String name) {
    int validationCount = 0;
    int failureCount = 0;
    Collection<ValidationTask> tasks = TARGET_TASKS.get(target);
    for (ValidationTask task : tasks) {
      String taskName = TASK_NAMES.get(task);
      if (name != null && !taskName.startsWith(name)) {
        continue;
      }

      System.out.format("Validating %s...", taskName);
      if (task.validate()) {
        System.out.println("OK");
      } else {
        System.out.println("Failed");
        failureCount++;
      }

      validationCount++;
    }

    if (failureCount > 0) {
      System.out.format("Validation failed. Total failures: %d.%n", failureCount);
      return false;
    }

    if (validationCount == 0) {
      System.out.format("No validation task matched name \"%s\".%n", name);
      return false;
    }

    System.out.println("Validation succeeded.");
    return false;
  }

  private static boolean validateWorkers(String name) throws InterruptedException {
    return validateRemote(readNodeList("workers"), "worker", name);
  }

  private static boolean validateMasters(String name) throws InterruptedException {
    return validateRemote(readNodeList("masters"), "master", name);
  }

  /**
   * Prints the help message.
   *
   * @param message message before standard usage information
   */
  public static void printHelp(String message) {
    System.err.println(message);
    HelpFormatter help = new HelpFormatter();
    help.printHelp(USAGE, OPTIONS);
  }

  /**
   * Validate environment.
   *
   * @param args list of arguments
   * @return 0 on success, 1 on failures
   */
  public static int validate(String... args) throws InterruptedException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      printHelp("Unable to parse input args: " + e.getMessage());
      return 1;
    }

    Preconditions.checkNotNull(cmd, "Unable to parse input args.");
    args = cmd.getArgs();
    if (args.length < 1) {
      printHelp("Target not specified.");
      return 1;
    }

    if (args.length > 2) {
      printHelp("More arguments than expected.");
      return 1;
    }

    String target = args[0];
    String name = args.length > 1 ? args[1] : null;

    boolean success = false;
    switch (target) {
      case "local":
      case "worker":
      case "master":
        success = validateLocal(target, name);
        break;
      case "all":
        success = validateMasters(name) || validateWorkers(name);
        break;
      case "workers":
        success = validateWorkers(name);
        break;
      case "masters":
        success = validateMasters(name);
        break;
      default:
        printHelp("Invalid target.");
    }

    return success ? 0 : 1;
  }

  /**
   * Validate Alluxio environment.
   *
   * @param args the arguments to specify which validation tasks to run
   */
  public static void main(String[] args) throws InterruptedException {
    System.exit(validate(args));
  }

  private ValidateEnv() {} // this class is not intended for instantiation
}
