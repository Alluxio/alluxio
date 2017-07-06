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
import java.util.TreeMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for checking Alluxio environment.
 */
public final class Check {
  private static final String USAGE = "USAGE: Check [task full name or prefix]\n\n"
      + "Check environment for Alluxio";

  private static final Options OPTIONS = new Options();

  private static final Map<String, CheckTask> TASK_MAP = initializeTaskMap();

  private static Map<String, CheckTask> initializeTaskMap() {
    Map<String, CheckTask> taskMap = new TreeMap<>();
    taskMap.put("master.port.available", new MasterRpcCheckTask());
    taskMap.put("master.workers.ssh.reachable", new MasterWorkersSshCheckTask());
    taskMap.put("worker.port.available", new WorkerRpcCheckTask());
    return taskMap;
  }

  private interface CheckTask {
    boolean check();
  }

  private static boolean isLocalPortAvailable(int port) {
    try (ServerSocket ss = new ServerSocket(port)) {
      ss.setReuseAddress(true);
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

  private static class MasterRpcCheckTask implements CheckTask {

    public MasterRpcCheckTask() {}

    @Override
    public boolean check() {
      if (isAlluxioRunning("alluxio.master.AlluxioMaster")) {
        System.out.println("Alluxio master is already running. Skip checking.");
        return true;
      }
      int port = Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
      if (!isLocalPortAvailable(port)) {
        System.err.format("Master RPC port %d is not available.%n", port);
        return false;
      }
      return true;
    }
  }

  private static class WorkerRpcCheckTask implements CheckTask {

    public WorkerRpcCheckTask() {}

    @Override
    public boolean check() {
      if (isAlluxioRunning("alluxio.worker.AlluxioWorker")) {
        System.out.println("Alluxio worker is already running. Skip checking.");
        return true;
      }
      int port = Configuration.getInt(PropertyKey.WORKER_RPC_PORT);
      if (!isLocalPortAvailable(port)) {
        System.err.format("Worker RPC port %d is not available.%n", port);
        return false;
      }
      return true;
    }
  }

  private static class MasterWorkersSshCheckTask implements CheckTask {

    public MasterWorkersSshCheckTask() {}

    @Override
    public boolean check() {
      String confDir = Configuration.get(PropertyKey.CONF_DIR);
      List<String> workerNames = null;
      try {
        workerNames = Files.readAllLines(Paths.get(confDir, "workers"), StandardCharsets.UTF_8);
      } catch (IOException e) {
        System.err.format("Unable to read workers file at %s/workers.%n", confDir);
        return false;
      }

      boolean hasUnreachableWorkers = false;

      for (String workerName : workerNames) {
        if (workerName.startsWith("#")) {
          continue;
        }
        if (!isAddressReachable(workerName, 22)) {
          System.err.format("Unable to reach ssh port 22 on worker %s.%n", workerName);
          hasUnreachableWorkers = true;
        }
      }
      return !hasUnreachableWorkers;
    }
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
   * Check environment.
   *
   * @param args list of arguments
   * @return 0 on success, 1 on failures
   */
  public static int check(String... args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(OPTIONS, args, true /* stopAtNonOption */);
    } catch (ParseException e) {
      printHelp("Unable to parse input args: " + e.getMessage());
      return 1;
    }

    Preconditions.checkNotNull(cmd, "Unable to parse input args");
    args = cmd.getArgs();
    if (args.length > 1) {
      printHelp("More arguments than expected");
      return 1;
    }

    String prefix = args.length > 0 ? args[0] : null;
    int checkCount = 0;
    int failureCount = 0;
    for (Map.Entry<String, CheckTask> taskEntry : TASK_MAP.entrySet()) {
      String taskName = taskEntry.getKey();
      if (prefix != null && !taskName.startsWith(prefix)) {
        continue;
      }
      System.out.format("Checking %s...", taskName);
      if (!taskEntry.getValue().check()) {
        failureCount++;
      } else {
        System.out.print("OK");
      }
      checkCount++;
      System.out.println();
    }

    if (failureCount > 0) {
      System.out.format("Check failed. Total failures: %d.%n", failureCount);
      return 1;
    }

    if (checkCount == 0) {
      System.out.format("No check matched \"%s\".%n", prefix);
      return 1;
    }

    System.out.println("Check succeeded.");
    return 0;
  }

  /**
   * Check Alluxio environment.
   *
   * @param args the arguments to specify which check tasks to run
   */
  public static void main(String[] args) {
    System.exit(check(args));
  }

  private Check() {} // this class is not intended for instantiation
}
