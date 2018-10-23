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

package alluxio.cli.validation;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.ShellUtils;
import alluxio.util.UnixMountInfo;

import com.google.common.base.Optional;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for validating Alluxio environment.
 */
public final class Utils {
  private static final String LINE_SEPARATOR = System.getProperty("line.separator").toString();

  /**
   * Validates whether a network address is reachable.
   *
   * @param hostname host name of the network address
   * @param port port of the network address
   * @return whether the network address is reachable
   */
  public static boolean isAddressReachable(String hostname, int port) {
    try (Socket socket = new Socket(hostname, port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Checks whether an Alluxio service is running.
   *
   * @param className class name of the Alluxio service
   * @return whether the Alluxio service is running
   */
  public static boolean isAlluxioRunning(String className) {
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

  /**
   * Reads a list of nodes from given file name ignoring comments and empty lines.
   * Can be used to read conf/workers or conf/masters.
   * @param fileName name of a file that contains the list of the nodes
   * @return list of the node names, null when file fails to read
   */
  public static List<String> readNodeList(String fileName) {
    String confDir = Configuration.get(PropertyKey.CONF_DIR);
    List<String> lines;
    try {
      lines = Files.readAllLines(Paths.get(confDir, fileName), StandardCharsets.UTF_8);
    } catch (IOException e) {
      System.err.format("Failed to read file %s/%s.%n", confDir, fileName);
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

  /**
   * Checks whether a path is the mounting point of a RAM disk volume.
   *
   * @param path  a string represents the path to be checked
   * @param fsTypes an array of strings represents expected file system type
   * @return true if the path is the mounting point of volume with one of the given fsTypes,
   *         false otherwise
   * @throws IOException if the function fails to get the mount information of the system
   */
  public static boolean isMountingPoint(String path, String[] fsTypes) throws IOException {
    List<UnixMountInfo> infoList = ShellUtils.getUnixMountInfo();
    for (UnixMountInfo info : infoList) {
      Optional<String> mountPoint = info.getMountPoint();
      Optional<String> fsType = info.getFsType();
      if (mountPoint.isPresent() && mountPoint.get().equals(path) && fsType.isPresent()) {
        for (String expectedType : fsTypes) {
          if (fsType.get().equalsIgnoreCase(expectedType)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Executes a command in another process and check for its execution result.
   *
   * @param args array representation of the command to execute
   * @return {@link ProcessExecutionResult} including the process's exit value, output and error
   */
  public static ProcessExecutionResult getResultFromProcess(String[] args) {
    try {
      Process process = Runtime.getRuntime().exec(args);
      StringBuilder outputSb = new StringBuilder();
      try (BufferedReader processOutputReader = new BufferedReader(
          new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = processOutputReader.readLine()) != null) {
          outputSb.append(line);
          outputSb.append(LINE_SEPARATOR);
        }
      }
      StringBuilder errorSb = new StringBuilder();
      try (BufferedReader processErrorReader = new BufferedReader(
          new InputStreamReader(process.getErrorStream()))) {
        String line;
        while ((line = processErrorReader.readLine()) != null) {
          errorSb.append(line);
          errorSb.append(LINE_SEPARATOR);
        }
      }
      process.waitFor();
      return new ProcessExecutionResult(process.exitValue(), outputSb.toString().trim(),
          errorSb.toString().trim());
    } catch (IOException e) {
      System.err.println("Failed to execute command.");
      return new ProcessExecutionResult(1, "", e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Interrupted.");
      return new ProcessExecutionResult(1, "", e.getMessage());
    }
  }

  /**
   * Class representing the return value of process execution.
   */
  static class ProcessExecutionResult {
    /** The exit value of the process. */
    private final int mExitValue;
    /** The output of the process. */
    private final String mOutput;
    /** The error of the process. */
    private final String mError;

    public ProcessExecutionResult(int val, String output, String error) {
      mExitValue = val;
      mOutput = output;
      mError = error;
    }

    public int getExitValue() {
      return mExitValue;
    }

    public String getOutput() {
      return mOutput;
    }

    public String getError() {
      return mError;
    }
  }

  private Utils() {} // prevents instantiation
}
