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
  /**
   * Validates whether a network address is reachable.
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

  private Utils() {} // prevents instantiation
}
