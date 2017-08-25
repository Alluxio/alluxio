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

package alluxio.logserver;

import alluxio.Process;
import alluxio.ProcessUtils;

/**
 * Alluxio log server receiving logs pushed from Alluxio servers.
 */
public final class AlluxioLogServer {
  /**
   * Main entry point of {@link AlluxioLogServer}.
   *
   * @param args command line arguments that will be parsed to initialize {@link AlluxioLogServer}
   */
  public static void main(String[] args) {
    final AlluxioLogServerProcess process = new AlluxioLogServerProcess(args[0]);
    addShutdownHook(process);
    ProcessUtils.run(process);
  }

  /**
   * Add a shutdown hook that will be invoked when a signal is sent to this process.
   *
   * @param process the data structure representing the process to terminate
   */
  private static void addShutdownHook(final Process process) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          process.stop();
        } catch (Exception e) {
          System.exit(0);
        }
      }
    }
    );
  }

  /**
   * Private constructor to prevent user from instantiating any
   * {@link AlluxioLogServer} instance.
   */
  private AlluxioLogServer() {} // prevent instantiation
}
