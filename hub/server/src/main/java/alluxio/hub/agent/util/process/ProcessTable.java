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

package alluxio.hub.agent.util.process;

import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.apache.commons.lang3.SystemUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * A process table interface to abstract away the different ways platforms retrieve information
 * about system processes.
 */
public interface ProcessTable {

  /**
   * A factory for creating the process table instances.
   */
  class Factory {
    private static final ProcessTable INSTANCE = getProcessTable();

    static ProcessTable getProcessTable() {
      if (SystemUtils.IS_OS_LINUX) {
        return new LinuxProcessTable();
      } else {
        return new MacOsProcessTable();
      }
    }

    /**
     * @return the platoform-specific implementation of the process table
     */
    public static ProcessTable getInstance() {
      return INSTANCE;
    }
  }

  /**
   * Gets all the Java PIDs on the system that are readable by the current process.
   *
   * In testing on a half-utilized 32-core system, this method takes ~25ms to execute, so use it
   * sparingly.
   *
   * @return a list of PIDs that belong to java processes
   * @throws IOException if the /proc filesystem is not available
   */
  Stream<Integer> getJavaPids() throws IOException;

  /**
   * Get all PIDs running on the system.
   *
   * @return a stream of PIDs
   * @throws IOException if the program cannot read the process table
   */
  Stream<Integer> getAllPids() throws IOException;

  /**
   * Gets a Java PID running with the given class.
   *
   * @param possiblePids a list of PIDs to check. If null, it will check all PIDs on the system
   * @param clazz the main class of the process that should be running
   * @return a list of PIDs running with that main class
   */
  List<Integer> getJavaPid(@Nullable Stream<Integer> possiblePids, Class<?> clazz)
      throws IOException;

  /**
   * @param pid pid to test
   * @return whether the process is alive
   */
  boolean isProcessAlive(int pid);

  /**
   * @param pid pid to test
   * @return whether this process is currently running
   */
  default boolean isProcessDead(int pid) {
    return !isProcessAlive(pid);
  }

  /**
   * Stops a process. Blocks until it is dead, or throws an exception.
   *
   * @param pid the PID to kill
   * @param timeoutMs the total amount of time we should wait on the process dying before giving
   *                  up
   * @throws IOException If we can't read the process table or send a signal
   * @throws InterruptedException if we are interrupted waiting for the process to die
   * @throws TimeoutException if we wait too long for the process to die
   */
  default void stopProcess(int pid, int timeoutMs)
      throws IOException, InterruptedException, TimeoutException {
    Kill.kill(pid);
    CommonUtils.waitFor("pid " + pid + " to die", () -> isProcessDead(pid),
        WaitForOptions.defaults()
            .setTimeoutMs(timeoutMs)
            .setInterval(250));
  }
}
