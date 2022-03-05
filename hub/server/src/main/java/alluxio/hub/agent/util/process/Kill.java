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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

/**
 * Sends signals to processes using the system {@code kill} utility.
 */
public class Kill {
  private static final Logger LOG = LoggerFactory.getLogger(Kill.class);

  /**
   * An enum representing a mapping of available signal names and corresponding value.
   */
  enum Signal {
    SIGKILL(9),
    SIGTERM(15),
    ;
    private final int mSignal;

    /**
     * Create a new signal.
     *
     * @param i the signal associated with this
     */
    Signal(int i) {
      mSignal = i;
    }

    /**
     * @return the signal associated with this signal
     */
    public int getSigNum() {
      return mSignal;
    }
  }

  private Kill() {
  }

  /**
   * Sends the desired signal to a given process.
   *
   * @param pid the pid to send the signal to
   * @param signal the signal to send
   * @throws IOException if kill couldn't complete successfully
   * @throws InterruptedException when interrupted waiting for subprocess to finish
   */
  public static void kill(int pid, Signal signal) throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec(String.format("kill -%d %s", signal.getSigNum(), pid));
    // in case there is more stdout, we should read it to make sure that the buffers aren't filled
    BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    try {
      String err = errReader.lines()
          .collect(Collectors.joining("\n"));
      String unused = outReader.lines()
          .collect(Collectors.joining("\n"));
      if (p.waitFor() != 0) {
        throw new IOException("Failed to send signal to process: " + err);
      }
    } finally {
      try {
        outReader.close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stdout reader when sending signal {} to pid {}",
            signal, pid, e);
      }
      try {
        errReader.close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stderr reader when sending signal {} to pid {}",
            signal, pid, e);
      }
      try {
        p.getOutputStream().close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stdin stream when sending signal {} to pid {}",
            signal, pid, e);
      }
    }
  }

  /**
   * Sends an {@link Signal#SIGTERM} to the given process.
   *
   * @param pid the pid to send the signal to
   * @throws IOException if kill couldn't send the signal properly
   * @throws InterruptedException if we are interrupted waiting for the subprocess to finish
   * @see #kill(int, Signal)
   */
  public static void kill(int pid) throws IOException, InterruptedException {
    kill(pid, Signal.SIGTERM);
  }

  /**
   * Sends an {@link Signal#SIGKILL} to the given process.
   *
   * @param pid the pid to send the signal to
   * @throws IOException if kill couldn't send the signal properly
   * @throws InterruptedException if we are interrupted waiting for the subprocess to finish
   * @see #kill(int, Signal)
   */
  public static void kill9(int pid) throws IOException, InterruptedException {
    kill(pid, Signal.SIGKILL);
  }
}
