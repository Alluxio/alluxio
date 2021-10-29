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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.util.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * An object which can launch new Alluxio processes.
 *
 * Under the hood, this object will use the {@link PropertyKey#HOME} variable to determine the
 * root path to search for the {@code alluxio-start.sh} script.
 */
public class ProcessLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessLauncher.class);

  private static final Path START_SCRIPT = Paths.get("bin", "alluxio-start.sh");

  private final Path mScript;

  /**
   * Creates a new instances of {@link ProcessLauncher}.
   *
   * @param conf configuration to use
   * @throws IOException if the {@code alluxio-start.sh} script cannot be found
   */
  public ProcessLauncher(AlluxioConfiguration conf) throws IOException {
    mScript = Paths.get(conf.get(PropertyKey.HOME)).resolve(START_SCRIPT);
    if (!Files.exists(mScript)) {
      throw new IOException("Alluxio start script at " + mScript.toString() + " does not exist");
    }
  }

  /**
   * Starts an Alluxio process of the given type.
   *
   * @param type the alluxio process type to launch
   *
   * @throws IOException if there is an issue creating the subprocess
   * @throws InterruptedException if the thread is interrupted while waiting for the process to
   *                              start
   */
  public void start(AlluxioNodeType type) throws IOException, InterruptedException {
    String cmd = type.toString().toLowerCase();
    Process p = Runtime.getRuntime().exec(String.format("%s %s", mScript, cmd));
    SequenceInputStream is = new SequenceInputStream(p.getInputStream(), p.getErrorStream());
    BufferedReader output = new BufferedReader(new InputStreamReader(is));
    try {
      List<String> lines = output.lines().collect(Collectors.toList());
      while (true) {
        if (p.waitFor(500, TimeUnit.MILLISECONDS)) {
          break;
        }
      }
      LOG.debug("process launch output for {}:\n{}", type, lines);
    } catch (UncheckedIOException e) {
      LogUtils.warnWithException(LOG, "Failed to launch Alluxio process {}",
          type, e);
    } finally {
      // Clean after launching the process
      try {
        is.close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stdin writer when starting {}", type, e);
      }
      try {
        output.close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stdout/err reader when starting {}", type, e);
      }
    }
  }

  /**
   * Stop an Alluxio process of the given type.
   *
   * @param type the alluxio process type to stop
   *
   * @throws IOException if there is an issue creating the subprocess
   * @throws InterruptedException if the thread is interrupted while waiting for the process to
   *                              stop
   */
  public void stop(AlluxioNodeType type) throws IOException, InterruptedException,
      TimeoutException {
    ProcessTable ps = ProcessTable.Factory.getInstance();
    int pid = NodeStatus.getPid(type, ps, null);
    // Process not found in table, nothing to do.
    if (pid < 0) {
      return;
    }
    ps.stopProcess(pid, 30000);
  }
}
