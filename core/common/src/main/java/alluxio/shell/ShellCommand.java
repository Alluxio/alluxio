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

package alluxio.shell;

import alluxio.util.ShellUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Object representation of a shell command.
 */
@NotThreadSafe
public class ShellCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ShellCommand.class);

  protected String[] mCommand;

  /**
   * Creates a ShellCommand object with the command to exec.
   *
   * @param execString
   */
  public ShellCommand(String[] execString) {
    mCommand = execString.clone();
  }

  /**
   * Runs a command and returns its stdout on success.
   *
   * @return the output
   * @throws IOException if the command returns a non-zero exit code
   */
  public String run() throws IOException {
    Process process = new ProcessBuilder(mCommand).redirectErrorStream(true).start();

    BufferedReader inReader =
            new BufferedReader(new InputStreamReader(process.getInputStream(),
                    Charset.defaultCharset()));

    try {
      // read the output of the command
      StringBuilder output = new StringBuilder();
      String line = inReader.readLine();
      while (line != null) {
        output.append(line);
        output.append("\n");
        line = inReader.readLine();
      }
      // wait for the process to finish and check the exit code
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new ShellUtils.ExitCodeException(exitCode, output.toString());
      }
      return output.toString();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } finally {
      // close the input stream
      try {
        // JDK 7 tries to automatically drain the input streams for us
        // when the process exits, but since close is not synchronized,
        // it creates a race if we close the stream first and the same
        // fd is recycled. the stream draining thread will attempt to
        // drain that fd!! it may block, OOM, or cause bizarre behavior
        // see: https://bugs.openjdk.java.net/browse/JDK-8024521
        // issue is fixed in build 7u60
        InputStream stdout = process.getInputStream();
        synchronized (stdout) {
          inReader.close();
        }
      } catch (IOException e) {
        LOG.warn("Error while closing the input stream", e);
      }
      process.destroy();
    }
  }

  /**
   * Runs a command and returns its stdout, stderr and exit code.
   * No matter it succeeds or not.
   *
   * @return {@link CommandReturn} object representation of stdout, stderr and exit code
   */
  public CommandReturn runTolerateFailure() throws IOException {
    Process process = new ProcessBuilder(mCommand).start();
    CommandReturn cr = null;

    try (BufferedReader inReader =
                 new BufferedReader(new InputStreamReader(process.getInputStream()));
         BufferedReader errReader =
                 new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
      // read the output of the command
      StringBuilder stdout = new StringBuilder();
      String outLine = inReader.readLine();
      while (outLine != null) {
        stdout.append(outLine);
        stdout.append("\n");
        outLine = inReader.readLine();
      }

      // read the stderr of the command
      StringBuilder stderr = new StringBuilder();
      String errLine = errReader.readLine();
      while (errLine != null) {
        stderr.append(errLine);
        stderr.append("\n");
        errLine = inReader.readLine();
      }

      // wait for the process to finish and check the exit code
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        // log error instead of throwing exception
        LOG.warn(String.format("Failed to run command %s%nExit Code: %s%nStderr: %s",
                Arrays.toString(mCommand), exitCode, stderr.toString()));
      }

      cr = new CommandReturn(exitCode, stdout.toString(), stderr.toString());

      // destroy the process
      if (process != null) {
        process.destroy();
      }

      return cr;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } finally {
      if (process != null) {
        process.destroy();
      }
    }
  }
}
