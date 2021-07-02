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

  private final String[] mCommand;

  /**
   * Creates a ShellCommand object with the command to exec.
   *
   * @param execString shell command
   */
  public ShellCommand(String[] execString) {
    mCommand = execString.clone();
  }

  /**
   * Runs a command and returns its stdout on success.
   * Stderr is redirected to stdout.
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
        LOG.warn(String.format("Error while closing the input stream of process %s: %s",
                process, e.getMessage()));
      }
      process.destroy();
    }
  }

  /**
   * Runs a command and returns its output and exit code in Object.
   * Preserves the output when the execution fails.
   * If the command execution fails (not by an interrupt),
   * try to wrap the Exception in the {@link CommandReturn}.
   * Stderr is redirected to stdout.
   *
   * @return {@link CommandReturn} object representation of stdout, stderr and exit code
   */
  public CommandReturn runWithOutput() throws IOException {
    Process process = null;
    BufferedReader inReader = null;
    try {
      process = new ProcessBuilder(mCommand).redirectErrorStream(true).start();
      inReader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));

      // read the output of the command
      StringBuilder stdout = new StringBuilder();
      String outLine = inReader.readLine();
      while (outLine != null) {
        stdout.append(outLine);
        stdout.append("\n");
        outLine = inReader.readLine();
      }

      // wait for the process to finish and check the exit code
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        // log error instead of throwing exception
        LOG.warn(String.format("Non-zero exit code (%d) from command %s",
                exitCode, Arrays.toString(mCommand)));
      }

      CommandReturn cr = new CommandReturn(exitCode, mCommand, stdout.toString());

      // destroy the process
      if (process != null) {
        process.destroy();
      }

      return cr;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (Exception e) {
      return new CommandReturn(1, String.format("Command %s failed, exception is %s",
              Arrays.toString(mCommand), e.getMessage()));
    } finally {
      if (inReader != null) {
        inReader.close();
      }
      if (process != null) {
        process.destroy();
      }
    }
  }

  /**
   * Converts the command to string repr.
   *
   * @return the shell command
   * */
  public String toString() {
    return Arrays.toString(mCommand);
  }

  /**
   * Gets the command. The original array is immutable.
   *
   * @return a copy of the command string array
   * */
  public String[] getCommand() {
    return Arrays.copyOfRange(mCommand, 0, mCommand.length);
  }
}
