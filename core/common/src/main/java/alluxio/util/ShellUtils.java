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

package alluxio.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A utility class for running Unix commands.
 */
@ThreadSafe
public final class ShellUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ShellUtils.class);

  /** a Unix command to set permission. */
  public static final String SET_PERMISSION_COMMAND = "chmod";

  /**
   * Gets a Unix command to get a given user's groups list.
   *
   * @param user the user name
   * @return the Unix command to get a given user's groups list
   */
  public static String[] getGroupsForUserCommand(final String user) {
    return new String[] {"bash", "-c", "id -gn " + user + "; id -Gn " + user};
  }

  /**
   * Returns a Unix command to set permission.
   *
   * @param perm the permission of file
   * @param filePath the file path
   * @return the Unix command to set permission
   */
  public static String[] getSetPermissionCommand(String perm, String filePath) {
    return new String[] {SET_PERMISSION_COMMAND, perm, filePath};
  }

  /** Token separator regex used to parse Shell tool outputs. */
  public static final String TOKEN_SEPARATOR_REGEX = "[ \t\n\r\f]";

  @NotThreadSafe
  private static class Command {
    private String[] mCommand;

    private Command(String[] execString) {
      mCommand = execString.clone();
    }

    /**
     * Runs a command and returns its stdout on success.
     *
     * @return the output
     * @throws ExitCodeException if the command returns a non-zero exit code
     */
    private String run() throws ExitCodeException, IOException {
      Process mProcess = new ProcessBuilder(mCommand).redirectErrorStream(true).start();

      BufferedReader inReader =
          new BufferedReader(new InputStreamReader(mProcess.getInputStream(),
              Charset.defaultCharset()));

      // read input streams as this would free up the buffers
      try {
        // read from the input stream buffer
        StringBuilder output = new StringBuilder();
        String line = inReader.readLine();
        while (line != null) {
          output.append(line);
          output.append("\n");
          line = inReader.readLine();
        }
        // wait for the process to finish and check the exit code
        int exitCode = mProcess.waitFor();
        if (exitCode != 0) {
          throw new ExitCodeException(exitCode, output.toString());
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
          InputStream stdout = mProcess.getInputStream();
          synchronized (stdout) {
            inReader.close();
          }
        } catch (IOException e) {
          LOG.warn("Error while closing the input stream", e);
        }
        mProcess.destroy();
      }
    }
  }

  /**
   * This is an IOException with exit code added.
   */
  public static class ExitCodeException extends IOException {

    private static final long serialVersionUID = -6520494427049734809L;

    private final int mExitCode;

    /**
     * Constructs an ExitCodeException.
     *
     * @param exitCode the exit code returns by shell
     * @param message the exception message
     */
    public ExitCodeException(int exitCode, String message) {
      super(message);
      mExitCode = exitCode;
    }

    /**
     * Gets the exit code.
     *
     * @return the exit code
     */
    public int getExitCode() {
      return mExitCode;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ExitCodeException ");
      sb.append("exitCode=").append(mExitCode).append(": ");
      sb.append(super.getMessage());
      return sb.toString();
    }
  }

  /**
   * Static method to execute a shell command.
   *
   * @param cmd shell command to execute
   * @return the output of the executed command
   */
  public static String execCommand(String... cmd) throws IOException {
    return new Command(cmd).run();
  }
}
