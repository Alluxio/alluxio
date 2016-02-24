/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util;

import alluxio.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import javax.annotation.concurrent.ThreadSafe;
/**
 * A base class for running a Unix command.
 */
@ThreadSafe
public final class ShellUtils {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
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

  private Process mProcess; // sub process used to execute the command
  private int mExitCode;
  private String[] mCommand;
  private StringBuffer mOutput;

  private ShellUtils(String[] execString) {
    mCommand = execString.clone();
  }

  /** Checks to see if a command needs to be executed and execute command.
   *
   * @throws IOException if command ran failed
   */
  protected void run() throws IOException {
    mExitCode = 0; // reset for next run
    runCommand();
  }

  /**
   * Runs a command.
   *
   * @throws IOException if command ran failed
   */
  private void runCommand() throws IOException {
    ProcessBuilder builder = new ProcessBuilder(getExecString());

    mProcess = builder.start();

    BufferedReader inReader =
        new BufferedReader(new InputStreamReader(mProcess.getInputStream(),
            Charset.defaultCharset()));
    final StringBuffer errMsg = new StringBuffer();

    // read input streams as this would free up the buffers
    try {
      parseExecResult(inReader); // parse the output
      // clear the input stream buffer
      String line = inReader.readLine();
      while (line != null) {
        line = inReader.readLine();
      }
      // wait for the process to finish and check the exit code
      mExitCode = mProcess.waitFor();
      if (mExitCode != 0) {
        throw new ExitCodeException(mExitCode, errMsg.toString());
      }
    } catch (InterruptedException e) {
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

  /**
   * @return an array containing the command name & its parameters
   */
  protected String[] getExecString() {
    return mCommand;
  }

  /** Parse the execution result. */
  protected void parseExecResult(BufferedReader lines) throws IOException {
    mOutput = new StringBuffer();
    char[] buf = new char[512];
    int nRead;
    while ((nRead = lines.read(buf, 0, buf.length)) > 0) {
      mOutput.append(buf, 0, nRead);
    }
  }

  /** @return the output of the shell command. */
  public String getOutput() {
    return (mOutput == null) ? "" : mOutput.toString();
  }

  /**
   * Gets the current sub-process executing the given command.
   *
   * @return process executing the command
   */
  public Process getProcess() {
    return mProcess;
  }

  /**
   * Gets the exit code.
   *
   * @return the exit code of the process
   */
  public int getExitCode() {
    return mExitCode;
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
      return "ExitCodeException " + "exitCode=" + mExitCode + ": " + super.getMessage();
    }
  }

  /**
   * Static method to execute a shell command.
   *
   * @param cmd shell command to execute
   * @return the output of the executed command
   * @throws IOException if command ran failed
   */
  public static String execCommand(String... cmd) throws IOException {
    ShellUtils exec = new ShellUtils(cmd);
    exec.run();
    return exec.getOutput();
  }

}
