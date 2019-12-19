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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A utility class for running Unix commands.
 */
@ThreadSafe
public final class ShellUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ShellUtils.class);

  /**
   * Common shell OPTS to prevent stalling.
   * a. Disable StrictHostKeyChecking to prevent interactive prompt
   * b. Set timeout for establishing connection with host
   */
  public static final String COMMON_SSH_OPTS = "-o StrictHostKeyChecking=no -o ConnectTimeout=5";

  /** a Unix command to set permission. */
  public static final String SET_PERMISSION_COMMAND = "chmod";

  /** a Unix command for getting mount information. */
  public static final String MOUNT_COMMAND = "mount";

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

  /**
   * Gets system mount information. This method should only be attempted on Unix systems.
   *
   * @return system mount information
   */
  public static List<UnixMountInfo> getUnixMountInfo() throws IOException {
    Preconditions.checkState(OSUtils.isLinux() || OSUtils.isMacOS());
    String output = execCommand(MOUNT_COMMAND);
    List<UnixMountInfo> mountInfo = new ArrayList<>();
    for (String line : output.split("\n")) {
      mountInfo.add(parseMountInfo(line));
    }
    return mountInfo;
  }

  /**
   * @param line the line to parse
   * @return the parsed {@link UnixMountInfo}
   */
  public static UnixMountInfo parseMountInfo(String line) {
    // Example mount lines:
    // ramfs on /mnt/ramdisk type ramfs (rw,relatime,size=1gb)
    // map -hosts on /net (autofs, nosuid, automounted, nobrowse)
    UnixMountInfo.Builder builder = new UnixMountInfo.Builder();

    // First get and remove the mount type if it's provided.
    Matcher matcher = Pattern.compile(".* (type \\w+ ).*").matcher(line);
    String lineWithoutType;
    if (matcher.matches()) {
      String match = matcher.group(1);
      builder.setFsType(match.replace("type", "").trim());
      lineWithoutType = line.replace(match, "");
    } else {
      lineWithoutType = line;
    }
    // Now parse the rest
    matcher = Pattern.compile("(.*) on (.*) \\((.*)\\)").matcher(lineWithoutType);
    if (!matcher.matches()) {
      LOG.warn("Unable to parse output of '{}': {}", MOUNT_COMMAND, line);
      return builder.build();
    }
    builder.setDeviceSpec(matcher.group(1));
    builder.setMountPoint(matcher.group(2));
    builder.setOptions(parseUnixMountOptions(matcher.group(3)));
    return builder.build();
  }

  private static UnixMountInfo.Options parseUnixMountOptions(String line) {
    UnixMountInfo.Options.Builder builder = new UnixMountInfo.Options.Builder();
    for (String option : line.split(",")) {
      Matcher matcher = Pattern.compile("(.*)=(.*)").matcher(option.trim());
      if (matcher.matches() && matcher.group(1).equalsIgnoreCase("size")) {
        try {
          builder.setSize(FormatUtils.parseSpaceSize(matcher.group(2)));
        } catch (IllegalArgumentException e) {
          LOG.debug("Failed to parse mount point size", e);
        }
      }
    }
    return builder.build();
  }

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
    protected String run() throws ExitCodeException, IOException {
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
     * Runs a command and returns its stdout, stderr and exit code,
     * no matter it succeeds or not
     *
     * @return {@link CommandReturn} object representation of stdout, stderr and exit code
     */
    protected CommandReturn runTolerateFailure() throws IOException {
      Process process = new ProcessBuilder(mCommand).redirectErrorStream(true).start();

      BufferedReader inReader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));
      BufferedReader errReader =
              new BufferedReader(new InputStreamReader(process.getErrorStream()));

      try {
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
          LOG.warn(String.format("Failed to run command %s\nExit Code: %s\nStderr: %s",
                  Arrays.toString(mCommand), exitCode, stderr.toString()));
        }

        return new CommandReturn(exitCode, stdout.toString(), stderr.toString());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } finally {
        try {
          // JDK 7 tries to automatically drain the input streams for us
          // when the process exits, but since close is not synchronized,
          // it creates a race if we close the stream first and the same
          // fd is recycled. the stream draining thread will attempt to
          // drain that fd!! it may block, OOM, or cause bizarre behavior
          // see: https://bugs.openjdk.java.net/browse/JDK-8024521
          // issue is fixed in build 7u60
          InputStream stdoutStream = process.getInputStream();
          InputStream stderrStream = process.getErrorStream();
          synchronized (stdoutStream) {
            inReader.close();
          }
          synchronized (stderrStream) {
            errReader.close();
          }
        } catch (IOException e) {
          LOG.warn("Error while closing the input stream and error stream", e);
        }
        process.destroy();
      }
    }
  }

  @NotThreadSafe
  private static class SshCommand extends Command {
    private String mHostName;
    private String[] mCommand;

    private SshCommand(String[] execString) {
      this("localhost", execString);
    }

    private SshCommand(String hostname, String[] execString) {
      super(execString);
      mHostName = hostname;
      mCommand = new String[]{"bash", "-c",
              String.format("ssh %s %s %s", ShellUtils.COMMON_SSH_OPTS, hostname,
                      String.join(" ", execString))};
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
   * Object representation of a command execution
   */
  public static class CommandReturn {
    private int mExitCode;
    private String mStdOut;
    private String mStdErr;

    /**
     * Constructor
     */
    public CommandReturn(int code, String stdOut, String stdErr) {
      mExitCode = code;
      mStdOut = stdOut;
      mStdErr = stdErr;
    }

    /**
     * Gets the stdout content
     *
     * @return stdout content
     */
    public String getStdOut() {
      return mStdOut;
    }

    /**
     * Gets the stderr content
     *
     * @return stderr content
     */
    public String getStdErr() {
      return mStdErr;
    }

    /**
     * Gets the exit code
     *
     * @return exit code of execution
     */
    public int getExitCode() {
      return mExitCode;
    }

    /**
     * Formats the object to more readable format.
     * This is not done in toString() because stdout and stderr may be long.
     *
     * @return
     */
    public String getFormattedOutput() {
      return String.format("StatusCode:%s\nStdOut:\n%s\nStdErr:\n%s", getExitCode(),
              getStdOut(), getStdErr());
    }
  }

  /**
   * Static method to execute a shell command. The StdErr is not returned.
   * If execution failed
   *
   * @param cmd shell command to execute
   * @return the output of the executed command
   * @throws {@link IOException} in various situations:
   *  1. {@link ExitCodeException} when exit code is non-zero
   *  2. when the executable is not valid, i.e. running ls in Windows
   *  3. execution interrupted
   *  4. other normal reasons for IOException
   */
  public static String execCommand(String... cmd) throws IOException {
    return new Command(cmd).run();
  }

  /**
   * Static method to execute a shell command and tolerate non-zero exit code.
   * Preserve exit code, stderr and stdout in object.
   *
   * @param cmd shell command to execute
   * @return the output of the executed command
   * @throws {@link IOException} in various situations:
   *  1. when the executable is not valid, i.e. running ls in Windows
   *  2. execution interrupted
   *  3. other normal reasons for IOException
   */
  public static CommandReturn execCommandTolerateFailure(String... cmd) throws IOException {
    return new Command(cmd).runTolerateFailure();
  }

  /**
   * Static method to execute a shell command remotely via ssh.
   * Preserve exit code, stderr and stdout in object.
   * SSH must be password-less.
   *
   * @param hostname Hostname where the command should execute
   * @param cmd shell command to execute
   * @return the output of the executed command
   * @throws {@link IOException} in various situations:
   *  1. when the executable is not valid, i.e. running ls in Windows
   *  2. execution interrupted
   *  3. other normal reasons for IOException
   */
  public static CommandReturn sshExecCommandTolerateFailure(String hostname, String... cmd) throws IOException {
    return new SshCommand(hostname, cmd).runTolerateFailure();
  }

  private ShellUtils() {} // prevent instantiation
}
