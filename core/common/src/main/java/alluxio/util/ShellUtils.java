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
      LOG.debug("Unable to parse output of 'mount': {}", line);
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
          LOG.debug("Failed to parse mount point size: {}", e);
        }
      }
    }
    return builder.build();
  }

  @NotThreadSafe
  private static final class Command {
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

  private ShellUtils() {} // prevent instantiation
}
