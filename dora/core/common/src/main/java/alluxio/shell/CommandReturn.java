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

import java.util.Arrays;

/**
 * Object representation of a command execution.
 */
public class CommandReturn {
  private int mExitCode;
  private String[] mCmd;
  private String mOutput;

  /**
   * Creates object from the contents.
   *
   * @param code exit code
   * @param output stdout content
   */
  public CommandReturn(int code, String output) {
    mExitCode = code;
    mCmd = new String[]{};
    mOutput = output;
  }

  /**
   * Creates object from the contents.
   * Copy the command array.
   *
   * @param code exit code
   * @param cmd the command executed
   * @param output stdout content
   */
  public CommandReturn(int code, String[] cmd, String output) {
    mExitCode = code;
    mCmd = Arrays.copyOfRange(cmd, 0, cmd.length);
    mOutput = output;
  }

  /**
   * Gets the exit code.
   *
   * @return exit code of execution
   */
  public int getExitCode() {
    return mExitCode;
  }

  /**
   * Gets the command run.
   *
   * @return the command
   * */
  public String[] getCmd() {
    return mCmd;
  }

  /**
   * Gets the stdout content.
   *
   * @return stdout content
   */
  public String getOutput() {
    return mOutput;
  }

  /**
   * Formats the object to more readable format.
   * This is not done in toString() because stdout and stderr may be long.
   *
   * @return pretty formatted output
   */
  public String getFormattedOutput() {
    return String.format("ExitCode:%s%nCommand:%s%nOutput:%n%s", getExitCode(),
            Arrays.toString(getCmd()), getOutput());
  }
}
