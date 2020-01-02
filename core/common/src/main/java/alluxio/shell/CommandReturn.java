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

/**
 * Object representation of a command execution.
 */
public class CommandReturn {
  private int mExitCode;
  private String mStdOut;
  private String mStdErr;

  /**
   * Creates object from the contents.
   *
   * @param code exit code
   * @param stdOut stdout content
   * @param stdErr stderr content
   */
  public CommandReturn(int code, String stdOut, String stdErr) {
    mExitCode = code;
    mStdOut = stdOut;
    mStdErr = stdErr;
  }

  /**
   * Gets the stdout content.
   *
   * @return stdout content
   */
  public String getStdOut() {
    return mStdOut;
  }

  /**
   * Gets the stderr content.
   *
   * @return stderr content
   */
  public String getStdErr() {
    return mStdErr;
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
   * Formats the object to more readable format.
   * This is not done in toString() because stdout and stderr may be long.
   *
   * @return pretty formatted output
   */
  public String getFormattedOutput() {
    return String.format("StatusCode:%s%nStdOut:%n%s%nStdErr:%n%s", getExitCode(),
            getStdOut(), getStdErr());
  }
}
