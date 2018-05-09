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

package alluxio.cli.validation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Task for validating system limit for current user.
 */
public final class UserLimitValidationTask extends AbstractValidationTask {
  private static final int NUMBER_OF_OPEN_FILES_MIN = 16384;
  private static final int NUMBER_OF_OPEN_FILES_MAX = 800000;
  private static final int NUMBER_OF_USER_PROCESSES_MIN = 16384;
  private final String mCommand;
  private final String mName;
  private final Integer mLowerBound;
  private final Integer mUpperBound;

  private UserLimitValidationTask(String name, String command,
                                  Integer lowerBound, Integer upperBound) {
    mName = name;
    mCommand = command;
    mLowerBound = lowerBound;
    mUpperBound = upperBound;
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    try {
      Process process = Runtime.getRuntime().exec(new String[] {"bash", "-c", mCommand});
      try (BufferedReader processOutputReader = new BufferedReader(
          new InputStreamReader(process.getInputStream()))) {
        String line = processOutputReader.readLine();
        if (line == null) {
          System.err.format("Unable to check user limit for %s.%n", mName);
          return TaskResult.FAILED;
        }

        if (line.equals("unlimited")) {
          if (mUpperBound != null) {
            System.err.format("The user limit for %s is unlimited. It should be less than %d%n",
                mName, mUpperBound);
            return TaskResult.WARNING;
          }

          return TaskResult.OK;
        }

        int value = Integer.parseInt(line);
        if (mUpperBound != null && value > mUpperBound) {
          System.err.format("The user limit for %s is too large. The current value is %d. "
              + "It should be less than %d%n", mName, value, mUpperBound);
          return TaskResult.WARNING;
        }

        if (mLowerBound != null && value < mLowerBound) {
          System.err.format("The user limit for %s is too small. The current value is %d. "
              + "For production use, it should be bigger than %d%n", mName, value, mLowerBound);
          return TaskResult.WARNING;
        }

        return TaskResult.OK;
      }
    } catch (IOException e) {
      System.err.format("Unable to check user limit for %s: %s.%n", mName, e.getMessage());
      return TaskResult.FAILED;
    }
  }

  /**
   * Creates a validation task for checking whether the user limit for number of open files
   * is within reasonable range.
   * @return the validation task for this check
   */
  public static ValidationTask createOpenFilesLimitValidationTask() {
    return new UserLimitValidationTask("number of open files", "ulimit -n",
        NUMBER_OF_OPEN_FILES_MIN, NUMBER_OF_OPEN_FILES_MAX);
  }

  /**
   * Creates a validation task for checking whether the user limit for number of user processes
   * is within reasonable range.
   * @return the validation task for this check
   */
  public static ValidationTask createUserProcessesLimitValidationTask() {
    return new UserLimitValidationTask("number of user processes", "ulimit -u",
        NUMBER_OF_USER_PROCESSES_MIN, null);
  }
}
