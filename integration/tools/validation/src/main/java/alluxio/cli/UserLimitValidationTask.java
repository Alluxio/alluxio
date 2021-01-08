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

package alluxio.cli;

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
  private final Integer mLowerBound;
  private final Integer mUpperBound;

  private UserLimitValidationTask(String command,
                                  Integer lowerBound, Integer upperBound) {
    mCommand = command;
    mLowerBound = lowerBound;
    mUpperBound = upperBound;
  }

  @Override
  public String getName() {
    return "ValidateUserLimit";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    ValidationUtils.State state = ValidationUtils.State.OK;
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    try {
      Process process = Runtime.getRuntime().exec(new String[] {"bash", "-c", mCommand});
      try (BufferedReader processOutputReader = new BufferedReader(
              new InputStreamReader(process.getInputStream()))) {
        String line = processOutputReader.readLine();
        if (line == null) {
          msg.append(String.format("Unable to check user limit for %s.%n", getName()));
          advice.append(String.format("Please check if you are able to run %s. ", mCommand));
          return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
                  msg.toString(), advice.toString());
        }

        if (line.equals("unlimited")) {
          msg.append(String.format("The user limit for %s is unlimited. ", getName()));
          if (mUpperBound != null) {
            state = ValidationUtils.State.WARNING;
            advice.append(String.format("The user limit should be less than %d. ", mUpperBound));
          }
          return new ValidationTaskResult(state, getName(),
                  msg.toString(), advice.toString());
        }

        int value = Integer.parseInt(line);
        if (mUpperBound != null && value > mUpperBound) {
          state = ValidationUtils.State.WARNING;
          msg.append(String.format("The user limit for %s is too large. The current value is %d. ",
                  getName(), value));
          advice.append(String.format("The user limit should be less than %d. ", mUpperBound));
        }

        if (mLowerBound != null && value < mLowerBound) {
          state = ValidationUtils.State.WARNING;
          msg.append(String.format("The user limit for %s is too small. The current value is %d. ",
                  getName(), value));
          advice.append(String.format("For production use, it should be bigger than %d%n",
                  mLowerBound));
        }

        return new ValidationTaskResult(state, getName(), msg.toString(), advice.toString());
      }
    } catch (IOException e) {
      msg.append(String.format("Unable to check user limit for %s: %s. ",
              getName(), e.getMessage()));
      msg.append(ValidationUtils.getErrorInfo(e));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              msg.toString(), advice.toString());
    }
  }

  /**
   * Creates a validation task for checking whether the user limit for number of open files
   * is within reasonable range.
   * @return the validation task for this check
   */
  public static AbstractValidationTask createOpenFilesLimitValidationTask() {
    return new UserLimitValidationTask("ulimit -n",
        NUMBER_OF_OPEN_FILES_MIN, NUMBER_OF_OPEN_FILES_MAX);
  }

  /**
   * Creates a validation task for checking whether the user limit for number of user processes
   * is within reasonable range.
   * @return the validation task for this check
   */
  public static AbstractValidationTask createUserProcessesLimitValidationTask() {
    return new UserLimitValidationTask("ulimit -u",
        NUMBER_OF_USER_PROCESSES_MIN, null);
  }
}
