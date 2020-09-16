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

import java.io.Serializable;

/**
 * Represents the result of a given task.
 */
public class ValidationTaskResult implements Serializable {
  private static final long serialVersionUID = -2746652850515278409L;

  ValidationUtils.State mState = ValidationUtils.State.OK;
  String mName = "";
  String mDesc = "";
  // Output stores stdout if test passed or stderr if error thrown
  String mOutput = "";
  String mAdvice = "";

  /**
   * Creates a new {@link ValidationTaskResult}.
   *
   * @param state  task state
   * @param name   task name
   * @param output task output
   * @param advice task advice
   */
  public ValidationTaskResult(ValidationUtils.State state, String name, String output,
      String advice) {
    mState = state;
    mName = name;
    mOutput = output;
    mAdvice = advice;
  }

  /**
   * Creates a new {@link ValidationTaskResult}.
   *
   * @param state  task state
   * @param name   task name
   * @param desc   task description
   * @param output task output
   * @param advice task advice
   */
  public ValidationTaskResult(ValidationUtils.State state, String name, String desc, String output,
      String advice) {
    mState = state;
    mName = name;
    mDesc = desc;
    mOutput = output;
    mAdvice = advice;
  }

  /**
   * Creates a new {@link ValidationTaskResult}.
   */
  public ValidationTaskResult() {
  }

  /**
   * Sets task state.
   *
   * @param state state to set
   * @return the task result
   */
  public ValidationTaskResult setState(ValidationUtils.State state) {
    mState = state;
    return this;
  }

  /**
   * Sets task name.
   *
   * @param name name to set
   * @return the task result
   */
  public ValidationTaskResult setName(String name) {
    mName = name;
    return this;
  }

  /**
   * Sets task name.
   *
   * @param desc description to set
   * @return the task result
   */
  public ValidationTaskResult setDesc(String desc) {
    mDesc = desc;
    return this;
  }

  /**
   * Sets task output.
   *
   * @param output output to set
   * @return the task result
   */
  public ValidationTaskResult setOutput(String output) {
    mOutput = output;
    return this;
  }

  /**
   * Sets task advice.
   *
   * @param advice advice to set
   * @return the task result
   */
  public ValidationTaskResult setAdvice(String advice) {
    mAdvice = advice;
    return this;
  }

  /**
   * @return task state
   */
  public ValidationUtils.State getState() {
    return mState;
  }

  /**
   * @return task name
   */
  public String getName() {
    return mName;
  }

  /**
   * @return task description
   */
  public String getDesc() {
    return mDesc;
  }

  /**
   * @return task result
   */
  public String getResult() {
    return mOutput;
  }

  /**
   * @return task advice
   */
  public String getAdvice() {
    return mAdvice;
  }

  @Override
  public String toString() {
    return String.format("%s: %s%nOutput: %s%nAdvice: %s%n", mName, mState, mOutput, mAdvice);
  }
}
