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

import alluxio.AlluxioURI;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

/**
 * Utilities to run the validation tests.
 */
public final class ValidationUtils {

  private ValidationUtils() {} // prevent instantiation

  /**
   * Task State.
   */
  public enum State {
    OK,
    WARNING,
    FAILED,
    SKIPPED
  }

  /**
   * Represents the result of a given task.
   */
  public static class TaskResult implements Serializable {
    private static final long serialVersionUID = -2746652850515278409L;

    State mState = State.OK;
    String mName = "";
    String mDesc = "";
    // Output stores stdout if test passed or stderr if error thrown
    String mOutput = "";
    String mAdvice = "";

    /**
     * Creates a new {@link TaskResult}.
     *
     * @param state task state
     * @param name task name
     * @param output task output
     * @param advice task advice
     */
    public TaskResult(State state, String name, String output, String advice) {
      mState = state;
      mName = name;
      mOutput = output;
      mAdvice = advice;
    }

    /**
     * Creates a new {@link TaskResult}.
     *
     * @param state task state
     * @param name task name
     * @param desc task description
     * @param output task output
     * @param advice task advice
     */
    public TaskResult(State state, String name, String desc, String output, String advice) {
      mState = state;
      mName = name;
      mDesc = desc;
      mOutput = output;
      mAdvice = advice;
    }

    /**
     * Creates a new {@link TaskResult}.
     */
    public TaskResult() {}

    /**
     * Sets task state.
     *
     * @param state state to set
     * @return the task result
     */
    public TaskResult setState(State state) {
      mState = state;
      return this;
    }

    /**
     * Sets task name.
     *
     * @param name name to set
     * @return the task result
     */
    public TaskResult setName(String name) {
      mName = name;
      return this;
    }

    /**
     * Sets task name.
     *
     * @param desc description to set
     * @return the task result
     */
    public TaskResult setDesc(String desc) {
      mDesc = desc;
      return this;
    }

    /**
     * Sets task output.
     *
     * @param output output to set
     * @return the task result
     */
    public TaskResult setOutput(String output) {
      mOutput = output;
      return this;
    }

    /**
     * Sets task advice.
     *
     * @param advice advice to set
     * @return the task result
     */
    public TaskResult setAdvice(String advice) {
      mAdvice = advice;
      return this;
    }

    /**
     * @return task state
     */
    public State getState() {
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

  /**
   * Convert a throwable into stacktrace, so it can be put in the TaskResult.
   *
   * @param t the throwable
   * @return the formatted trace for the throwable
   * */
  public static String getErrorInfo(Throwable t) {
    StringWriter errors = new StringWriter();
    t.printStackTrace(new PrintWriter(errors));
    return errors.toString();
  }

  /**
   * Checks if a path is HDFS.
   *
   * @param path the UFS path
   * @return true if the path is HDFS
   * */
  public static boolean isHdfsScheme(String path) {
    String scheme = new AlluxioURI(path).getScheme();
    if (scheme == null || !scheme.startsWith("hdfs")) {
      return false;
    }
    return true;
  }
}
