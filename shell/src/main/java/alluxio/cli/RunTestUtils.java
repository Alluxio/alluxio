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

import alluxio.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Utilities to run the examples.
 */
public final class RunTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RunTestUtils.class);

  private RunTestUtils() {} // prevent instantiation

  /**
   * Prints information of the test result.
   *
   * @param pass the test result
   */
  public static void printPassInfo(boolean pass) {
    if (pass) {
      System.out.println(Constants.ANSI_GREEN + "Passed the test!" + Constants.ANSI_RESET);
    } else {
      System.out.println(Constants.ANSI_RED + "Failed the test!" + Constants.ANSI_RESET);
    }
  }

  /**
   * Runs an example.
   *
   * @param example the example to run
   * @return whether the example completes
   */
  public static boolean runExample(final Callable<Boolean> example) {
    boolean result;
    try {
      result = example.call();
    } catch (Exception e) {
      LOG.error("Exception running test: " + example, e);
      result = false;
    }
    RunTestUtils.printPassInfo(result);
    return result;
  }

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
    TaskResult(State state, String name, String output, String advice) {
      mState = state;
      mName = name;
      mOutput = output;
      mAdvice = advice;
    }

    /**
     * Creates a new {@link TaskResult}.
     */
    TaskResult() {}

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
  }
}
