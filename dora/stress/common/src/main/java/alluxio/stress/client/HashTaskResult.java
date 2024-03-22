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

package alluxio.stress.client;

import alluxio.stress.BaseParameters;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Hash test all relevant results, including time, standard deviation, etc.
 */
public class HashTaskResult implements TaskResult {
  private List<String> mErrors;
  private HashParameters mParameters;
  private BaseParameters mBaseParameters;

  /**
   * All Hashing Algorithms Result.
   */
  private List<SingleTestResult> mAllTestsResult;

  /**
   * An empty constructor.
   * */
  public HashTaskResult() {
    mAllTestsResult = new ArrayList<>();
    mErrors = new ArrayList<>();
  }

  /**
   * The constructor used for serialization.
   *
   * @param errors the errors
   * */
  public HashTaskResult(@JsonProperty("errors") List<String> errors) {
    mErrors = errors;
  }

  /**
   * @param errorMsg an error msg to add
   * */
  public void addError(String errorMsg) {
    mErrors.add(errorMsg);
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new HashTaskResult.Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<HashTaskResult> {
    @Override
    public Summary aggregate(Iterable<HashTaskResult> results) throws Exception {
      return new HashTaskSummary(reduceList(results));
    }
  }

  /**
   * @return Errors if exist
   */
  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param baseParameters the {@link BaseParameters} to use
   * */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  @Override
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param errors the errors
   * */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  /**
   * @return the {@link HashParameters}
   * */
  public HashParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link HashParameters} to use
   * */
  public void setParameters(HashParameters parameters) {
    mParameters = parameters;
  }

  @Override
  public String toString() {
    String summary = "";
    for (SingleTestResult result: mAllTestsResult) {
      summary += String.format(
          "Hashing Algorithm=%s, Time Cost=%sms, Standard Deviation=%s, File Reallocated Num=%s%n",
          result.mHashAlgorithm, result.mTimeCost,
          result.mStandardDeviation, result.mFileReallocatedNum);
    }
    return summary;
  }

  /**
   * Results of a single hash test.
   */
  public static class SingleTestResult {
    /**
     * The chosen hashing algorithm.
     */
    private String mHashAlgorithm;

    /**
     * Hash testing takes time.
     */
    private long mTimeCost;
    /**
     * The standard deviation of the number of files allocated to each worker.
     */
    private double mStandardDeviation;
    /**
     * The number of workers reallocated after deleting a worker.
     */
    private int mFileReallocatedNum;

    /**
     * @param hashAlgorithm The chosen hashing algorithm
     * @param timeCost Hash testing takes time
     * @param standardDeviation The standard deviation of the number of files allocated to workers
     * @param fileReallocatedNum The number of workers reallocated after deleting a worker
     *
     */
    public SingleTestResult(String hashAlgorithm, long timeCost,
        double standardDeviation, int fileReallocatedNum) {
      mHashAlgorithm = hashAlgorithm;
      mTimeCost = timeCost;
      mStandardDeviation = standardDeviation;
      mFileReallocatedNum = fileReallocatedNum;
    }
  }

  /**
   * Add the result of one hash test to the total result.
   * @param result The result of one hash test
   */
  public void addSingleTestResult(SingleTestResult result) {
    mAllTestsResult.add(result);
  }

  /**
   * Reduce a list of {@link HashTaskResult} into one.
   *
   * @param results a list of results to combine
   * @return the combined result
   * */
  public static HashTaskResult reduceList(Iterable<HashTaskResult> results) {
    HashTaskResult aggreResult = new HashTaskResult();
    for (HashTaskResult r : results) {
      aggreResult = r;
    }
    return aggreResult;
  }
}
