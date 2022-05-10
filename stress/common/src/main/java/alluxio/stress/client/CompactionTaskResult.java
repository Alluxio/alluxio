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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;
import alluxio.stress.common.SummaryStatistics;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.DataFormatException;
import javax.annotation.Nullable;

/**
 * Task results for the compaction bench.
 */
public class CompactionTaskResult implements TaskResult {
  private BaseParameters mBaseParameters;
  private CompactionParameters mParameters;
  private List<String> mErrors;
  private CompactionTaskResultStatistics mStatistics;

  /**
   * Creates an empty result.
   */
  public CompactionTaskResult() {
    mErrors = new ArrayList<>();
    mStatistics = new CompactionTaskResultStatistics();
  }

  /**
   * Copy constructor.
   * @param from instance to copy from
   */
  public CompactionTaskResult(CompactionTaskResult from) {
    mBaseParameters = from.mBaseParameters;
    mParameters = from.mParameters;
    mErrors = from.mErrors;
    mStatistics = from.mStatistics;
  }

  @Nullable
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the {@link BaseParameters} to use
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the {@link CompactionParameters}
   */
  @Nullable
  public CompactionParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link CompactionParameters} to use
   */
  public void setParameters(CompactionParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @param errorMsg an error msg to add
   */
  public void addError(String errorMsg) {
    mErrors.add(errorMsg);
  }

  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the errors
   */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  /**
   * Merges a result into this one.
   * @param toMerge the result to merge
   */
  public void merge(CompactionTaskResult toMerge) throws Exception {
    mStatistics.merge(toMerge.getStatistics());
    mErrors.addAll(toMerge.getErrors());
  }

  /**
   * Increase number of successes by 1.
   */
  public void incrementNumSuccess() {
    mStatistics.mNumSuccess += 1;
  }

  /**
   * @return the result statistics
   */
  public CompactionTaskResultStatistics getStatistics() {
    return mStatistics;
  }

  /**
   * @param statistics the result statistics
   */
  public void setStatistics(CompactionTaskResultStatistics statistics) {
    mStatistics = statistics;
  }

  @Override
  public Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<CompactionTaskResult> {
    @Override
    public CompactionSummary aggregate(Iterable<CompactionTaskResult> results) throws Exception {
      Iterator<CompactionTaskResult> iterator = results.iterator();
      if (!iterator.hasNext()) {
        return new CompactionSummary(new CompactionTaskResult());
      }
      CompactionTaskResult mergedResult = new CompactionTaskResult(iterator.next());
      while (iterator.hasNext()) {
        mergedResult.merge(iterator.next());
      }
      return new CompactionSummary(mergedResult);
    }
  }

  /**
   * Summary of the benchmark results.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  public static class CompactionSummary implements Summary {
    @JsonProperty("baseParameters")
    private final BaseParameters mBaseParameters;
    @JsonProperty("parameters")
    private final CompactionParameters mParameters;
    @JsonProperty("numSuccess")
    private final long mNumSuccess;
    @JsonProperty("errors")
    private final List<String> mErrors;
    @JsonProperty("statistics")
    private final SummaryStatistics mSummaryStatistics;

    /**
     * Creates a summary from a result object.
     * @param mergedResult the final result
     */
    public CompactionSummary(CompactionTaskResult mergedResult) throws DataFormatException {
      mBaseParameters = mergedResult.getBaseParameters();
      mParameters = mergedResult.getParameters();
      mNumSuccess = mergedResult.getStatistics().mNumSuccess;
      mErrors = mergedResult.getErrors();
      mSummaryStatistics = mergedResult.getStatistics().toBenchSummaryStatistics();
    }

    @Override
    public GraphGenerator graphGenerator() {
      return null;
    }
  }
}
