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
import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;
import alluxio.stress.common.TaskResultStatistics;
import alluxio.stress.rpc.RpcTaskResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

public class CompactionTaskResult implements TaskResult, Summary {
  private BaseParameters mBaseParameters;
  private CompactionParameters mParameters;
  private List<String> mErrors;
  private CompactionTaskResultStatistics mStatistics;

  public CompactionTaskResult() {
    mErrors = new ArrayList<>();
    mStatistics = new CompactionTaskResultStatistics();
  }

  /**
   * @return the {@link BaseParameters}
   */
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

  /**
   * @return all the error messages
   */
  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the errors
   */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  public void merge(CompactionTaskResult toMerge) throws Exception {
    mStatistics.merge(toMerge.getStatistics());
    mErrors.addAll(toMerge.getErrors());
  }

  public void incrementNumSuccess() {
    mStatistics.mNumSuccess += 1;
  }

  public CompactionTaskResultStatistics getStatistics() {
    return mStatistics;
  }

  public void setStatistics(CompactionTaskResultStatistics statistics) {
    mStatistics = statistics;
  }

  @Override
  public Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<CompactionTaskResult> {
    @Override
    public CompactionTaskResult aggregate(Iterable<CompactionTaskResult> results) throws Exception {
      Iterator<CompactionTaskResult> iterator = results.iterator();
      if (!iterator.hasNext()) {
        return new CompactionTaskResult();
      }
      CompactionTaskResult aggreResult = iterator.next();
      while (iterator.hasNext()) {
        aggreResult.merge(iterator.next());
      }
      return aggreResult;
    }
  }

  @Override
  public GraphGenerator graphGenerator() {
    return null;
  }
}
