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

package alluxio.stress.master;

import alluxio.stress.TaskResult;
import alluxio.stress.common.SummaryStatistics;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

/**
 * The task result for the master stress tests.
 */
public class MasterBenchTaskResult extends MasterBenchTaskResultBase {
  private MasterBenchTaskResultStatistics mStatistics;

  /**
   * Increments the number of successes by an amount.
   *
   * @param numSuccess the amount to increment by
   */
  public void incrementNumSuccess(long numSuccess) {
    mStatistics.mNumSuccess += numSuccess;
  }

  /**
   * @return the array of max response times (in ns)
   */
  public long[] getMaxResponseTimeNs() {
    return mStatistics.mMaxResponseTimeNs;
  }

  /**
   * @param maxResponseTimeNs the array of max response times (in ns)
   */
  public void setMaxResponseTimeNs(long[] maxResponseTimeNs) {
    mStatistics.mMaxResponseTimeNs = maxResponseTimeNs;
  }

  /**
   * @return the statistics
   */
  public MasterBenchTaskResultStatistics getStatistics() {
    return mStatistics;
  }

  /**
   * @param statistics the statistics
   */
  public void setStatistics(MasterBenchTaskResultStatistics statistics) {
    mStatistics = statistics;
  }

  @Override
  void mergeResultStatistics(MasterBenchTaskResultBase result) throws Exception {
    Preconditions.checkState(result instanceof MasterBenchTaskResult);
    MasterBenchTaskResult convertedResult = (MasterBenchTaskResult) result;
    mStatistics.merge(convertedResult.mStatistics);
  }

  @Override
  long getTotalSuccess() {
    return mStatistics.mNumSuccess;
  }

  @Override
  List<SummaryStatistics> getSummaryStatistics() throws DataFormatException {
    return Collections.singletonList(mStatistics.toBenchSummaryStatistics());
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator
      implements TaskResult.Aggregator<MasterBenchTaskResult> {
    @Override
    public MasterBenchSummary aggregate(Iterable<MasterBenchTaskResult> results)
        throws Exception {
      Map<String, MasterBenchTaskResult> nodes = new HashMap<>();
      MasterBenchTaskResult mergingTaskResult = null;

      for (MasterBenchTaskResult taskResult : results) {
        nodes.put(taskResult.getBaseParameters().mId, taskResult);

        if (mergingTaskResult == null) {
          mergingTaskResult = taskResult;
          continue;
        }
        mergingTaskResult.aggregateByWorker(taskResult);
      }

      return new MasterBenchSummary(mergingTaskResult, nodes);
    }
  }
}
