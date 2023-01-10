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

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * The task result for the master stress tests.
 */
public final class MasterBenchTaskResult extends MasterBenchTaskResultBase<MasterBenchParameters> {
  private MasterBenchTaskResultStatistics mStatistics;

  private Map<String, MasterBenchTaskResultStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public MasterBenchTaskResult() {
    super();
    mStatistics = new MasterBenchTaskResultStatistics();
    mStatisticsPerMethod = new HashMap<>();
  }

  @Override
  void mergeResultStatistics(MasterBenchTaskResultBase<MasterBenchParameters> result)
      throws Exception {
    Preconditions.checkState(result instanceof MasterBenchTaskResult);
    MasterBenchTaskResult convertedResult = (MasterBenchTaskResult) result;

    mStatistics.merge(convertedResult.mStatistics);
    for (Map.Entry<String, MasterBenchTaskResultStatistics> entry :
        convertedResult.mStatisticsPerMethod.entrySet()) {
      final String key = entry.getKey();
      final MasterBenchTaskResultStatistics value = entry.getValue();

      if (!mStatisticsPerMethod.containsKey(key)) {
        mStatisticsPerMethod.put(key, value);
      } else {
        mStatisticsPerMethod.get(key).merge(value);
      }
    }
  }

  /**
   * Increments the number of successes by an amount.
   *
   * @param numSuccess the amount to increment by
   */
  public void incrementNumSuccess(long numSuccess) {
    mStatistics.mNumSuccess += numSuccess;
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

  /**
   * @return the statistics per method
   */
  public Map<String, MasterBenchTaskResultStatistics> getStatisticsPerMethod() {
    return mStatisticsPerMethod;
  }

  /**
   * @param statisticsPerMethod the statistics per method
   */
  public void setStatisticsPerMethod(Map<String, MasterBenchTaskResultStatistics>
                                         statisticsPerMethod) {
    mStatisticsPerMethod = statisticsPerMethod;
  }

  @Override
  public void putStatisticsForMethod(String method, MasterBenchTaskResultStatistics statistics) {
    mStatisticsPerMethod.put(method, statistics);
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<MasterBenchTaskResult> {
    @Override
    public MasterBenchSummary aggregate(Iterable<MasterBenchTaskResult> results) throws Exception {
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
