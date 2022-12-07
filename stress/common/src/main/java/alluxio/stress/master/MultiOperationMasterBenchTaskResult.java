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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The task result for the multi-operation master stress tests.
 */
public final class MultiOperationMasterBenchTaskResult
    extends MasterBenchTaskResultBase<MultiOperationMasterBenchParameters> {
  private final List<MasterBenchTaskResultStatistics> mStatistics;

  private final Map<String, MasterBenchTaskResultStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   * @param operations the operations
   */
  public MultiOperationMasterBenchTaskResult(Operation[] operations) {
    super();
    mStatisticsPerMethod = new HashMap<>();
    mStatistics = new ArrayList<>();
    for (int i = 0; i < operations.length; ++i) {
      mStatistics.add(new MasterBenchTaskResultStatistics(operations[i]));
    }
  }

  /**
   * Empty constructor for json deserialization.
   */
  public MultiOperationMasterBenchTaskResult() {
    super();
    mStatisticsPerMethod = new HashMap<>();
    mStatistics = new ArrayList<>();
  }

  @Override
  void mergeResultStatistics(MasterBenchTaskResultBase<MultiOperationMasterBenchParameters> result)
      throws Exception {
    Preconditions.checkState(result instanceof MultiOperationMasterBenchTaskResult);
    MultiOperationMasterBenchTaskResult convertedResult =
        (MultiOperationMasterBenchTaskResult) result;

    for (int i = 0; i < mStatistics.size(); i++) {
      mStatistics.get(i).merge(convertedResult.mStatistics.get(i));
    }
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
   * @param operationIndex the operation index
   * @param numSuccess the amount to increment by
   */
  public void incrementNumSuccess(int operationIndex, long numSuccess) {
    mStatistics.get(operationIndex).mNumSuccess += numSuccess;
  }

  /**
   * @param operationIndex the operation index
   * @return the statistics
   */
  public MasterBenchTaskResultStatistics getStatistics(int operationIndex) {
    return mStatistics.get(operationIndex);
  }

  /**
   * @return the statistics
   */
  public List<MasterBenchTaskResultStatistics> getAllStatistics() {
    return mStatistics;
  }

  /**
   * @return the statistics per method
   */
  public Map<String, MasterBenchTaskResultStatistics> getStatisticsPerMethod() {
    return mStatisticsPerMethod;
  }

  @Override
  public void putStatisticsForMethod(String method, MasterBenchTaskResultStatistics statistics) {
    mStatisticsPerMethod.put(method, statistics);
  }

  @Override
  public Aggregator aggregator() {
    // Not supported for now
    return new Aggregator();
  }

  /**
   * @return the number of success operations from all statistics
   */
  @JsonIgnore
  public long getNumSuccessOperations() {
    long throughput = 0;
    for (MasterBenchTaskResultStatistics statistic: mStatistics) {
      throughput += statistic.mNumSuccess;
    }
    return throughput;
  }

  private static final class Aggregator
      implements TaskResult.Aggregator<MultiOperationMasterBenchTaskResult> {
    @Override
    public MultiOperationMasterBenchSummary aggregate(
        Iterable<MultiOperationMasterBenchTaskResult> results) throws Exception {
      Map<String, MultiOperationMasterBenchTaskResult> nodes = new HashMap<>();
      MultiOperationMasterBenchTaskResult mergingTaskResult = null;

      for (MultiOperationMasterBenchTaskResult taskResult : results) {
        nodes.put(taskResult.getBaseParameters().mId, taskResult);

        if (mergingTaskResult == null) {
          mergingTaskResult = taskResult;
          continue;
        }
        mergingTaskResult.aggregateByWorker(taskResult);
      }

      return new MultiOperationMasterBenchSummary(mergingTaskResult, nodes);
    }
  }
}
