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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

/**
 * The task result for the master stress tests.
 */
public final class MultiOperationsMasterBenchTaskResult extends MasterBenchTaskResultBase {
  private List<MasterBenchTaskResultStatistics> mAllStatistics;

  /**
   * Creates an instance.
   */

  public MultiOperationsMasterBenchTaskResult(int opCpunt) {
    // Default constructor required for json deserialization
    mErrors = new ArrayList<>();
    mAllStatistics = new ArrayList<>(opCpunt);
    mStatisticsPerMethod =  new HashMap<>();
    for (int i = 0; i < opCpunt; i++) {
      mAllStatistics.add(new MasterBenchTaskResultStatistics());
    }
  }

  @Override
  List<SummaryStatistics> getSummaryStatistics() throws DataFormatException {
    List<SummaryStatistics> result = new ArrayList<>();
    for (MasterBenchTaskResultStatistics statistics: mAllStatistics ) {
      result.add(statistics.toBenchSummaryStatistics());
    }
    return result;
  }

  @Override
  void mergeResultStatistics(MasterBenchTaskResultBase result) throws Exception {
    Preconditions.checkState(result instanceof MultiOperationsMasterBenchTaskResult);
    MultiOperationsMasterBenchTaskResult convertedResult =
        (MultiOperationsMasterBenchTaskResult) result;

    for (int i = 0; i < mAllStatistics.size(); i++) {
      mAllStatistics.get(i).merge(convertedResult.mAllStatistics.get(i));
    }
  }

  @Override
  long getTotalSuccess() {
    long result = 0;
    for (MasterBenchTaskResultStatistics stat: mAllStatistics) {
      result += stat.mNumSuccess;
    }
    return result;
  }

  public void incrementNumSuccess(long numSuccess, int opIdx) {
    mAllStatistics.get(opIdx).mNumSuccess += numSuccess;
  }

  public long[] getMaxResponseTimeNs(int opIdx) {
    return mAllStatistics.get(opIdx).mMaxResponseTimeNs;
  }

  public void setMaxResponseTimeNs(long[] maxResponseTimeNs, int opIdx) {
    mAllStatistics.get(opIdx).mMaxResponseTimeNs = maxResponseTimeNs;
  }

  public List<MasterBenchTaskResultStatistics> getAllStatistics() {
    return mAllStatistics;
  }

  public void setAllStatistics(List<MasterBenchTaskResultStatistics> statistics) {
    mAllStatistics = statistics;
  }

  public MasterBenchTaskResultStatistics getStatistics(int opIdx) {
    return mAllStatistics.get(opIdx);
  }

  /**
   * @param statistics the statistics
   */
  public void setStatistics(MasterBenchTaskResultStatistics statistics, int opIdx) {
    mAllStatistics.set(opIdx, statistics);
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<MultiOperationsMasterBenchTaskResult> {
    @Override
    public MultiOperationsMasterBenchSummary aggregate(Iterable<MultiOperationsMasterBenchTaskResult> results) throws Exception {
      Map<String, MultiOperationsMasterBenchTaskResult> nodes = new HashMap<>();
      MultiOperationsMasterBenchTaskResult mergingTaskResult = null;

      for (MultiOperationsMasterBenchTaskResult taskResult : results) {
        nodes.put(taskResult.getBaseParameters().mId, taskResult);

        if (mergingTaskResult == null) {
          mergingTaskResult = taskResult;
          continue;
        }
        mergingTaskResult.aggregateByWorker(taskResult);
      }

      return new MultiOperationsMasterBenchSummary(mergingTaskResult, nodes);
    }
  }
}
