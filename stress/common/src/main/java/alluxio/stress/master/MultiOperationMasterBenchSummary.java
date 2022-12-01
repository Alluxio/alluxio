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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.common.SummaryStatistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

/**
 * The summary for the multi-operation master stress tests.
 */
public final class MultiOperationMasterBenchSummary
    extends GeneralBenchSummary<MultiOperationMasterBenchTaskResult> {
  private long mDurationMs;
  long mEndTimeMs;
  private MultiOperationMasterBenchParameters mParameters;

  List<SummaryStatistics> mStatistics;

  private Map<String, SummaryStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public MultiOperationMasterBenchSummary() {
    // Default constructor required for json deserialization
  }

  /**
   * Creates an instance.
   *
   * @param mergedTaskResults the merged task result
   * @param nodes the map storing the nodes' result
   */
  @SuppressFBWarnings("BC_UNCONFIRMED_CAST")
  public MultiOperationMasterBenchSummary(
      MultiOperationMasterBenchTaskResult mergedTaskResults,
      Map<String, MultiOperationMasterBenchTaskResult> nodes) throws DataFormatException {
    mStatistics = mergedTaskResults.getAllStatistics()
        .stream().map(it -> {
          try {
            return it.toBenchSummaryStatistics();
          } catch (DataFormatException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());
    mStatisticsPerMethod = new HashMap<>();
    for (Map.Entry<String, MasterBenchTaskResultStatistics> entry :
        mergedTaskResults.getStatisticsPerMethod().entrySet()) {
      final String key = entry.getKey();
      final MasterBenchTaskResultStatistics value = entry.getValue();

      mStatisticsPerMethod.put(key, value.toBenchSummaryStatistics());
    }

    mDurationMs = mergedTaskResults.getEndMs() - mergedTaskResults.getRecordStartMs();
    mEndTimeMs = mergedTaskResults.getEndMs();
    mThroughput = ((float) mergedTaskResults.getNumSuccessOperations() / mDurationMs) * 1000.0f;
    mParameters = mergedTaskResults.getParameters();
    mNodeResults = nodes;
  }

  /**
   * @return the duration (in ms)
   */
  public long getDurationMs() {
    return mDurationMs;
  }

  /**
   * @param durationMs the duration (in ms)
   */
  public void setDurationMs(long durationMs) {
    mDurationMs = durationMs;
  }

  /**
   * @return the parameters
   */
  public MultiOperationMasterBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(MultiOperationMasterBenchParameters parameters) {
    mParameters = parameters;
  }

  @Override
  public alluxio.stress.GraphGenerator graphGenerator() {
    throw new RuntimeException("Graph generation is not supported in " + this.getClassName());
  }
}
