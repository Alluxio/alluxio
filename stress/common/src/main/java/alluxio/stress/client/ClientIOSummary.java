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

import alluxio.collections.Pair;
import alluxio.stress.BaseParameters;
import alluxio.stress.Parameters;
import alluxio.stress.Summary;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.graph.Graph;

import com.google.common.base.Splitter;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The summary for Fuse IO stress bench.
 */
public class ClientIOSummary extends GeneralBenchSummary<ClientIOTaskResult> {
  private ClientIOParameters mParameters;
  private BaseParameters mBaseParameters;
  private long mRecordStartMs;
  private long mEndMs;
  private long mIOBytes;
  private float mIOMBps;

  /**
   * Default constructor required for json deserialization.
   */
  public ClientIOSummary() {
    this(null, null, new HashMap<>(), 0, 0, 0, 0);
  }

  /**
   * Creates an instance.
   *
   * @param parameters the parameters for the Fuse IO stress bench
   * @param baseParameters the base parameters for the Fuse IO stress bench
   * @param nodes the unique ids of all job workers
   * @param recordStartMs the timestamp starting counting bytes
   * @param endMs the timestamp that the test ends
   * @param ioBytes total number of bytes processed by workers
   * @param ioMBps aggregated throughput data
   */
  public ClientIOSummary(ClientIOParameters parameters, BaseParameters baseParameters,
      Map<String, ClientIOTaskResult> nodes, long recordStartMs, long endMs,
      long ioBytes, float ioMBps) {
    mNodeResults = nodes;
    mParameters = parameters;
    mBaseParameters = baseParameters;
    mRecordStartMs = recordStartMs;
    mEndMs = endMs;
    mIOBytes = ioBytes;
    mThroughput = ioMBps;
    mIOMBps = mThroughput;
  }

  @Override
  public GraphGenerator graphGenerator() {
    return new GraphGenerator();
  }

  /**
   * @return Fuse IO stress bench parameters
   */
  public ClientIOParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters Fuse IO stress bench parameters
   */
  public void setParameters(ClientIOParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the base parameters
   */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the base parameters
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the timestamp starting counting bytes (in ms)
   */
  public long getRecordStartMs() {
    return mRecordStartMs;
  }

  /**
   * @param recordStartMs the timestamp starting counting bytes (in ms)
   */
  public void setRecordStartMs(long recordStartMs) {
    mRecordStartMs = recordStartMs;
  }

  /**
   * @return the timestamp that test ends (in ms)
   */
  public long getEndMs() {
    return mEndMs;
  }

  /**
   * @param endMs the timestamp that test ends (in ms)
   */
  public void setEndMs(long endMs) {
    mEndMs = endMs;
  }

  /**
   * @return total number of bytes processed during test time
   */
  public long getIOBytes() {
    return mIOBytes;
  }

  /**
   * @param ioBytes total number of bytes processed during test time
   */
  public void setIOBytes(long ioBytes) {
    mIOBytes = ioBytes;
  }

  /**
   * @return overall throughput (in MB / s)
   */
  public float getIOMBps() {
    return mIOMBps;
  }

  /**
   * @param ioMBps overall throughput (in MB / s)
   */
  public void setIOMBps(float ioMBps) {
    mIOMBps = ioMBps;
  }

  /**
   * The graph generator for this summary.
   */
  public static final class GraphGenerator extends alluxio.stress.GraphGenerator {
    @Override
    public List<Graph> generate(List<? extends Summary> results) {
      List<Graph> graphs = new ArrayList<>();
      // only examine ClientIOSummary
      List<ClientIOSummary> summaries =
          results.stream().map(x -> (ClientIOSummary) x).collect(Collectors.toList());

      // Iterate over all operations
      for (ClientIOOperation operation : ClientIOOperation.values()) {
        for (Boolean readRandom : Arrays.asList(false, true)) {
          List<ClientIOSummary> opSummaries =
              summaries.stream().filter(x -> x.mParameters.mOperation == operation)
                  .filter(x -> x.mParameters.mReadRandom == readRandom)
                  .collect(Collectors.toList());
          for (ClientIOSummary summary: opSummaries) {
            List<ClientIOTaskResult> clientResults =
                new ArrayList<>(summary.getNodeResults().values());
            graphs.addAll(clientResults.get(0).graphGenerator().generate(clientResults));
          }
        }
      }

      return graphs;
    }
  }
}
