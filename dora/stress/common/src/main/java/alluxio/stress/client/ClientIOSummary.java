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
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.graph.Graph;

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
  private Map<Integer, Float> mThreadCountIoMbps;

  /**
   * Default constructor required for json deserialization.
   */
  public ClientIOSummary() {
    this(null, null, new HashMap<>(), new HashMap<>());
  }

  /**
   * Creates an instance.
   *
   * @param parameters the parameters for the Fuse IO stress bench
   * @param baseParameters the base parameters for the Fuse IO stress bench
   * @param nodes the result of each client
   * @param threadCountIoMbps aggregated throughput data with different number of threads
   */
  public ClientIOSummary(ClientIOParameters parameters, BaseParameters baseParameters,
      Map<String, ClientIOTaskResult> nodes, Map<Integer, Float> threadCountIoMbps) {
    mNodeResults = nodes;
    mParameters = parameters;
    mBaseParameters = baseParameters;
    mThreadCountIoMbps = threadCountIoMbps;
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
   * @return the aggregated IOMbps with different thread numbers
   */
  public Map<Integer, Float> getThreadCountIoMbps() {
    return mThreadCountIoMbps;
  }

  /**
   * @param threadCountIoMbps the aggregated IOMbps with different thread numbers
   */
  public void setThreadCountIoMbps(Map<Integer, Float> threadCountIoMbps) {
    mThreadCountIoMbps = threadCountIoMbps;
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
