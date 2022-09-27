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

package alluxio.cross.cluster.cli;

import alluxio.AlluxioURI;
import alluxio.util.ConfigurationUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test the latency between clusters.
 */
public class CrossClusterWriteMain {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterWriteMain.class);

  /**
   * Parameters for cross cluster latency benchmark.
   */
  public static class CrossClusterWriteParams extends CrossClusterBaseParams {
    public static final String RATE_LIMIT = "--rate-limit";
    public static final String WRITE_THREADS = "--write-threads";
    public static final String DURATION = "--duration";

    @Parameter(names = {DURATION},
        description = "Benchmark duration in ms")
    public long mDuration = 10_000;

    @Parameter(names = {RATE_LIMIT},
        description = "If non-zero will limit the number of writes per second"
            + "to the given value")
    public long mRateLimit = 0;

    @Parameter(names = {WRITE_THREADS},
        description = "The number of threads writing on each cluster")
    public int mWriterThreads = 1;
  }

  CrossClusterWriteParams mParams = new CrossClusterWriteParams();

  private CrossClusterWriteMain(String[] args) {
    JCommander jc = new JCommander(mParams);
    jc.setProgramName(this.getClass().getSimpleName());

    try {
      jc.parse(args);
      if (mParams.mHelp) {
        jc.usage();
        System.exit(0);
      }
    } catch (Exception e) {
      LOG.error("Failed to parse command: ", e);
      jc.usage();
      throw e;
    }
  }

  void run() throws Exception {
    List<List<InetSocketAddress>> clusterAddresses = mParams.mClusterIps.stream().map(
            nxt -> ConfigurationUtils.parseAsList(nxt, ","))
        .map(ConfigurationUtils::parseInetSocketAddresses).collect(Collectors.toList());
    CrossClusterWrite test = new CrossClusterWrite(new AlluxioURI(mParams.mRootPath),
        clusterAddresses, mParams.mWriterThreads, mParams.mDuration, mParams.mSyncLatency,
        mParams.mRateLimit == 0 ? null : mParams.mRateLimit);
    test.doSetup();
    test.run();
    for (CrossClusterLatencyStatistics result : test.computeResults()) {
      System.out.println(result.toSummary().toJson());
    }
    test.doCleanup();
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) throws Exception {
    new CrossClusterWriteMain(args).run();
  }
}
