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
import alluxio.grpc.WritePType;
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
public class CrossClusterLatencyMain {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterLatencyMain.class);

  /**
   * Parameters for cross cluster latency benchmark.
   */
  public static class CrossClusterLatencyParams extends CrossClusterBaseParams {
    public static final String FILE_COUNT = "--file-count";
    public static final String RAND_READER = "--rand-reader";

    @Parameter(names = {RAND_READER},
        description = "Run a thread that randomly reads files on the read cluster,"
            + " indicates the number of reader threads on each cluster")
    public int mRandReader = 0;

    @Parameter(names = {FILE_COUNT},
        description = "The number of files to create to measure cross cluster latency")
    public int mFileCount = 100;
  }

  CrossClusterLatencyParams mParams = new CrossClusterLatencyParams();

  private CrossClusterLatencyMain(String[] args) {
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
    CrossClusterLatency test = new CrossClusterLatency(new AlluxioURI(mParams.mRootPath),
        clusterAddresses, mParams.mFileCount, mParams.mSyncLatency, mParams.mRandReader,
        WritePType.valueOf(mParams.mWriteType));
    test.doSetup();
    test.run();
    System.out.println("\nLatency of each cluster until visible on all other clusters");
    for (CrossClusterLatencyStatistics result : test.computeResults()) {
      System.out.println(result.toSummary().toJson());
    }
    System.out.println("\nResults of all reads during latency checks");
    System.out.println(test.computeAllReadResults().toSummary().toJson());
    System.out.println("\nResults of all random reads");
    System.out.println(test.computeAllRandResults().toSummary().toJson());
    test.doCleanup();
  }

  /**
   * @param args command-line arguments
   */
  public static void main(String[] args) throws Exception {
    new CrossClusterLatencyMain(args).run();
  }
}
