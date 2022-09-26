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

import static alluxio.stress.BaseParameters.HELP_FLAG;

import alluxio.AlluxioURI;
import alluxio.util.ConfigurationUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test the latency between clusters.
 */
public class CrossClusterLatencyMain {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterLatencyMain.class);

  public static final String FILE_COUNT = "--file-count";
  public static final String IP_LIST = "--ip-list";
  public static final String ROOT_PATH = "--path";
  public static final String SYNC_LATENCY = "--latency";
  public static final String RAND_READER = "--rand-reader";

  @Parameter(names = {RAND_READER},
      description = "Run a thread that randomly reads files on the read cluster,"
          + " indicates the number of reader threads on each cluster")
  public int mRandReader = 0;

  @Parameter(names = {SYNC_LATENCY},
      description = "Metadata sync latency (not needed for cross cluster mounts)")
  public int mSyncLatency = 0;

  @Parameter(names = {ROOT_PATH},
      description = "In alluxio path where to create and read files",
      required = true)
  public String mRootPath;

  @Parameter(names = {FILE_COUNT},
      description = "The number of files to create to measure cross cluster latency")
  public int mFileCount = 100;

  @Parameter(names = {IP_LIST}, splitter = NoSplitter.class,
      description = "Each entry should be a comma seperated list of ip:port of the masters of each"
          + " cluster, this should be repeated for each cluster eg: --ip-list \"127.0.0.1:19998\""
          + " --ip-list \"127.0.0.1:19997,127.0.0.1:19996\"",
      required = true)
  public List<String> mClusterIps = new ArrayList<>();

  @Parameter(names = {"-h", HELP_FLAG}, help = true)
  public boolean mHelp = false;

  static class NoSplitter implements IParameterSplitter {
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }

  private CrossClusterLatencyMain(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName(this.getClass().getSimpleName());
    try {
      jc.parse(args);
      if (mHelp) {
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
    List<List<InetSocketAddress>> clusterAddresses = mClusterIps.stream().map(
            nxt -> ConfigurationUtils.parseAsList(nxt, ","))
        .map(ConfigurationUtils::parseInetSocketAddresses).collect(Collectors.toList());
    CrossClusterLatency test = new CrossClusterLatency(new AlluxioURI(mRootPath),
        clusterAddresses, mFileCount, mSyncLatency, mRandReader);
    test.doSetup();
    test.run();
    for (CrossClusterLatencyStatistics result : test.computeResults()) {
      System.out.println(result.toSummary().toJson());
    }
    System.out.println("Results of all reads during latency checks");
    System.out.println(test.computeAllReadResults().toSummary().toJson());
    System.out.println("Results of all random reads");
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
