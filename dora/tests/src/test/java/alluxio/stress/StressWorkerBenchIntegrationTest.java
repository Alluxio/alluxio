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

package alluxio.stress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.PositionReadFileInStream;
import alluxio.client.file.dora.DoraCacheClient;
import alluxio.client.file.dora.netty.NettyDataReader;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.hadoop.HdfsFileInputStream;
import alluxio.stress.cli.client.ClientIOWritePolicy;
import alluxio.stress.cli.worker.StressWorkerBench;
import alluxio.stress.worker.WorkerBenchSummary;
import alluxio.util.JsonSerializable;

import alluxio.wire.WorkerNetAddress;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;


import java.io.FilterInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests {@link StressWorkerBench}.
 */
public class StressWorkerBenchIntegrationTest extends AbstractStressBenchIntegrationTest {
  @Test
  public void readThroughput() throws Exception {
    // Only in-process will work for unit testing.
    String output = new StressWorkerBench().run(new String[] {
        "--in-process",
        "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/stress-worker-base/",
        "--threads", "2",
        "--file-size", "1m",
        "--warmup", "0s", "--duration", "1s",
        "--cluster-limit", "1"
    });

    generateAndVerifyReport(Collections.singletonList("Worker Throughput"), output);
  }

  @Test
  @Ignore
  @DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "bowen",
      comment = "multiple node results is unsupported in DORA")
  public void testForMultipleNodeResults() throws Exception {
    long startTime = System.currentTimeMillis();
    String basePath = sLocalAlluxioClusterResource.get().getMasterURI() + "/stress-worker-base/";
    String output = new StressWorkerBench().run(new String[] {
        "--in-process",
        "--base", basePath,
        "--threads", "2",
        "--file-size", "1m",
        "--block-size", "128k",
        "--warmup", "0s", "--duration", "1s",
        "--cluster-limit", "2"
    });

    WorkerBenchSummary summary = (WorkerBenchSummary) JsonSerializable.fromJson(output);
    assertEquals(summary.getParameters().mBasePath, basePath);
    assertEquals(summary.getParameters().mFileSize, "1m");
    assertEquals(summary.getParameters().mThreads, 2);
    assertEquals(summary.getParameters().mWarmup, "0s");
    assertEquals(summary.getParameters().mDuration, "1s");

    assertTrue(summary.getEndTimeMs() > startTime);
    assertTrue(summary.getIOBytes() > 0);
    assertTrue(summary.getDurationMs() > 0);
    assertTrue(summary.getNodeResults().size() >= 1);
    assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
  }

  // TODO(jiacheng): this test needs rewriting
//  @Test
//  public void findLocalWorker() throws Exception {
//    // Only in-process will work for unit testing.
//    StressWorkerBench bench = new StressWorkerBench();
//    // Generate 64 * 8 files took 3.5 min
//    String[] args = new String[] {
//            "--in-process",
//            "--start-ms", Long.toString(System.currentTimeMillis() + 1000),
//            "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/stress-worker-base/",
//            "--threads", "64",
//            "--file-size", "1m",
//            "--warmup", "0s", "--duration", "1s",
//            "--cluster-limit", "8",
//            "--mode", "LOCAL"
//    };
//    bench.parseParameters(args);
//    bench.prepare();
//    Path[] filePaths = bench.getFilePaths();
//    // Here we verify each path will be routed to a local worker
//    FileSystemContext context = FileSystemContext.create(Configuration.global());
//    List<BlockWorkerInfo> workers = context.getCachedWorkers();
//    FileSystem fs = FileSystem.Factory.create(Configuration.global());
//    DoraCacheFileSystem doraFs = (DoraCacheFileSystem) fs;
//    DoraCacheClient client = doraFs.getClient();
//
//    // Create HDFS config for all FS instances
//    // Create those FS instances and cache, for the testing step
//    org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
//    hdfsConf.set(PropertyKey.Name.USER_BLOCK_WRITE_LOCATION_POLICY,
//            ClientIOWritePolicy.class.getName());
//    hdfsConf.set(PropertyKey.Name.USER_UFS_BLOCK_READ_LOCATION_POLICY,
//            ClientIOWritePolicy.class.getName());
//    org.apache.hadoop.fs.FileSystem hadoopFs = org.apache.hadoop.fs.FileSystem
//        .get(new URI(sLocalAlluxioClusterResource.get().getMasterURI() + "/stress-worker-base/"), hdfsConf);
//    System.out.println("Prepared HDFS FS for verification");
//
//    Pattern workerIdFinder = Pattern.compile("worker-(?<workerId>[0-9])-");
//
//
//    for (int i = 0; i < filePaths.length; i++) {
//      Path p = filePaths[i];
//      // This path contains a worker name
//      Matcher m = workerIdFinder.matcher(p.toString());
//      if (!m.find()) {
//        fail();
//      }
//      int workerId = Integer.parseInt(m.group("workerId"));
//      BlockWorkerInfo fileWantsWorker = workers.get(workerId);
//      System.out.format("Path %s specified workerId=%s to worker %s%n", p, workerId, workers.get(workerId));
//
//      try (FSDataInputStream inStream = hadoopFs.open(p)) {
//        System.out.println("Got stream");
//        Field privateField = FilterInputStream.class.getDeclaredField("in");
//        privateField.setAccessible(true);
//        HdfsFileInputStream in = (HdfsFileInputStream) privateField.get(inStream);
//
//        WorkerNetAddress targetWorkerAddr = ((NettyDataReader)((PositionReadFileInStream) in.mInputStream).mPositionReader).mAddress;
//        assertEquals(fileWantsWorker.getNetAddress(), targetWorkerAddr);
//      }
//    }
//  }
}
