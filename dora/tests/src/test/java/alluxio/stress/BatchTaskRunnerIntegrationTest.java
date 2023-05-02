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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.stress.cli.BatchTaskRunner;
import alluxio.stress.common.FileSystemClientType;
import alluxio.stress.master.MasterBenchSummary;
import alluxio.stress.master.MasterBenchTaskResult;
import alluxio.util.JsonSerializable;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

public class BatchTaskRunnerIntegrationTest extends AbstractStressBenchIntegrationTest {
  private PrintStream mOriginalOut;
  private ByteArrayOutputStream mOut;

  // the possible operations
  private List<String> mOperation = ImmutableList.of("CreateFile", "ListDir", "ListDirLocated",
      "GetBlockLocations", "GetFileStatus", "OpenFile", "DeleteFile");

  @Before
  public void before() {
    // redirect the output stream
    mOut = new ByteArrayOutputStream();
    mOriginalOut = System.out;
    System.setOut(new PrintStream(mOut));
  }

  @After
  public void after() {
    // reset the output to the console
    System.setOut(mOriginalOut);
  }

  @Test
  public void MasterIntegrationFileTestAllParameters() throws Exception {
    BatchTaskRunner.main(new String[]{
        "MasterComprehensiveFileBatchTask",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--warmup", "0s",
        "--threads", "1",
        "--stop-count", "100",
        "--write-type", "MUST_CACHE",
        "--read-type", "NO_CACHE",
        "--create-file-size", "0",
        "--client-type", "AlluxioHDFS",
        "--clients", "1",
        "--cluster-limit", "1",
        "--cluster-start-delay", "3s",
        "--bench-timeout", "10m",
        "--java-opt", " -Xmx4g",
        "--in-process",
    });

    String printOutResult = mOut.toString();
    List<String> resultList = getJsonResult(printOutResult);
    assertEquals(resultList.size(), 7);

    for (int i = 0; i < resultList.size(); i++) {
      MasterBenchSummary summary = (MasterBenchSummary) JsonSerializable.fromJson(
          resultList.get(i));
      // confirm that the task was executed with certain parameter and output no errors
      assertEquals(summary.getParameters().mOperation.toString(), mOperation.get(i));
      assertEquals(summary.getParameters().mWarmup, "0s");
      assertEquals(summary.getParameters().mThreads, 1);
      assertEquals(summary.getParameters().mStopCount, 100);
      assertEquals(summary.getParameters().mWriteType, "MUST_CACHE");
      assertEquals(summary.getParameters().mReadType.toString(), "NO_CACHE");
      assertEquals(summary.getParameters().mCreateFileSize, "0");
      assertEquals(summary.getParameters().mClientType, FileSystemClientType.ALLUXIO_HDFS);
      assertEquals(summary.getParameters().mClients, 1);
      Map<String, MasterBenchTaskResult> results = summary.getNodeResults();
      assertFalse(results.isEmpty());
      for (MasterBenchTaskResult res : results.values()) {
        assertEquals(res.getBaseParameters().mClusterLimit, 1);
        assertEquals(res.getBaseParameters().mClusterStartDelay, "3s");
        assertEquals(res.getBaseParameters().mBenchTimeout, "10m");
        assertEquals(res.getBaseParameters().mJavaOpts.get(0), " -Xmx4g");
      }
      assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
    }
  }

  @Test
  public void MasterIntegrationFileTestWriteType() throws Exception {
    List<String> writeTypes = ImmutableList.of("MUST_CACHE", "CACHE_THROUGH",
        "ASYNC_THROUGH", "THROUGH");

    for (String type : writeTypes) {
      BatchTaskRunner.main(new String[]{
          "MasterComprehensiveFileBatchTask",
          "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
          "--warmup", "0s",
          "--threads", "1",
          "--stop-count", "100",
          "--write-type", type,
          "--read-type", "NO_CACHE",
          "--client-type", "AlluxioHDFS",
          "--create-file-size", "0",
          "--in-process",
      });

      String printOutResult = mOut.toString();
      List<String> resultList = getJsonResult(printOutResult);
      assertEquals(resultList.size(), 7);
      mOut.reset();

      for (int i = 0; i < resultList.size(); i++) {
        MasterBenchSummary summary = (MasterBenchSummary) JsonSerializable.fromJson(
            resultList.get(i));
        // confirm that the task was executed with certain parameter and output no errors
        assertEquals(summary.getParameters().mOperation.toString(), mOperation.get(i));
        assertEquals(summary.getParameters().mWarmup, "0s");
        assertEquals(summary.getParameters().mThreads, 1);
        assertEquals(summary.getParameters().mStopCount, 100);
        assertEquals(summary.getParameters().mWriteType, type);
        assertEquals(summary.getParameters().mReadType.toString(), "NO_CACHE");
        assertEquals(summary.getParameters().mCreateFileSize, "0");
        assertEquals(summary.getParameters().mClientType, FileSystemClientType.ALLUXIO_HDFS);
        Map<String, MasterBenchTaskResult> results = summary.getNodeResults();

        assertFalse(results.isEmpty());
        assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
      }
    }
  }

  @Test
  public void MasterIntegrationFileTestWriteTypeAll() throws Exception {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    PrintStream originalErr = System.err;
    System.setErr(new PrintStream(err));

    BatchTaskRunner.main(new String[]{
        "MasterComprehensiveFileBatchTask",
        "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
        "--warmup", "0s",
        "--threads", "1",
        "--stop-count", "100",
        "--write-type", "ALL",
        "--read-type", "NO_CACHE",
        "--client-type", "AlluxioHDFS",
        "--create-file-size", "0",
        "--in-process",
    });

    assertEquals(err.toString(), "Parameter write-type ALL is not supported in"
        + " batch task MasterComprehensiveFileBatchTask");

    System.setErr(originalErr);
  }

  @Test
  public void MasterIntegrationFileTestReadType() throws Exception {
    List<String> readTypes = ImmutableList.of("NO_CACHE", "CACHE", "CACHE_PROMOTE");

    for (String type : readTypes) {
      BatchTaskRunner.main(new String[]{
          "MasterComprehensiveFileBatchTask",
          "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
          "--warmup", "0s",
          "--threads", "1",
          "--stop-count", "100",
          "--write-type", "MUST_CACHE",
          "--read-type", type,
          "--client-type", "AlluxioHDFS",
          "--create-file-size", "0",
          "--in-process",
      });

      String printOutResult = mOut.toString();
      List<String> resultList = getJsonResult(printOutResult);
      assertEquals(resultList.size(), 7);
      mOut.reset();

      for (int i = 0; i < resultList.size(); i++) {
        MasterBenchSummary summary = (MasterBenchSummary) JsonSerializable.fromJson(
            resultList.get(i));
        // confirm that the task was executed with certain parameter and output no errors
        assertEquals(summary.getParameters().mOperation.toString(), mOperation.get(i));
        assertEquals(summary.getParameters().mWarmup, "0s");
        assertEquals(summary.getParameters().mThreads, 1);
        assertEquals(summary.getParameters().mStopCount, 100);
        assertEquals(summary.getParameters().mWriteType, "MUST_CACHE");
        assertEquals(summary.getParameters().mReadType.toString(), type);
        assertEquals(summary.getParameters().mClientType, FileSystemClientType.ALLUXIO_HDFS);
        assertEquals(summary.getParameters().mCreateFileSize, "0");
        Map<String, MasterBenchTaskResult> results = summary.getNodeResults();

        assertFalse(results.isEmpty());
        assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
      }
    }
  }

  @Test
  public void MasterIntegrationFileTestFileSize() throws Exception {
    List<String> fileSizes = ImmutableList.of("0", "1k", "5k", "1m");

    for (String size : fileSizes) {
      BatchTaskRunner.main(new String[]{
          "MasterComprehensiveFileBatchTask",
          "--base", sLocalAlluxioClusterResource.get().getMasterURI() + "/",
          "--warmup", "0s",
          "--threads", "1",
          "--stop-count", "10",
          "--write-type", "MUST_CACHE",
          "--read-type", "NO_CACHE",
          "--client-type", "AlluxioHDFS",
          "--create-file-size", size,
          "--in-process",
      });

      String printOutResult = mOut.toString();
      List<String> resultList = getJsonResult(printOutResult);
      assertEquals(resultList.size(), 7);
      mOut.reset();

      for (int i = 0; i < resultList.size(); i++) {
        MasterBenchSummary summary = (MasterBenchSummary) JsonSerializable.fromJson(
            resultList.get(i));
        // confirm that the task was executed with certain parameter and output no errors
        assertEquals(summary.getParameters().mOperation.toString(), mOperation.get(i));
        assertEquals(summary.getParameters().mWarmup, "0s");
        assertEquals(summary.getParameters().mThreads, 1);
        assertEquals(summary.getParameters().mStopCount, 10);
        assertEquals(summary.getParameters().mWriteType, "MUST_CACHE");
        assertEquals(summary.getParameters().mReadType.toString(), "NO_CACHE");
        assertEquals(summary.getParameters().mClientType, FileSystemClientType.ALLUXIO_HDFS);
        assertEquals(summary.getParameters().mCreateFileSize, size);
        Map<String, MasterBenchTaskResult> results = summary.getNodeResults();

        assertFalse(results.isEmpty());
        assertTrue(summary.collectErrorsFromAllNodes().isEmpty());
      }
    }
  }
}
