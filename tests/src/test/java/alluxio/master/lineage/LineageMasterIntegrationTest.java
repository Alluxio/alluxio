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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.CommonTestUtils;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.IntegrationTestConstants;
import alluxio.IntegrationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.client.lineage.LineageMasterClient;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.master.file.meta.PersistenceState;
import alluxio.wire.LineageInfo;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for the lineage module.
 */
public class LineageMasterIntegrationTest {
  protected static final int BLOCK_SIZE_BYTES = 128;
  protected static final long WORKER_CAPACITY_BYTES = Constants.GB;
  protected static final int BUFFER_BYTES = 100;
  protected static final String OUT_FILE = "/test";

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource = new LocalAlluxioClusterResource(
      WORKER_CAPACITY_BYTES, BLOCK_SIZE_BYTES,
      Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES),
      Constants.WORKER_DATA_SERVER, IntegrationTestConstants.NETTY_DATA_SERVER,
      Constants.USER_LINEAGE_ENABLED, "true",
      Constants.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS, "1000",
      Constants.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS, "100"
      );

  protected Configuration mTestConf;
  protected CommandLineJob mJob;

  @Before
  public void before() throws Exception {
    mJob = new CommandLineJob("test", new JobConf("output"));
    mTestConf = mLocalAlluxioClusterResource.get().getMasterConf();
  }

  @Test
  public void lineageCreationTest() throws Exception {

    try (LineageMasterClient lineageMasterClient = getLineageMasterClient()) {
      ArrayList<String> outFiles = new ArrayList<>();
      Collections.addAll(outFiles, OUT_FILE);
      lineageMasterClient.createLineage(new ArrayList<String>(), outFiles, mJob);

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      Assert.assertEquals(1, infos.size());
      AlluxioURI uri = new AlluxioURI(infos.get(0).getOutputFiles().get(0));
      URIStatus status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
      Assert.assertFalse(status.isCompleted());
    }
  }

  @Test
  public void lineageCompleteAndAsyncPersistTest() throws Exception {

    try (LineageMasterClient lineageMasterClient = getLineageMasterClient()) {
      ArrayList<String> outFiles = new ArrayList<>();
      Collections.addAll(outFiles, OUT_FILE);
      lineageMasterClient.createLineage(new ArrayList<String>(), outFiles, mJob);

      CreateFileOptions options = CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
          .setBlockSizeBytes(BLOCK_SIZE_BYTES);
      LineageFileSystem fs = (LineageFileSystem) mLocalAlluxioClusterResource.get().getClient();
      FileOutStream outputStream = fs.createFile(new AlluxioURI(OUT_FILE), options);
      outputStream.write(1);
      outputStream.close();

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      AlluxioURI uri = new AlluxioURI(infos.get(0).getOutputFiles().get(0));
      URIStatus status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertNotEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
      Assert.assertTrue(status.isCompleted());

      IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, uri);

      // worker notifies the master
      status = getFileSystemMasterClient().getStatus(uri);
      Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    }
  }

  /**
   * Tests that a lineage job is executed when the output file for the lineage is reported as lost.
   */
  @Test(timeout = 20000)
  public void lineageRecoveryTest() throws Exception {
    final File logFile = mFolder.newFile();
    // Delete the log file so that when it starts to exist we know that it was created by the
    // lineage recompute job
    logFile.delete();
    FileSystem fs = FileSystem.Factory.get();
    try (LineageMasterClient lineageClient = getLineageMasterClient()) {
      lineageClient.createLineage(ImmutableList.<String>of(), ImmutableList.of("/testFile"),
          new CommandLineJob("echo hello world", new JobConf(logFile.getAbsolutePath())));
      FileOutStream out = fs.createFile(new AlluxioURI("/testFile"));
      out.write("foo".getBytes());
      out.close();
      lineageClient.reportLostFile("/testFile");
    }

    // Wait for the log file to be created by the recompute job
    CommonTestUtils.waitFor("the log file to be written", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        if (!logFile.exists()) {
          return false;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
          String line = reader.readLine();
          return line != null && line.equals("hello world");
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, 10 * Constants.SECOND_MS);
  }

  /**
   * Runs code given in the docs (http://alluxio.org/documentation/Lineage-API.html) to make sure it
   * actually runs.
   *
   * If you need to update the doc-code here, make sure you also update it in the docs.
   */
  @Test
  public void docExampleTest() throws Exception {
    // create input files
    FileSystem fs = FileSystem.Factory.get();
    fs.createFile(new AlluxioURI("/inputFile1")).close();
    fs.createFile(new AlluxioURI("/inputFile2")).close();

    // ------ code block from docs ------
    AlluxioLineage tl = AlluxioLineage.get();
    // input file paths
    AlluxioURI input1 = new AlluxioURI("/inputFile1");
    AlluxioURI input2 = new AlluxioURI("/inputFile2");
    ArrayList<AlluxioURI> inputFiles = new ArrayList<>();
    Collections.addAll(inputFiles, input1, input2);
    // output file paths
    AlluxioURI output = new AlluxioURI("/outputFile");
    ArrayList<AlluxioURI> outputFiles = new ArrayList<>();
    Collections.addAll(outputFiles, output);
    // command-line job
    JobConf conf = new JobConf("/tmp/recompute.log");
    CommandLineJob job = new CommandLineJob("my-spark-job.sh", conf);
    long lineageId = tl.createLineage(inputFiles, outputFiles, job);

    // ------ code block from docs ------
    DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(true);
    tl.deleteLineage(lineageId);

    fs.delete(new AlluxioURI("/outputFile"));
    lineageId = tl.createLineage(inputFiles, outputFiles, job);

    // ------ code block from docs ------
    tl.deleteLineage(lineageId, options);
  }

  protected LineageMasterClient getLineageMasterClient() {
    return new LineageMasterClient(mLocalAlluxioClusterResource.get().getMaster().getAddress(),
        mTestConf);
  }

  protected FileSystemMasterClient getFileSystemMasterClient() {
    return new FileSystemMasterClient(mLocalAlluxioClusterResource.get().getMaster().getAddress(),
        mTestConf);
  }
}
