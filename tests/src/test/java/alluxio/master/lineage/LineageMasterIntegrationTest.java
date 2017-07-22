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
import alluxio.AuthenticatedUserRule;
import alluxio.Constants;
import alluxio.IntegrationTestUtils;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.client.lineage.LineageMasterClient;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.master.file.meta.PersistenceState;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
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
public class LineageMasterIntegrationTest extends BaseIntegrationTest {
  private static final int BLOCK_SIZE_BYTES = 128;
  private static final int BUFFER_BYTES = 100;
  private static final String OUT_FILE = "/test";
  private static final int RECOMPUTE_INTERVAL_MS = 1000;
  private static final int CHECKPOINT_INTERVAL_MS = 100;
  private static final GetStatusOptions GET_STATUS_OPTIONS = GetStatusOptions.defaults();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES))
          .setProperty(PropertyKey.USER_LINEAGE_ENABLED, "true")
          .setProperty(PropertyKey.MASTER_LINEAGE_RECOMPUTE_INTERVAL_MS,
              Integer.toString(RECOMPUTE_INTERVAL_MS))
          .setProperty(PropertyKey.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS,
              Integer.toString(CHECKPOINT_INTERVAL_MS))
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, "test")
          .build();

  private CommandLineJob mJob;

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule("test");

  @Before
  public void before() throws Exception {
    mJob = new CommandLineJob("test", new JobConf("output"));
  }

  @Test
  public void lineageCreation() throws Exception {
    try (LineageMasterClient lineageMasterClient = getLineageMasterClient()) {
      ArrayList<String> outFiles = new ArrayList<>();
      Collections.addAll(outFiles, OUT_FILE);
      lineageMasterClient.createLineage(new ArrayList<String>(), outFiles, mJob);

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      Assert.assertEquals(1, infos.size());
      AlluxioURI uri = new AlluxioURI(infos.get(0).getOutputFiles().get(0));
      URIStatus status = getFileSystemMasterClient().getStatus(uri, GET_STATUS_OPTIONS);
      Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), status.getPersistenceState());
      Assert.assertFalse(status.isCompleted());
    }
  }

  @Test
  public void lineageCompleteAndAsyncPersist() throws Exception {
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
      URIStatus status = getFileSystemMasterClient().getStatus(uri, GET_STATUS_OPTIONS);
      Assert.assertNotEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
      Assert.assertTrue(status.isCompleted());

      IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, uri);

      // worker notifies the master
      status = getFileSystemMasterClient().getStatus(uri, GET_STATUS_OPTIONS);
      Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    }
  }

  /**
   * Tests that a lineage job is executed when the output file for the lineage is reported as lost.
   *
   * The checkpoint interval is set high so that we are guaranteed to call reportLostFile
   * before persistence is complete.
   */
  @Test(timeout = 100000)
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.MASTER_LINEAGE_CHECKPOINT_INTERVAL_MS, "100000"})
  public void lineageRecovery() throws Exception {
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
    CommonUtils.waitFor("the log file to be written", new Function<Void, Boolean>() {
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
    }, WaitForOptions.defaults().setTimeoutMs(100 * Constants.SECOND_MS));
  }

  /**
   * Runs code given in the docs (http://alluxio.org/documentation/Lineage-API.html) to make sure it
   * actually runs.
   *
   * If you need to update the doc-code here, make sure you also update it in the docs.
   */
  @Test
  public void docExample() throws Exception {
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

  private LineageMasterClient getLineageMasterClient() {
    return new LineageMasterClient(
        mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getAddress());
  }

  private FileSystemMasterClient getFileSystemMasterClient() {
    return FileSystemMasterClient.Factory
        .create(mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getAddress());
  }
}
