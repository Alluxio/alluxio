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

package alluxio.client.cli.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.cli.fs.FileSystemShell;
import alluxio.client.file.FileSystem;
import alluxio.conf.ServerConfiguration;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Tests that the job service is available to the shell when running in fault-tolerant mode.
 */
public final class JobServiceFaultToleranceShellTest extends BaseIntegrationTest {
  private MultiMasterLocalAlluxioCluster mLocalAlluxioCluster;
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  private ByteArrayOutputStream mOutput;

  @Rule
  public TestName mTestName = new TestName();

  @Before
  public void before() throws Exception {
    mLocalAlluxioCluster = new MultiMasterLocalAlluxioCluster(2);
    mLocalAlluxioCluster.initConfiguration(
        IntegrationTestUtils.getTestName(getClass().getSimpleName(), mTestName.getMethodName()));
    mLocalAlluxioCluster.start();
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mOutput = new ByteArrayOutputStream();
    System.setOut(new PrintStream(mOutput));
  }

  @After
  public void after() throws Exception {
    if (mLocalAlluxioJobCluster != null) {
      mLocalAlluxioJobCluster.stop();
    }
    if (mLocalAlluxioCluster != null) {
      mLocalAlluxioCluster.stop();
    }
    System.setOut(System.out);
    ServerConfiguration.reset();
  }

  @Test
  public void distributedCp() throws Exception {
    FileSystem fs = FileSystem.Factory.create(ServerConfiguration.global());
    try (OutputStream out = fs.createFile(new AlluxioURI("/test"))) {
      out.write("Hello".getBytes());
    }

    try (FileSystemShell shell = new FileSystemShell(ServerConfiguration.global())) {
      int exitCode = shell.run("distributedCp", "/test", "/test2");
      assertEquals("Command failed, output: " + mOutput.toString(), 0, exitCode);
    }
    assertTrue(fs.exists(new AlluxioURI("/test2")));
  }
}
