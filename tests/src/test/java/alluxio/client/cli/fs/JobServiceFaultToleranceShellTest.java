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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Tests that the job service is available to the shell when running in fault-tolerant mode.
 */
public final class JobServiceFaultToleranceShellTest extends BaseIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mClusterResource = LocalAlluxioClusterResource.newBuilder()
      .setProperty(PropertyKey.ZOOKEEPER_ENABLED, true)
      .build();

  private LocalAlluxioCluster mCluster = mClusterResource.get();
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  private ByteArrayOutputStream mOutput;

  @Before
  public void before() throws Exception {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mOutput = new ByteArrayOutputStream();
    System.setOut(new PrintStream(mOutput));
  }

  @After
  public void after() throws Exception {
    mLocalAlluxioJobCluster.stop();
    mCluster.stop();
    System.setOut(System.out);
    ServerConfiguration.reset();
  }

  @Test
  public void distributedMv() throws Exception {
    FileSystem fs = FileSystem.Factory.create(ServerConfiguration.global());
    try (OutputStream out = fs.createFile(new AlluxioURI("/test"))) {
      out.write("Hello".getBytes());
    }

    try (FileSystemShell shell = new FileSystemShell(ServerConfiguration.global())) {
      int exitCode = shell.run("distributedMv", "/test", "/test2");
      assertEquals("Command failed, output: " + mOutput.toString(), 0, exitCode);
    }
    assertTrue(fs.exists(new AlluxioURI("/test2")));
  }
}
