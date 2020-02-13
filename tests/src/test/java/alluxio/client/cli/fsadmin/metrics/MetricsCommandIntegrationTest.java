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

package alluxio.client.cli.fsadmin.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import alluxio.client.file.FileSystemTestUtils;
import alluxio.grpc.WritePType;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;

/**
 * Integration tests for the metrics command.
 */
public final class MetricsCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void clearLeadingMasterMetrics() throws IOException {
    FileSystemTestUtils.createByteFile(mLocalAlluxioCluster.getClient(),
        "/file", WritePType.MUST_CACHE, 10);

    int errCode = mFsAdminShell.run("report", "metrics");
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);
    assertThat(mOutput.toString(), CoreMatchers.containsString(
        "CreateFileOps  (Type: COUNTER, Value: 1)"));

    errCode = mFsAdminShell.run("metrics", "clear", "--master");
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);

    mOutput.reset();
    errCode = mFsAdminShell.run("report", "metrics");
    assertEquals("", mErrOutput.toString());
    assertEquals(0, errCode);
    assertThat(mOutput.toString(), CoreMatchers.containsString(
        "CreateFileOps  (Type: COUNTER, Value: 0)"));
  }
}
