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

package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.FileSystemShellUtilsTest;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;

import alluxio.master.block.BlockMaster;
import org.junit.Test;

/**
 * Tests for freeWorker Command.
 */
public final class FreeWorkerCommandIntegrationTest extends AbstractFileSystemShellTest {

  @Test
  public void removeWorkerMetadata() throws AlluxioException {
    sLocalAlluxioCluster.getLocalAlluxioMaster().getMasterProcess().getMaster(BlockMaster.class);
  }
}
