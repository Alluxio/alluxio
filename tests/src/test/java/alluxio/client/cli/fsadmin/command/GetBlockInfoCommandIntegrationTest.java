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

package alluxio.client.cli.fsadmin.command;

import static org.hamcrest.CoreMatchers.containsString;

import alluxio.AlluxioURI;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AlluxioException;
import alluxio.grpc.WritePType;
import alluxio.master.block.BlockId;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for getBlockInfo command.
 */
public final class GetBlockInfoCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void invalidId() {
    String invalidId = "invalidId";
    int ret = mFsAdminShell.run("getBlockInfo", invalidId);
    Assert.assertEquals(-1, ret);
    Assert.assertThat(mOutput.toString(), containsString(invalidId + " is not a valid block id."));
  }

  @Test
  public void blockMetaNotFound() {
    long invalidId = 1421312312L;
    int ret = mFsAdminShell.run("getBlockInfo", String.valueOf(invalidId));
    // invalid block id should still continue to return useful information
    Assert.assertEquals(0, ret);
    Assert.assertThat(mOutput.toString(),
        containsString("BlockMeta is not available for blockId"));
    Assert.assertThat(mOutput.toString(),
        containsString("This block belongs to file"));
  }

  @Test
  public void getBlockInfo() throws IOException, AlluxioException {
    FileSystem fileSystem = mLocalAlluxioCluster.getClient();
    fileSystem.createDirectory(new AlluxioURI("/foo"));
    FileSystemTestUtils.createByteFile(fileSystem, "/foo/foobar1", WritePType.MUST_CACHE, 10);
    long blockId = fileSystem.listStatus(new AlluxioURI("/foo/foobar1"))
        .get(0).getBlockIds().get(0);
    int ret = mFsAdminShell.run("getBlockInfo", String.valueOf(blockId));
    Assert.assertEquals(0, ret);
    String output = mOutput.toString();

    Assert.assertThat(output, containsString(
        "BlockInfo{id=" + blockId + ", length=10, locations=[BlockLocation{workerId="));
    Assert.assertThat(output, containsString(
        "This block belongs to file {id=" + BlockId.getFileId(blockId) + ", path=/foo/foobar1}"));
  }
}
