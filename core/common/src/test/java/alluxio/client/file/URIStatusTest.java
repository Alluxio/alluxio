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

package alluxio.client.file;

import alluxio.wire.FileInfo;
import alluxio.wire.FileInfoTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link URIStatus} class.
 */
public final class URIStatusTest {

  @Test
  public void constructor() {
    try {
      new URIStatus(null);
      Assert.fail("Cannot create a URIStatus from a null FileInfo");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NullPointerException);
      Assert.assertTrue(e.getMessage().contains(
          "Cannot create a URIStatus from a null FileInfo"));
    }
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    FileInfo fileInfo = FileInfoTest.createRandom();
    URIStatus uriStatus = new URIStatus(fileInfo);
    Assert.assertEquals(uriStatus.getBlockIds(), fileInfo.getBlockIds());
    Assert.assertEquals(uriStatus.getBlockSizeBytes(),
        fileInfo.getBlockSizeBytes());
    Assert.assertEquals(uriStatus.getCreationTimeMs(),
        fileInfo.getCreationTimeMs());
    Assert.assertEquals(uriStatus.getFileId(), fileInfo.getFileId());
    Assert.assertEquals(uriStatus.getGroup(), fileInfo.getGroup());
    Assert.assertEquals(uriStatus.getInMemoryPercentage(),
        fileInfo.getInMemoryPercentage());
    Assert.assertEquals(uriStatus.getLastModificationTimeMs(),
        fileInfo.getLastModificationTimeMs());
    Assert.assertEquals(uriStatus.getLength(), fileInfo.getLength());
    Assert.assertEquals(uriStatus.getName(), fileInfo.getName());
    Assert.assertEquals(uriStatus.getPath(), fileInfo.getPath());
    Assert.assertEquals(uriStatus.getMode(), fileInfo.getMode());
    Assert.assertEquals(uriStatus.getPersistenceState(),
        fileInfo.getPersistenceState());
    Assert.assertEquals(uriStatus.getTtl(), fileInfo.getTtl());
    Assert.assertEquals(uriStatus.getTtlAction(), fileInfo.getTtlAction());
    Assert.assertEquals(uriStatus.getUfsPath(), fileInfo.getUfsPath());
    Assert.assertEquals(uriStatus.getOwner(), fileInfo.getOwner());
    Assert.assertEquals(uriStatus.isCacheable(), fileInfo.isCacheable());
    Assert.assertEquals(uriStatus.isCompleted(), fileInfo.isCompleted());
    Assert.assertEquals(uriStatus.isFolder(), fileInfo.isFolder());
    Assert.assertEquals(uriStatus.isPersisted(), fileInfo.isPersisted());
    Assert.assertEquals(uriStatus.isPinned(), fileInfo.isPinned());
    Assert.assertEquals(uriStatus.isMountPoint(), fileInfo.isMountPoint());
    Assert.assertEquals(uriStatus.getFileBlockInfos(),
        fileInfo.getFileBlockInfos());
    Assert.assertEquals(uriStatus.toString(), fileInfo.toString());
  }

  @Test
  public void testEquals() throws Exception {
    FileInfo fileInfo = FileInfoTest.createRandom();
    URIStatus uriStatus1 = new URIStatus(fileInfo);
    URIStatus uriStatus2 = new URIStatus(fileInfo);
    Assert.assertTrue(uriStatus1.equals(uriStatus2));
    Assert.assertEquals(uriStatus1.hashCode(), uriStatus2.hashCode());
  }
}
