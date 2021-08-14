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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.wire.FileInfo;
import alluxio.wire.FileInfoTest;

import org.junit.Test;

import java.util.Map;

/**
 * Tests for the {@link URIStatus} class.
 */
public final class URIStatusTest {

  @Test
  public void constructor() {
    try {
      new URIStatus(null);
      fail("Cannot create a URIStatus from a null FileInfo");
    } catch (Exception e) {
      assertTrue(e instanceof NullPointerException);
    }
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    FileInfo fileInfo = FileInfoTest.createRandom();
    URIStatus uriStatus = new URIStatus(fileInfo);
    assertEquals(uriStatus.getBlockIds(), fileInfo.getBlockIds());
    assertEquals(uriStatus.getBlockSizeBytes(),
        fileInfo.getBlockSizeBytes());
    assertEquals(uriStatus.getCreationTimeMs(),
        fileInfo.getCreationTimeMs());
    assertEquals(uriStatus.getFileId(), fileInfo.getFileId());
    assertEquals(uriStatus.getGroup(), fileInfo.getGroup());
    assertEquals(uriStatus.getInMemoryPercentage(),
        fileInfo.getInMemoryPercentage());
    assertEquals(uriStatus.getLastModificationTimeMs(),
        fileInfo.getLastModificationTimeMs());
    assertEquals(uriStatus.getLength(), fileInfo.getLength());
    assertEquals(uriStatus.getName(), fileInfo.getName());
    assertEquals(uriStatus.getPath(), fileInfo.getPath());
    assertEquals(uriStatus.getMode(), fileInfo.getMode());
    assertEquals(uriStatus.getPersistenceState(),
        fileInfo.getPersistenceState());
    assertEquals(uriStatus.getTtl(), fileInfo.getTtl());
    assertEquals(uriStatus.getTtlAction(), fileInfo.getTtlAction());
    assertEquals(uriStatus.getUfsPath(), fileInfo.getUfsPath());
    assertEquals(uriStatus.getOwner(), fileInfo.getOwner());
    assertEquals(uriStatus.isCacheable(), fileInfo.isCacheable());
    assertEquals(uriStatus.isCompleted(), fileInfo.isCompleted());
    assertEquals(uriStatus.isFolder(), fileInfo.isFolder());
    assertEquals(uriStatus.isPersisted(), fileInfo.isPersisted());
    assertEquals(uriStatus.isPinned(), fileInfo.isPinned());
    assertEquals(uriStatus.isMountPoint(), fileInfo.isMountPoint());
    assertEquals(uriStatus.getFileBlockInfos(), fileInfo.getFileBlockInfos());
    assertEquals(uriStatus.getXAttr().size(), fileInfo.getXAttr().size());
    for (Map.Entry<String, byte[]> entry : uriStatus.getXAttr().entrySet()) {
      assertArrayEquals(entry.getValue(), fileInfo.getXAttr().get(entry.getKey()));
    }
  }

  @Test
  public void testEquals() throws Exception {
    FileInfo fileInfo = FileInfoTest.createRandom();
    URIStatus uriStatus1 = new URIStatus(fileInfo);
    URIStatus uriStatus2 = new URIStatus(fileInfo);
    assertTrue(uriStatus1.equals(uriStatus2));
    assertEquals(uriStatus1.hashCode(), uriStatus2.hashCode());
  }
}
