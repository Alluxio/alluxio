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

package alluxio.underfs;

import static org.junit.Assert.assertEquals;

import alluxio.util.CommonUtils;

import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link UfsFileStatus} class.
 */
public final class UfsFileStatusTest {
  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    String contentHash = CommonUtils.randomAlphaNumString(10);
    long contentLength = random.nextLong();
    long lastModifiedTimeMs = random.nextLong();
    short mode = 077;
    long blockSize = random.nextLong();
    UfsFileStatus status =
        new UfsFileStatus("name", contentHash, contentLength, lastModifiedTimeMs, "owner", "group",
            mode, blockSize);

    assertEquals("name", status.getName());
    assertEquals(contentHash, status.getContentHash());
    assertEquals(contentLength, status.getContentLength());
    assertEquals(false, status.isDirectory());
    assertEquals(true, status.isFile());
    assertEquals(lastModifiedTimeMs, (long) status.getLastModifiedTime());
    assertEquals("owner", status.getOwner());
    assertEquals("group", status.getGroup());
    assertEquals(mode, status.getMode());
    assertEquals(blockSize, status.getBlockSize());
  }

  /**
   * Tests if the copy constructor works.
   */
  @Test
  public void copy() {
    Random random = new Random();
    String contentHash = CommonUtils.randomAlphaNumString(10);
    long contentLength = random.nextLong();
    long lastModifiedTimeMs = random.nextLong();
    short mode = 077;
    long blockSize = random.nextLong();

    UfsFileStatus statusToCopy =
        new UfsFileStatus("name", contentHash, contentLength, lastModifiedTimeMs, "owner", "group",
            mode, blockSize);
    UfsFileStatus status = new UfsFileStatus(statusToCopy);
    assertEquals(statusToCopy, status);
  }
}
