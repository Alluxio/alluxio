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

import alluxio.CommonTestUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the {@link UnderFileStatus} class.
 */
public final class UnderFileStatusTest {
  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long contentLength = random.nextLong();
    boolean isDirectory = random.nextBoolean();
    long lastModifiedTimeMs = random.nextLong();
    short mode = 077;
    UnderFileStatus status = new UnderFileStatus("name", contentLength, isDirectory,
        lastModifiedTimeMs, "owner", "group", mode);

    Assert.assertEquals("name", status.getName());
    Assert.assertEquals(contentLength, status.getContentLength());
    Assert.assertEquals(isDirectory, status.isDirectory());
    Assert.assertEquals(!isDirectory, status.isFile());
    Assert.assertEquals(lastModifiedTimeMs, status.getLastModifiedTime());
    Assert.assertEquals("owner", status.getOwner());
    Assert.assertEquals("group", status.getGroup());
    Assert.assertEquals(mode, status.getMode());
  }
}
