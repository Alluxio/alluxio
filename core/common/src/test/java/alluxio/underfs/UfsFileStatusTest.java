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

import org.junit.Assert;
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
    long contentLength = random.nextLong();
    long lastModifiedTimeMs = random.nextLong();
    short mode = 077;
    UfsFileStatus status =
        new UfsFileStatus("name", contentLength, lastModifiedTimeMs, "owner", "group", mode);

    Assert.assertEquals("name", status.getName());
    Assert.assertEquals(contentLength, status.getContentLength());
    Assert.assertEquals(false, status.isDirectory());
    Assert.assertEquals(true, status.isFile());
    Assert.assertEquals(lastModifiedTimeMs, status.getLastModifiedTime());
    Assert.assertEquals("owner", status.getOwner());
    Assert.assertEquals("group", status.getGroup());
    Assert.assertEquals(mode, status.getMode());
  }

  /**
   * Tests if the copy constructor works.
   */
  @Test
  public void copy() {
    Random random = new Random();
    long contentLength = random.nextLong();
    long lastModifiedTimeMs = random.nextLong();
    short mode = 077;
    UfsFileStatus statusToCopy =
        new UfsFileStatus("name", contentLength, lastModifiedTimeMs, "owner", "group", mode);
    UfsFileStatus status = new UfsFileStatus(statusToCopy);
    Assert.assertEquals(statusToCopy, status);
  }
}
