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

import org.junit.Test;

/**
 * Tests for the {@link UfsDirectoryStatus} class.
 */
public final class UfsDirectoryStatusTest {
  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    short mode = 077;
    UfsDirectoryStatus status = new UfsDirectoryStatus("name", "owner", "group", mode);

    assertEquals("name", status.getName());
    assertEquals(true, status.isDirectory());
    assertEquals(false, status.isFile());
    assertEquals("owner", status.getOwner());
    assertEquals("group", status.getGroup());
    assertEquals(mode, status.getMode());
  }

  /**
   * Tests if the copy constructor works.
   */
  @Test
  public void copy() {
    short mode = 077;
    UfsDirectoryStatus statusToCopy =
        new UfsDirectoryStatus("name", "owner", "group", mode);
    UfsDirectoryStatus status = new UfsDirectoryStatus(statusToCopy);
    assertEquals(statusToCopy, status);
  }
}
