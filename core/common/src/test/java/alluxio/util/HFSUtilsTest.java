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

package alluxio.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the {@link HFSUtils} class.
 */
public class HFSUtilsTest {

  /**
   * Tests the {@link HFSUtils#getNumSector(String, String)} method.
   */
  @Test
  public void getSectorTest0() {
    String testRequestSize = "0";
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(1L, result);
  }

  @Test
  public void getSectorTest20() {
    String testRequestSize = "20";
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(1L, result);
  }

  @Test
  public void getSectorTest512() {
    String testRequestSize = "512";
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(2L, result);
  }

  @Test
  public void getSectorTestMB() {
    String testRequestSize = "1048576"; // 1MB
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(2080L, result); // 1MB/512B = 2048
  }

  @Test
  public void getSectorTestGB() {
    String testRequestSize = "1073741824"; // 1GB
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(2128667L, result); // 1GB/512B = 2097152
  }

  @Test
  public void getSectorTest100GB() {
    String testRequestSize = "107374182400"; // 100GB
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(212866577L, result); // 100GB/512B = 209715200
  }

  @Test
  public void getSectorTest512GB() {
    String testRequestSize = "549755813888"; // 512GB
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(1089876870L, result);
  }

  @Test
  public void getSectorTestTB() {
    String testRequestSize = "1099511627776"; // 1TB
    String testSectorSize = "512";
    long result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    assertEquals(2179753739L, result);
  }
}
