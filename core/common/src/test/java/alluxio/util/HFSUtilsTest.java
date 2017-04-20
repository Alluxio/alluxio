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

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
/**
 * Tests the {@link HFSUtils} class.
 */
public class HFSUtilsTest {

  /**
   * Tests the {@link HFSUtils#getNumSector(String, String)} method.
   */
  @Test
  public void getSectorTest1() {
    String testRequestSize = "0";
    String testSectorSize = "512";
    BigDecimal result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    Assert.assertEquals(new BigDecimal("1"), result);
  }

  @Test
  public void getSectorTest2() {
    String testRequestSize = "20";
    String testSectorSize = "512";
    BigDecimal result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    Assert.assertEquals(new BigDecimal("1"), result);
  }

  @Test
  public void getSectorTest3() {
    String testRequestSize = "512";
    String testSectorSize = "512";
    BigDecimal result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    Assert.assertEquals(new BigDecimal("2"), result);
  }

  @Test
  public void getSectorTest4() {
    String testRequestSize = "1048576"; // 1MB
    String testSectorSize = "512";
    BigDecimal result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    Assert.assertEquals(new BigDecimal("2080"), result); // 1MB/512B = 2048
  }

  @Test
  public void getSectorTest5() {
    String testRequestSize = "1073741824"; // 1GB
    String testSectorSize = "512";
    BigDecimal result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    Assert.assertEquals(new BigDecimal("2128667"), result); // 1GB/512B = 2097152
  }

  @Test
  public void getSectorTest6() {
    String testRequestSize = "107374182400"; // 100GB
    String testSectorSize = "512";
    BigDecimal result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    Assert.assertEquals(new BigDecimal("212866577"), result); // 100GB/512B = 209715200
  }

  @Test
  public void getSectorTest7() {
    String testRequestSize = "1099511627776"; // 1TB
    String testSectorSize = "512";
    BigDecimal result = HFSUtils.getNumSector(testRequestSize, testSectorSize);
    Assert.assertEquals(new BigDecimal("2179753739"), result);
  }
}
