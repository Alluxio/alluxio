/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.block.meta;

import alluxio.Constants;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Unit tests for {@link MasterBlockInfo}.
 */
public final class MasterBlockInfoTest {
  private MasterBlockInfo mInfo;

  /**
   * Sets up a new {@link MasterBlockInfo} before a test runs.
   */
  @Before
  public void before() {
    mInfo = new MasterBlockInfo(0, Constants.KB);
  }

  /**
   * Tests the {@link MasterBlockInfo#addWorker(long, String)} method.
   */
  @Test
  public void addWorkerTest() {
    Assert.assertEquals(0, mInfo.getWorkers().size());
    mInfo.addWorker(1, "MEM");
    Assert.assertTrue(mInfo.getWorkers().contains(1L));
    mInfo.addWorker(1, "MEM");
    Assert.assertEquals(1, mInfo.getWorkers().size());
  }

  /**
   * Tests the {@link MasterBlockInfo#removeWorker(long)} method.
   */
  @Test
  public void removeWorkerTest() {
    mInfo.addWorker(1, "MEM");
    Assert.assertTrue(mInfo.getWorkers().contains(1L));
    mInfo.removeWorker(1);
    Assert.assertEquals(0, mInfo.getWorkers().size());

    // remove nonexiting worker.
    mInfo.removeWorker(1);
    Assert.assertEquals(0, mInfo.getWorkers().size());
  }

  /**
   * Tests the {@link MasterBlockInfo#getNumLocations()} method.
   */
  @Test
  public void getNumLocationsTest() {
    mInfo.addWorker(1, "MEM");
    mInfo.addWorker(2, "MEM");
    mInfo.addWorker(3, "HDD");
    Assert.assertEquals(3, mInfo.getNumLocations());
  }

  /**
   * Tests the {@link MasterBlockInfo#getBlockLocations()} method.
   */
  @Test
  public void getBlockLocationsTest() {
    mInfo.addWorker(3, "HDD");
    mInfo.addWorker(1, "MEM");
    mInfo.addWorker(2, "MEM");

    List<MasterBlockLocation> locations = mInfo.getBlockLocations();
    Assert.assertEquals(3, mInfo.getNumLocations());
    // mem in the top of the list
    Assert.assertEquals(1, locations.get(0).getWorkerId());
    Assert.assertEquals(2, locations.get(1).getWorkerId());
    Assert.assertEquals(3, locations.get(2).getWorkerId());
  }

  /**
   * Tests that setting the tier alias via the {@link MasterBlockInfo#addWorker(long, String)}
   * method works together with the {@link MasterBlockInfo#isInTier(String)} method.
   */
  @Test
  public void isInMemoryTest() {
    mInfo.addWorker(3, "HDD");
    Assert.assertFalse(mInfo.isInTier("MEM"));
    mInfo.addWorker(1, "MEM");
    Assert.assertTrue(mInfo.isInTier("MEM"));
  }
}
