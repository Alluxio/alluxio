/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.block.meta;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;

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
