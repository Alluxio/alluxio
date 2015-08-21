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

package tachyon.master.next.block.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.StorageLevelAlias;

/**
 * Unit tests for MasterBlockInfo.
 */
public final class MasterBlockInfoTests {
  private MasterBlockInfo mInfo;

  @Before
  public void before() throws Exception {
    mInfo = new MasterBlockInfo(0, Constants.KB);
  }

  @Test
  public void addWorkerTest() {
    assertEquals(0, mInfo.getWorkers().size());
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    assertTrue(mInfo.getWorkers().contains(1L));
  }

  @Test
  public void removeWorkerTest() {
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    assertTrue(mInfo.getWorkers().contains(1L));
    mInfo.removeWorker(1);
    assertEquals(0, mInfo.getWorkers().size());
  }

  @Test
  public void getNumLocationsTest() {
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    mInfo.addWorker(2, StorageLevelAlias.MEM.getValue());
    mInfo.addWorker(3, StorageLevelAlias.HDD.getValue());
    assertEquals(3, mInfo.getNumLocations());
  }

  @Test
  public void getBlockLocationsTest() {
    mInfo.addWorker(3, StorageLevelAlias.HDD.getValue());
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    mInfo.addWorker(2, StorageLevelAlias.MEM.getValue());

    List<MasterBlockLocation> locations = mInfo.getBlockLocations();
    assertEquals(3, mInfo.getNumLocations());
    // mem in the top of the list
    assertEquals(1, locations.get(0).getWorkerId());
    assertEquals(2, locations.get(1).getWorkerId());
    assertEquals(3, locations.get(2).getWorkerId());
  }

  @Test
  public void isInMemoryTest() {
    mInfo.addWorker(3, StorageLevelAlias.HDD.getValue());
    assertFalse(mInfo.isInMemory());
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    assertTrue(mInfo.isInMemory());
  }
}
