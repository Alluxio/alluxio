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
    Assert.assertEquals(0, mInfo.getWorkers().size());
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    Assert.assertTrue(mInfo.getWorkers().contains(1L));
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    Assert.assertEquals(1, mInfo.getWorkers().size());
  }

  @Test
  public void removeWorkerTest() {
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    Assert.assertTrue(mInfo.getWorkers().contains(1L));
    mInfo.removeWorker(1);
    Assert.assertEquals(0, mInfo.getWorkers().size());

    // remove nonexiting worker.
    mInfo.removeWorker(1);
    Assert.assertEquals(0, mInfo.getWorkers().size());
  }

  @Test
  public void getNumLocationsTest() {
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    mInfo.addWorker(2, StorageLevelAlias.MEM.getValue());
    mInfo.addWorker(3, StorageLevelAlias.HDD.getValue());
    Assert.assertEquals(3, mInfo.getNumLocations());
  }

  @Test
  public void getBlockLocationsTest() {
    mInfo.addWorker(3, StorageLevelAlias.HDD.getValue());
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    mInfo.addWorker(2, StorageLevelAlias.MEM.getValue());

    List<MasterBlockLocation> locations = mInfo.getBlockLocations();
    Assert.assertEquals(3, mInfo.getNumLocations());
    // mem in the top of the list
    Assert.assertEquals(1, locations.get(0).getWorkerId());
    Assert.assertEquals(2, locations.get(1).getWorkerId());
    Assert.assertEquals(3, locations.get(2).getWorkerId());
  }

  @Test
  public void isInMemoryTest() {
    mInfo.addWorker(3, StorageLevelAlias.HDD.getValue());
    Assert.assertFalse(mInfo.isInMemory());
    mInfo.addWorker(1, StorageLevelAlias.MEM.getValue());
    Assert.assertTrue(mInfo.isInMemory());
  }
}
