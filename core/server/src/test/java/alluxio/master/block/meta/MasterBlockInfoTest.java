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

package alluxio.master.block.meta;

import alluxio.Constants;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Unit tests for {@link MasterBlockInfo}.
 */
public final class MasterBlockInfoTest {
  private MasterBlockInfo mInfo;

  @Before
  public void before() {
    mInfo = new MasterBlockInfo(0, Constants.KB);
  }

  @Test
  public void addWorker() {
    mInfo.addWorker(1, "MEM");
    Assert.assertTrue(mInfo.getWorkers().contains(1L));
  }

  @Test
  public void removeWorker() {
    mInfo.addWorker(1, "MEM");
    mInfo.removeWorker(1);
    Assert.assertEquals(0, mInfo.getWorkers().size());
  }

  @Test
  public void removeNonexistingWorkerIsOk() {
    mInfo.removeWorker(1);
  }

  @Test
  public void getNumLocations() {
    mInfo.addWorker(1, "MEM");
    mInfo.addWorker(2, "MEM");
    mInfo.addWorker(3, "HDD");
    Assert.assertEquals(3, mInfo.getNumLocations());
  }

  @Test
  public void getBlockLocations() {
    mInfo.addWorker(1, "MEM");
    mInfo.addWorker(2, "MEM");
    mInfo.addWorker(3, "HDD");

    List<MasterBlockLocation> locations = mInfo.getBlockLocations();
    Set<MasterBlockLocation> expectedLocations = ImmutableSet.of(
        new MasterBlockLocation(1, "MEM"),
        new MasterBlockLocation(2, "MEM"),
        new MasterBlockLocation(3, "HDD"));
    Assert.assertEquals(expectedLocations, ImmutableSet.copyOf(locations));
  }

  @Test
  public void isInTier() {
    mInfo.addWorker(1, "HDD");
    Assert.assertTrue(mInfo.isInTier("HDD"));
  }

  @Test
  public void isNotInTier() {
    mInfo.addWorker(1, "HDD");
    Assert.assertFalse(mInfo.isInTier("MEM"));
  }

  @Test
  public void getLength() {
    Assert.assertEquals(Constants.KB, mInfo.getLength());
  }

  @Test
  public void updateKnownLengthDoesNothing() {
    mInfo.updateLength(2 * Constants.KB);
    Assert.assertEquals(Constants.KB, mInfo.getLength());
  }

  @Test
  public void updateUnknownLengthUpdates() {
    MasterBlockInfo info = new MasterBlockInfo(0, Constants.UNKNOWN_SIZE);
    info.updateLength(2 * Constants.KB);
    Assert.assertEquals(2 * Constants.KB, info.getLength());
  }
}
