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
package tachyon.master;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.BlockInfo.
 */
public class BlockInfoTest {
  private TachyonConf mTachyonConf;

  @Before
  public void before() {
    mTachyonConf = new TachyonConf();
  }

  @Test
  public void computeBlockIdTest() {
    Assert.assertEquals(1073741824, BlockInfo.computeBlockId(1, 0));
    Assert.assertEquals(1073741825, BlockInfo.computeBlockId(1, 1));
    Assert.assertEquals(2147483646, BlockInfo.computeBlockId(1, 1073741822));
    Assert.assertEquals(2147483647, BlockInfo.computeBlockId(1, 1073741823));
    Assert.assertEquals(3221225472L, BlockInfo.computeBlockId(3, 0));
    Assert.assertEquals(3221225473L, BlockInfo.computeBlockId(3, 1));
    Assert.assertEquals(4294967294L, BlockInfo.computeBlockId(3, 1073741822));
    Assert.assertEquals(4294967295L, BlockInfo.computeBlockId(3, 1073741823));
  }

  @Test
  public void computeBlockIndexTest() {
    Assert.assertEquals(0, BlockInfo.computeBlockIndex(1073741824));
    Assert.assertEquals(1, BlockInfo.computeBlockIndex(1073741825));
    Assert.assertEquals(1073741822, BlockInfo.computeBlockIndex(2147483646));
    Assert.assertEquals(1073741823, BlockInfo.computeBlockIndex(2147483647));
    Assert.assertEquals(0, BlockInfo.computeBlockIndex(3221225472L));
    Assert.assertEquals(1, BlockInfo.computeBlockIndex(3221225473L));
    Assert.assertEquals(1073741822, BlockInfo.computeBlockIndex(4294967294L));
    Assert.assertEquals(1073741823, BlockInfo.computeBlockIndex(4294967295L));
  }

  @Test
  public void computeInodeIdTest() {
    Assert.assertEquals(1, BlockInfo.computeInodeId(1073741824));
    Assert.assertEquals(1, BlockInfo.computeInodeId(1073741825));
    Assert.assertEquals(1, BlockInfo.computeInodeId(2147483646));
    Assert.assertEquals(1, BlockInfo.computeInodeId(2147483647));
    Assert.assertEquals(3, BlockInfo.computeInodeId(3221225472L));
    Assert.assertEquals(3, BlockInfo.computeInodeId(3221225473L));
    Assert.assertEquals(3, BlockInfo.computeInodeId(4294967294L));
    Assert.assertEquals(3, BlockInfo.computeInodeId(4294967295L));
  }

  @Test
  public void constructorTest() {
    BlockInfo tInfo =
        new BlockInfo(new InodeFile("t", 100, 0, Constants.DEFAULT_BLOCK_SIZE_BYTE,
            System.currentTimeMillis()), 300, 800);
    Assert.assertEquals(tInfo.mBlockIndex, 300);
    Assert.assertEquals(tInfo.mBlockId, BlockInfo.computeBlockId(100, 300));
    Assert.assertEquals(tInfo.mOffset, (long) Constants.DEFAULT_BLOCK_SIZE_BYTE * 300);
    Assert.assertEquals(tInfo.mLength, 800);
  }

  @Test(expected = IllegalArgumentException.class)
  public void constructorTestWithIllegalArgumentException() {
    new BlockInfo(new InodeFile("t", 100, 0, Constants.DEFAULT_BLOCK_SIZE_BYTE,
        System.currentTimeMillis()), 300, (1L << 31));
  }

  @Test
  public void generateClientBlockInfoTest() {
    BlockInfo tInfo =
        new BlockInfo(new InodeFile("t", 100, 0, Constants.DEFAULT_BLOCK_SIZE_BYTE,
            System.currentTimeMillis()), 300, 800);
    long storageDirId = StorageDirId.getStorageDirId(0, StorageLevelAlias.MEM.getValue(), 0);
    tInfo.addLocation(15, new NetAddress("abc", 1, 11), storageDirId);
    tInfo.addLocation(22, new NetAddress("def", 2, 21), storageDirId);
    tInfo.addLocation(29, new NetAddress("gh", 3, 31), storageDirId);
    ClientBlockInfo clientBlockInfo = tInfo.generateClientBlockInfo(mTachyonConf);
    Assert.assertEquals((long) Constants.DEFAULT_BLOCK_SIZE_BYTE * 300, clientBlockInfo.offset);
    Assert.assertEquals(800, clientBlockInfo.length);
    Assert.assertEquals(3, clientBlockInfo.getWorkers().size());
  }

  private List<String> getHosts(List<NetAddress> locations) {
    List<String> ret = new ArrayList<String>();
    for (NetAddress address : locations) {
      ret.add(address.getMHost());
    }
    return ret;
  }

  @Test
  public void locationTest() {
    BlockInfo tInfo =
        new BlockInfo(new InodeFile("t", 100, 0, Constants.DEFAULT_BLOCK_SIZE_BYTE,
            System.currentTimeMillis()), 300, 800);
    long memStorageDirId = StorageDirId.getStorageDirId(0, StorageLevelAlias.MEM.getValue(), 0);
    long ssdStorageDirId = StorageDirId.getStorageDirId(1, StorageLevelAlias.SSD.getValue(), 0);
    long hddStorageDirId = StorageDirId.getStorageDirId(2, StorageLevelAlias.HDD.getValue(), 0);
    Assert.assertEquals(0, tInfo.getWorkerAddresses().size());
    tInfo.addLocation(15, new NetAddress("abc", 1, 11), hddStorageDirId);
    Assert.assertEquals(1, tInfo.getWorkerAddresses().size());
    tInfo.addLocation(22, new NetAddress("def", 2, 21), memStorageDirId);
    List<NetAddress> locations = tInfo.getWorkerAddresses();
    List<String> hosts = getHosts(locations);
    Assert.assertEquals(2, locations.size());
    Assert.assertTrue(hosts.contains("def"));
    Assert.assertTrue(hosts.contains("abc"));
    tInfo.addLocation(29, new NetAddress("gh", 3, 31), ssdStorageDirId);
    locations = tInfo.getWorkerAddresses();
    hosts = getHosts(locations);
    Assert.assertEquals(3, locations.size());
    Assert.assertTrue(hosts.contains("def"));
    Assert.assertTrue(hosts.contains("gh"));
    Assert.assertTrue(hosts.contains("abc"));
    tInfo.addLocation(15, new NetAddress("abc", 1, 11), hddStorageDirId);
    Assert.assertEquals(3, tInfo.getWorkerAddresses().size());
    tInfo.addLocation(22, new NetAddress("def", 2, 21), memStorageDirId);
    Assert.assertEquals(3, tInfo.getWorkerAddresses().size());
    tInfo.addLocation(36, new NetAddress("ij", 4, 41), memStorageDirId);
    locations = tInfo.getWorkerAddresses();
    hosts = getHosts(locations);
    Assert.assertEquals(4, locations.size());
    Assert.assertEquals(10, locations.get(0).getMPort() + locations.get(1).getMPort()
        + locations.get(2).getMPort() + locations.get(3).getMPort());
    Assert.assertTrue(hosts.contains("gh"));
    Assert.assertTrue(hosts.contains("abc"));
    tInfo.removeLocation(15, hddStorageDirId);
    tInfo.removeLocation(36, memStorageDirId);
    locations = tInfo.getWorkerAddresses();
    Assert.assertEquals(2, locations.size());
    Assert.assertTrue(hosts.contains("def"));
    Assert.assertTrue(hosts.contains("gh"));
    tInfo.removeLocation(10, memStorageDirId);
    Assert.assertEquals(2, tInfo.getWorkerAddresses().size());
  }

  @Test
  public void isInMemoryTest1() {
    // First add all the pages in SSD, and isInMemory should be false. Then add them
    // in MEM, and it should return true
    BlockInfo tInfo =
        new BlockInfo(new InodeFile("t", 100, 0, Constants.DEFAULT_BLOCK_SIZE_BYTE,
            System.currentTimeMillis()), 300, Constants.MB * 10);
    long memStorageDirId = StorageDirId.getStorageDirId(0, StorageLevelAlias.MEM.getValue(), 0);
    long ssdStorageDirId = StorageDirId.getStorageDirId(0, StorageLevelAlias.SSD.getValue(), 0);
    // Add all the pages in SSD
    tInfo.addLocation(1, new NetAddress("abc", 1, 1), ssdStorageDirId);
    Assert.assertEquals(false, tInfo.isInMemory());
    // Add all the pages in MEM
    tInfo.addLocation(2, new NetAddress("def", 1, 1), memStorageDirId);
    Assert.assertEquals(true, tInfo.isInMemory());
  }
}
