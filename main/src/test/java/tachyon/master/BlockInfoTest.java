/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;
import tachyon.master.BlockInfo;
import tachyon.master.InodeFile;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.BlockInfo.
 */
public class BlockInfoTest {
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
    Assert.assertEquals(tInfo.BLOCK_INDEX, 300);
    Assert.assertEquals(tInfo.BLOCK_ID, BlockInfo.computeBlockId(100, 300));
    Assert.assertEquals(tInfo.OFFSET, (long) Constants.DEFAULT_BLOCK_SIZE_BYTE * 300);
    Assert.assertEquals(tInfo.LENGTH, 800);
  }

  @Test
  public void generateClientBlockInfoTest() {
    BlockInfo tInfo =
        new BlockInfo(new InodeFile("t", 100, 0, Constants.DEFAULT_BLOCK_SIZE_BYTE,
            System.currentTimeMillis()), 300, 800);
    tInfo.addLocation(15, new NetAddress("abc", 1));
    tInfo.addLocation(22, new NetAddress("def", 2));
    tInfo.addLocation(29, new NetAddress("gh", 3));
    ClientBlockInfo clientBlockInfo = tInfo.generateClientBlockInfo();
    Assert.assertEquals((long) Constants.DEFAULT_BLOCK_SIZE_BYTE * 300, clientBlockInfo.offset);
    Assert.assertEquals(800, clientBlockInfo.length);
    Assert.assertEquals(3, clientBlockInfo.locations.size());
  }

  @Test
  public void localtionTest() {
    BlockInfo tInfo =
        new BlockInfo(new InodeFile("t", 100, 0, Constants.DEFAULT_BLOCK_SIZE_BYTE,
            System.currentTimeMillis()), 300, 800);
    tInfo.addLocation(15, new NetAddress("abc", 1));
    Assert.assertEquals(1, tInfo.getLocations().size());
    tInfo.addLocation(22, new NetAddress("def", 2));
    Assert.assertEquals(2, tInfo.getLocations().size());
    tInfo.addLocation(29, new NetAddress("gh", 3));
    Assert.assertEquals(3, tInfo.getLocations().size());
    tInfo.addLocation(15, new NetAddress("abc", 1));
    Assert.assertEquals(3, tInfo.getLocations().size());
    tInfo.addLocation(22, new NetAddress("def", 2));
    Assert.assertEquals(3, tInfo.getLocations().size());
    tInfo.addLocation(29, new NetAddress("gh", 3));
    Assert.assertEquals(3, tInfo.getLocations().size());
    tInfo.removeLocation(15);
    Assert.assertEquals(2, tInfo.getLocations().size());
    tInfo.removeLocation(10);
    Assert.assertEquals(2, tInfo.getLocations().size());
  }
}
