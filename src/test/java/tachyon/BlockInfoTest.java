package tachyon;

import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.BlockInfo.
 */
public class BlockInfoTest {
  @Test
  public void constructorTest() {
    BlockInfo tInfo = new BlockInfo(new InodeFile("t", 100, 0), 300, 1500, 800);
    Assert.assertEquals(tInfo.BLOCK_INDEX, 300);
    Assert.assertEquals(tInfo.BLOCK_ID, BlockInfo.computeBlockId(100, 300));
    Assert.assertEquals(tInfo.OFFSET, 1500);
    Assert.assertEquals(tInfo.LENGTH, 800);
  }

  @Test
  public void localtionTest() {
    BlockInfo tInfo = new BlockInfo(new InodeFile("t", 100, 0), 300, 1500, 800);
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

  @Test
  public void generateClientBlockInfoTest() {
    BlockInfo tInfo = new BlockInfo(new InodeFile("t", 100, 0), 300, 1500, 800);
    tInfo.addLocation(15, new NetAddress("abc", 1));
    tInfo.addLocation(22, new NetAddress("def", 2));
    tInfo.addLocation(29, new NetAddress("gh", 3));
    ClientBlockInfo clientBlockInfo = tInfo.generateClientBlockInfo();
    Assert.assertEquals(1500, clientBlockInfo.offset);
    Assert.assertEquals(800, clientBlockInfo.length);
    Assert.assertEquals(3, clientBlockInfo.locations.size());
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
}
