package tachyon.master;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.Pair;
import tachyon.UnderFileSystem;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.NetworkUtils;

/**
 * Block info on the master side.
 */
public class BlockInfo {
  /**
   * Compute the block's id with the inode's id and the block's index in the inode. In Tachyon, the
   * blockId is equal to ((inodeId << 30) + blockIndex).
   * 
   * @param inodeId
   *          The inode's id of the block
   * @param blockIndex
   *          The block's index of the block in the inode
   * @return the block's id
   */
  public static long computeBlockId(int inodeId, int blockIndex) {
    return ((long) inodeId << 30) + blockIndex;
  }

  /**
   * Compute the block's index in the inode with the block's id. The blockIndex is the last 30 bits
   * of the blockId.
   * 
   * @param blockId
   *          The id of the block
   * @return the block's index in the inode
   */
  public static int computeBlockIndex(long blockId) {
    return (int) (blockId & 0x3fffffff);
  }

  /**
   * Compute the inode's id of the block. The inodeId is the first 34 bits of the blockId.
   * 
   * @param blockId
   *          The id of the block
   * @return the inode's id of the block
   */
  public static int computeInodeId(long blockId) {
    return (int) (blockId >> 30);
  }

  private final InodeFile INODE_FILE;

  public final int BLOCK_INDEX;

  public final long BLOCK_ID;

  public final long OFFSET;

  public final long LENGTH;

  private Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>(5);

  /**
   * @param inodeFile
   * @param blockIndex
   * @param length
   *          Can not be no bigger than 2^31 - 1
   */
  BlockInfo(InodeFile inodeFile, int blockIndex, long length) {
    INODE_FILE = inodeFile;
    BLOCK_INDEX = blockIndex;
    BLOCK_ID = computeBlockId(INODE_FILE.getId(), BLOCK_INDEX);
    OFFSET = inodeFile.getBlockSizeByte() * blockIndex;
    LENGTH = length;
  }

  /**
   * Add a location of the block. It means that the worker has the data of the block in memory.
   * 
   * @param workerId
   *          The id of the worker
   * @param workerAddress
   *          The net address of the worker
   */
  public synchronized void addLocation(long workerId, NetAddress workerAddress) {
    mLocations.put(workerId, workerAddress);
  }

  /**
   * Generate a ClientBlockInfo of the block, which is used for the thrift server.
   * 
   * @return the generated ClientBlockInfo
   */
  public synchronized ClientBlockInfo generateClientBlockInfo() {
    ClientBlockInfo ret = new ClientBlockInfo();

    ret.blockId = BLOCK_ID;
    ret.offset = OFFSET;
    ret.length = LENGTH;
    ret.locations = getLocations();

    return ret;
  }

  /**
   * Get the list of pairs "blockId, workerId", where the blockId is the id of this block, and the
   * workerId is the id of the worker who has the block's data in memory.
   * 
   * @return the list of those pairs
   */
  public synchronized List<Pair<Long, Long>> getBlockIdWorkerIdPairs() {
    List<Pair<Long, Long>> ret = new ArrayList<Pair<Long, Long>>(mLocations.size());
    for (long workerId : mLocations.keySet()) {
      ret.add(new Pair<Long, Long>(BLOCK_ID, workerId));
    }
    return ret;
  }

  /**
   * Get the InodeFile of the block
   * 
   * @return the InodeFile of the block
   */
  public synchronized InodeFile getInodeFile() {
    return INODE_FILE;
  }

  /**
   * Get the locations of the block, which are the workers' net address who has the data of the
   * block in memory.
   * 
   * @return the net addresses of the locations
   */
  public synchronized List<NetAddress> getLocations() {
    List<NetAddress> ret = new ArrayList<NetAddress>(mLocations.size());
    ret.addAll(mLocations.values());
    if (ret.isEmpty() && INODE_FILE.hasCheckpointed()) {
      UnderFileSystem ufs = UnderFileSystem.get(INODE_FILE.getUfsPath());
      List<String> locs = null;
      try {
        locs = ufs.getFileLocations(INODE_FILE.getUfsPath(), OFFSET);
      } catch (IOException e) {
        return ret;
      }
      if (locs != null) {
        for (String loc : locs) {
          String resolvedHost;
          try {
            resolvedHost = NetworkUtils.resolveHostName(loc);
          } catch (UnknownHostException e) {
            resolvedHost = loc;
          }
          ret.add(new NetAddress(resolvedHost, -1, -1));
        }
      }
    }
    return ret;
  }

  /**
   * @return true if the block is in some worker's memory, false otherwise
   */
  public synchronized boolean isInMemory() {
    return mLocations.size() > 0;
  }

  /**
   * Remove the worker from the block's locations
   * 
   * @param workerId
   *          The id of the removed worker
   */
  public synchronized void removeLocation(long workerId) {
    mLocations.remove(workerId);
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("BlockInfo(BLOCK_INDEX: ");
    sb.append(BLOCK_INDEX);
    sb.append(", BLOCK_ID: ").append(BLOCK_ID);
    sb.append(", OFFSET: ").append(OFFSET);
    sb.append(", LENGTH: ").append(LENGTH);
    sb.append(", mLocations: ").append(mLocations).append(")");
    return sb.toString();
  }
}
