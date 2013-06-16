package tachyon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon file system's file representation in master.
 */
public class InodeFile extends Inode {
  private static final long UNINITIAL_VALUE = -1;

  private final int BLOCK_SIZE_BYTE;

  private long mLength;
  private boolean mPin = false;
  private boolean mCache = false;
  private String mCheckpointPath = "";
  private List<BlockInfo> mBlocks = new ArrayList<BlockInfo>(5);

  private Map<Long, NetAddress> mLocations = new HashMap<Long, NetAddress>();

  public InodeFile(String name, int id, int parentId) {
    this(name, id, parentId, Constants.DEFAULT_BLOCK_SIZE_BYTE);
  }

  public InodeFile(String name, int id, int parentId, int blockSizeByte) {
    super(name, id, parentId, InodeType.File);
    BLOCK_SIZE_BYTE = blockSizeByte;
    mLength = UNINITIAL_VALUE;
  }

  public synchronized long getLength() {
    return mLength;
  }

  public synchronized void setLength(long length) throws SuspectedFileSizeException {
    // TODO Set block info at the same time.
    if (mLength != UNINITIAL_VALUE) {
      throw new SuspectedFileSizeException("InodeFile length was set previously.");
    }
    if (length < 0) {
      throw new SuspectedFileSizeException("InodeFile new length " + length + " is illegal.");
    }
    mLength = length;
  }

  public synchronized boolean isReady() {
    return mLength != UNINITIAL_VALUE;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH:").append(mLength);
    sb.append(", CheckpointPath:").append(mCheckpointPath).append(")");
    return sb.toString();
  }

  public synchronized void setCheckpointPath(String checkpointPath) {
    mCheckpointPath = checkpointPath;
  }

  public synchronized String getCheckpointPath() {
    return mCheckpointPath;
  }
  
  public synchronized void addBlock(BlockInfo blockInfo) throws BlockInfoException {
    if (mBlocks.size() > 0 && mBlocks.get(mBlocks.size() - 1).LENGTH != BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("BLOCK_SIZE_BYTE is " + BLOCK_SIZE_BYTE + ", but the " +
      		"previous block size is " + mBlocks.get(mBlocks.size() - 1).LENGTH);
    }
    if (blockInfo.getInodeFile() != this) {
      throw new BlockInfoException("Inode");
    }
  }

  public synchronized void addLocation(int blockIndex, long workerId, NetAddress workerAddress) {
    mLocations.put(workerId, workerAddress);
  }

  public synchronized void removeLocation(int blockIndex, long workerId) {
    mLocations.remove(workerId);
  }

  public int getBlockSizeByte() {
    return BLOCK_SIZE_BYTE;
  }

  public synchronized List<ClientBlockInfo> getBlockLocations(int blockId) {
    // TODO Implement this.
    List<NetAddress> ret = new ArrayList<NetAddress>(mLocations.size());
    ret.addAll(mLocations.values());
    if (ret.isEmpty() && hasCheckpointed()) {
      UnderFileSystem ufs = UnderFileSystem.get(mCheckpointPath);
      List<String> locs = null;
      try {
        locs = ufs.getFileLocations(mCheckpointPath);
      } catch (IOException e) {
        return ret;
      }
      if (locs != null) {
        for (String loc: locs) {
          ret.add(new NetAddress(loc, -1));
        }
      }
    }
    return ret;
  }

  public synchronized List<ClientBlockInfo> getLocations() {
    List<NetAddress> ret = new ArrayList<NetAddress>(mLocations.size());
    ret.addAll(mLocations.values());
    if (ret.isEmpty() && hasCheckpointed()) {
      UnderFileSystem ufs = UnderFileSystem.get(mCheckpointPath);
      List<String> locs = null;
      try {
        locs = ufs.getFileLocations(mCheckpointPath);
      } catch (IOException e) {
        return ret;
      }
      if (locs != null) {
        for (String loc: locs) {
          ret.add(new NetAddress(loc, -1));
        }
      }
    }
    return ret;
  }

  public synchronized boolean isInMemory() {
    return mLocations.size() > 0;
  }

  public synchronized void setPin(boolean pin) {
    mPin = pin;
  }

  public synchronized boolean isPin() {
    return mPin;
  }

  public synchronized void setCache(boolean cache) {
    mCache = cache;
  }

  public synchronized boolean isCache() {
    return mCache;
  }

  public synchronized boolean hasCheckpointed() {
    return !mCheckpointPath.equals("");
  }
}