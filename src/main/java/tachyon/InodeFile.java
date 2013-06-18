package tachyon;

import java.util.ArrayList;
import java.util.List;

import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon file system's file representation in master.
 */
public class InodeFile extends Inode {
  private static final long UNINITIAL_VALUE = -1;

  private final long BLOCK_SIZE_BYTE;

  private long mLength;
  private boolean mIsComplete = false;
  private boolean mPin = false;
  private boolean mCache = false;
  private String mCheckpointPath = "";
  private List<BlockInfo> mBlocks = new ArrayList<BlockInfo>(5);

  public InodeFile(String name, int id, int parentId, long blockSizeByte) {
    super(name, id, parentId, InodeType.File);
    BLOCK_SIZE_BYTE = blockSizeByte;
    mLength = UNINITIAL_VALUE;
  }

  public synchronized long getLength() {
    return mLength;
  }

  public synchronized void setLength(long length) 
      throws SuspectedFileSizeException, BlockInfoException {
    if (mLength != UNINITIAL_VALUE) {
      throw new SuspectedFileSizeException("InodeFile length was set previously.");
    }
    if (length < 0) {
      throw new SuspectedFileSizeException("InodeFile new length " + length + " is illegal.");
    }
    mLength = 0;
    while (length >= BLOCK_SIZE_BYTE) {
      addBlock(new BlockInfo(this, mBlocks.size(), BLOCK_SIZE_BYTE));
      length -= BLOCK_SIZE_BYTE;
    }
    if (length > 0) {
      addBlock(new BlockInfo(this, mBlocks.size(), (int) length));
    }
  }

  public synchronized boolean isComplete() {
    return mIsComplete;
  }

  public synchronized void setComplete() {
    mIsComplete = true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH: ").append(mLength);
    sb.append(", CheckpointPath: ").append(mCheckpointPath);
    sb.append(", mBlocks: ").append(mBlocks).append(")");
    return sb.toString();
  }

  public synchronized void setCheckpointPath(String checkpointPath) {
    mCheckpointPath = checkpointPath;
  }

  public synchronized String getCheckpointPath() {
    return mCheckpointPath;
  }

  public synchronized void addBlock(BlockInfo blockInfo) throws BlockInfoException {
    if (mIsComplete) {
      throw new BlockInfoException("The file is complete: " + this);
    }
    if (mBlocks.size() > 0 && mBlocks.get(mBlocks.size() - 1).LENGTH != BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("BLOCK_SIZE_BYTE is " + BLOCK_SIZE_BYTE + ", but the " +
          "previous block size is " + mBlocks.get(mBlocks.size() - 1).LENGTH);
    }
    if (blockInfo.getInodeFile() != this) {
      throw new BlockInfoException("InodeFile unmatch: " + this + " != " + blockInfo);
    }
    if (blockInfo.BLOCK_INDEX != mBlocks.size()) {
      throw new BlockInfoException("BLOCK_INDEX unmatch: " + mBlocks.size() + " != " + blockInfo);
    }
    if (blockInfo.OFFSET != (long) mBlocks.size() * BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("OFFSET unmatch: " + (long) mBlocks.size() * BLOCK_SIZE_BYTE +
          " != " + blockInfo);
    }
    if (blockInfo.LENGTH > BLOCK_SIZE_BYTE) {
      throw new BlockInfoException("LENGTH too big: " + BLOCK_SIZE_BYTE + " " + blockInfo);
    }
    if (mLength == UNINITIAL_VALUE) {
      mLength = 0;
    }
    mLength += blockInfo.LENGTH;
    mBlocks.add(blockInfo);
  }

  public synchronized void addLocation(int blockIndex, long workerId, NetAddress workerAddress) 
      throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException("BlockIndex " + blockIndex + " out of bounds." + toString());
    }
    mBlocks.get(blockIndex).addLocation(workerId, workerAddress);
  }

  public synchronized void removeLocation(int blockIndex, long workerId) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException("BlockIndex " + blockIndex + " out of bounds." + toString());
    }
    mBlocks.get(blockIndex).removeLocation(workerId);
  }

  public long getBlockSizeByte() {
    return BLOCK_SIZE_BYTE;
  }

  public synchronized List<NetAddress> getBlockLocations(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex > mBlocks.size()) {
      throw new BlockInfoException("BlockIndex is out of the boundry: " + blockIndex);
    }

    return mBlocks.get(blockIndex).getLocations();
  }

  public synchronized ClientBlockInfo getClientBlockInfo(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex > mBlocks.size()) {
      throw new BlockInfoException("BlockIndex is out of the boundry: " + blockIndex);
    }

    return mBlocks.get(blockIndex).generateClientBlockInfo();
  }

  /**
   * Get file's complete location information.
   * @return all blocks locations
   */
  public synchronized List<ClientBlockInfo> getLocations() {
    List<ClientBlockInfo> ret = new ArrayList<ClientBlockInfo>(mBlocks.size());
    for (BlockInfo tInfo: mBlocks) {
      ret.add(tInfo.generateClientBlockInfo());
    }
    return ret;
  }

  public synchronized boolean isFullyInMemory() {
    return getInMemoryPercentage() == 100;
  }

  public synchronized int getInMemoryPercentage() {
    long inMemoryLength = 0;
    for (BlockInfo info: mBlocks) {
      if (info.isInMemory()) {
        inMemoryLength += info.LENGTH;
      }
    }
    return (int) (inMemoryLength * 100 / mLength);
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

  public synchronized List<Long> getBlockIds() {
    List<Long> ret = new ArrayList<Long>(mBlocks.size());
    for (int k = 0; k < mBlocks.size(); k ++) {
      ret.add(mBlocks.get(k).BLOCK_ID);
    }
    return ret;
  }

  public synchronized List<Pair<Long, Long>> getBlockIdWorkerIdPairs() {
    List<Pair<Long, Long>> ret = new ArrayList<Pair<Long, Long>>();
    for (BlockInfo info: mBlocks) {
      ret.addAll(info.getBlockIdWorkerIdPairs());
    }
    return ret;
  }

  @Override
  public ClientFileInfo generateClientFileInfo(String path) {
    ClientFileInfo ret = new ClientFileInfo();

    ret.id = getId();
    ret.name = getName();
    ret.path = path;
    ret.checkpointPath = mCheckpointPath;
    ret.length = mLength;
    ret.blockSizeByte = BLOCK_SIZE_BYTE;
    ret.creationTimeMs = getCreationTimeMs();
    ret.complete = isComplete();
    ret.folder = false;
    ret.inMemory = isFullyInMemory();
    ret.needPin = mPin;
    ret.needCache = mCache;
    ret.blockIds = getBlockIds();

    return ret;
  }
}