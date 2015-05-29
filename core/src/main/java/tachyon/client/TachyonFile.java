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

package tachyon.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;

/**
 * Tachyon File.
 */
public class TachyonFile implements Comparable<TachyonFile> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  final TachyonFS mTachyonFS;
  final int mFileId;

  private Object mUFSConf = null;

  private final TachyonConf mTachyonConf;

  /**
   * A Tachyon File handler, based file id
   * 
   * @param tfs the Tachyon file system client handler
   * @param fid the file id
   * @param tachyonConf the TachyonConf for this file.
   */
  TachyonFile(TachyonFS tfs, int fid, TachyonConf tachyonConf) {
    mTachyonFS = tfs;
    mFileId = fid;
    mTachyonConf = tachyonConf;
  }

  private ClientFileInfo getCachedFileStatus() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, true);
  }

  private ClientFileInfo getUnCachedFileStatus() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, false);
  }

  @Override
  public int compareTo(TachyonFile o) {
    if (mFileId == o.mFileId) {
      return 0;
    }
    return mFileId < o.mFileId ? -1 : 1;
  }

  @Override
  public boolean equals(Object obj) {
    if ((obj != null) && (obj instanceof TachyonFile)) {
      return compareTo((TachyonFile) obj) == 0;
    }
    return false;
  }

  /**
   * Return the block id of a block in the file, specified by blockIndex
   * 
   * @param blockIndex the index of the block in this file
   * @return the block id
   * @throws IOException
   */
  public long getBlockId(int blockIndex) throws IOException {
    return mTachyonFS.getBlockId(mFileId, blockIndex);
  }

  /**
   * Get the block id by the file id and offset. it will check whether the file and the block exist.
   * 
   * @param offset The offset of the file.
   * @return the block id if exists
   * @throws IOException
   */
  long getBlockIdBasedOnOffset(long offset) throws IOException {
    return getBlockId((int) (offset / getBlockSizeByte()));
  }

  /**
   * Return the block's size of this file
   * 
   * @return the block's size in bytes
   * @throws IOException
   */
  public long getBlockSizeByte() throws IOException {
    return getCachedFileStatus().getBlockSizeByte();
  }

  /**
   * Get a ClientBlockInfo by the file id and block index
   * 
   * @param blockIndex The index of the block in the file.
   * @return the ClientBlockInfo of the specified block
   * @throws IOException
   */
  public synchronized ClientBlockInfo getClientBlockInfo(int blockIndex) throws IOException {
    return mTachyonFS.getClientBlockInfo(getBlockId(blockIndex));
  }

  /**
   * Return the creation time of this file
   * 
   * @return the creation time, in milliseconds
   * @throws IOException
   */
  public long getCreationTimeMs() throws IOException {
    return getCachedFileStatus().getCreationTimeMs();
  }

  public int getDiskReplication() {
    // TODO Implement it.
    return 3;
  }

  /**
   * Return the InStream of this file, use the specified read type. If it has no block, return an
   * EmptyBlockInStream. Else if it has only one block ,return a BlockInStream of the block. Else,
   * return a FileInStream.
   * 
   * @param readType the InStream's read type
   * @return the InStream
   * @throws IOException
   */
  public InStream getInStream(ReadType readType) throws IOException {
    if (readType == null) {
      throw new IOException("ReadType can not be null.");
    }

    if (!isComplete()) {
      throw new IOException("The file " + this + " is not complete.");
    }

    List<Long> blocks = getUnCachedFileStatus().getBlockIds();

    if (blocks.size() == 0) {
      return new EmptyBlockInStream(this, readType, mTachyonConf);
    } else if (blocks.size() == 1) {
      return BlockInStream.get(this, readType, 0, mUFSConf, mTachyonConf);
    }

    return new FileInStream(this, readType, mUFSConf, mTachyonConf);
  }

  /**
   * Returns the local filename for the block if that file exists on the local file system. This is
   * an alpha power-api feature for applications that want short-circuit-read files directly. There
   * is no guarantee that the file still exists after this call returns, as Tachyon may evict blocks
   * from memory at any time.
   * 
   * @param blockIndex The index of the block in the file.
   * @return filename on local file system or null if file not present on local file system.
   * @throws IOException
   */
  public String getLocalFilename(int blockIndex) throws IOException {
    ClientBlockInfo blockInfo = getClientBlockInfo(blockIndex);
    long blockId = blockInfo.getBlockId();
    int blockLockId = mTachyonFS.getBlockLockId();
    String filename = mTachyonFS.lockBlock(blockId, blockLockId);
    if (filename != null) {
      mTachyonFS.unlockBlock(blockId, blockLockId);
    }
    return filename;
  }

  /**
   * Return the net address of all the location hosts
   * 
   * @return the list of those net address, in String
   * @throws IOException
   */
  public List<String> getLocationHosts() throws IOException {
    List<String> ret = new ArrayList<String>();
    if (getNumberOfBlocks() > 0) {
      List<NetAddress> locations = getClientBlockInfo(0).getLocations();
      if (locations != null) {
        for (int k = 0; k < locations.size(); k ++) {
          ret.add(locations.get(k).mHost);
        }
      }
    }

    return ret;
  }

  /**
   * Return the number of blocks the file has.
   * 
   * @return the number of blocks
   * @throws IOException
   */
  public int getNumberOfBlocks() throws IOException {
    return getUnCachedFileStatus().getBlockIds().size();
  }

  /**
   * Return the OutStream of this file, use the specified write type. Always return a FileOutStream.
   * 
   * @param writeType the OutStream's write type
   * @return the OutStream
   * @throws IOException
   */
  public OutStream getOutStream(WriteType writeType) throws IOException {
    if (isComplete()) {
      throw new IOException("Overriding after completion not supported.");
    }

    if (writeType == null) {
      throw new IOException("WriteType can not be null.");
    }

    return new FileOutStream(this, writeType, mUFSConf, mTachyonConf);
  }

  /**
   * Return the path of this file in the Tachyon file system
   * 
   * @return the path
   * @throws IOException
   */
  public String getPath() throws IOException {
    return getUnCachedFileStatus().getPath();
  }

  /**
   * To get the configuration object for UnderFileSystem.
   * 
   * @return configuration object used for concrete ufs instance
   */
  public Object getUFSConf() {
    return mUFSConf;
  }

  /**
   * Return the under filesystem path in the under file system of this file
   * 
   * @return the under filesystem path
   * @throws IOException
   */
  String getUfsPath() throws IOException {
    ClientFileInfo info = getCachedFileStatus();

    if (!info.getUfsPath().isEmpty()) {
      return info.getUfsPath();
    }

    return getUnCachedFileStatus().getUfsPath();
  }

  @Override
  public int hashCode() {
    return mFileId;
  }

  /**
   * Return whether this file is complete or not
   * 
   * @return true if this file is complete, false otherwise
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    return getCachedFileStatus().isComplete || getUnCachedFileStatus().isComplete;
  }

  /**
   * @return true if this is a directory, false otherwise
   * @throws IOException
   */
  public boolean isDirectory() throws IOException {
    return getCachedFileStatus().isFolder;
  }

  /**
   * @return true if this is a file, false otherwise
   * @throws IOException
   */
  public boolean isFile() throws IOException {
    return !isDirectory();
  }

  /**
   * Return whether the file is in memory or not. Note that a file may be partly in memory. This
   * value is true only if the file is fully in memory.
   * 
   * @return true if the file is fully in memory, false otherwise
   * @throws IOException
   */
  public boolean isInMemory() throws IOException {
    return getUnCachedFileStatus().getInMemoryPercentage() == 100;
  }

  /**
   * @return the file size in bytes
   * @throws IOException
   */
  public long length() throws IOException {
    return getUnCachedFileStatus().getLength();
  }

  /**
   * @return true if this file is pinned, false otherwise
   * @throws IOException
   */
  public boolean needPin() throws IOException {
    return getUnCachedFileStatus().isPinned;
  }

  /**
   * Get the owner of the file.
   * 
   * @return owner of the file. The string could be empty if there is no notion of owner of a file
   *         in a filesystem or if it could not be determined (rare).
   */
  public String getOwner() throws IOException {
    return getUnCachedFileStatus().getOwner();
  }

  /**
   * Get the group associated with the file.
   * 
   * @return group for the file. The string could be empty if there is no notion of group of a file
   *         in a filesystem or if it could not be determined (rare).
   */
  public String getGroup() throws IOException {
    return getUnCachedFileStatus().getGroup();
  }

  /**
   * Get a short permission associated with the file.
   * 
   * @return a short permission.
   */
  public short getPermission() throws IOException {
    return (short) getUnCachedFileStatus().getPermission();
  }

  /**
   * Promote block back to top layer after access
   * 
   * @param blockIndex the index of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean promoteBlock(int blockIndex) throws IOException {
    ClientBlockInfo blockInfo = getClientBlockInfo(blockIndex);
    return mTachyonFS.promoteBlock(blockInfo.getBlockId());
  }

  /**
   * Advanced API.
   * 
   * Return a TachyonByteBuffer of the block specified by the blockIndex
   * 
   * @param blockIndex The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException
   */
  public TachyonByteBuffer readByteBuffer(int blockIndex) throws IOException {
    if (!isComplete()) {
      return null;
    }

    TachyonByteBuffer ret = readLocalByteBuffer(blockIndex);
    if (ret == null) {
      // TODO Make it local cache if the OpType is try cache.
      ret = readRemoteByteBuffer(getClientBlockInfo(blockIndex));
    }

    return ret;
  }

  /**
   * Get the the whole block.
   * 
   * @param blockIndex The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException
   */
  TachyonByteBuffer readLocalByteBuffer(int blockIndex) throws IOException {
    return readLocalByteBuffer(blockIndex, 0, -1);
  }

  /**
   * Read local block return a TachyonByteBuffer
   * 
   * @param blockIndex The id of the block.
   * @param offset The start position to read.
   * @param len The length to read. -1 represents read the whole block.
   * @return <code>TachyonByteBuffer</code> containing the block.
   * @throws IOException
   */
  private TachyonByteBuffer readLocalByteBuffer(int blockIndex, long offset, long len)
      throws IOException {
    if (offset < 0) {
      throw new IOException("Offset can not be negative: " + offset);
    }
    if (len < 0 && len != -1) {
      throw new IOException("Length can not be negative except -1: " + len);
    }

    ClientBlockInfo info = getClientBlockInfo(blockIndex);
    long blockId = info.blockId;

    int blockLockId = mTachyonFS.getBlockLockId();
    String localFileName = mTachyonFS.lockBlock(blockId, blockLockId);

    if (localFileName != null) {
      Closer closer = Closer.create();
      try {
        RandomAccessFile localFile = closer.register(new RandomAccessFile(localFileName, "r"));

        long fileLength = localFile.length();
        String error = null;
        if (offset > fileLength) {
          error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
        }
        if (error == null && len != -1 && offset + len > fileLength) {
          error =
              String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
                  len, fileLength);
        }
        if (error != null) {
          throw new IOException(error);
        }

        if (len == -1) {
          len = fileLength - offset;
        }

        FileChannel localFileChannel = closer.register(localFile.getChannel());
        final ByteBuffer buf = localFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, len);
        mTachyonFS.accessLocalBlock(blockId);
        return new TachyonByteBuffer(mTachyonFS, buf, blockId, blockLockId);
      } catch (FileNotFoundException e) {
        LOG.info(localFileName + " is not on local disk.");
      } catch (IOException e) {
        LOG.warn("Failed to read local file " + localFileName + " because:", e);
      } finally {
        closer.close();
      }
    }

    mTachyonFS.unlockBlock(blockId, blockLockId);
    return null;
  }

  /**
   * Get the the whole block from remote workers.
   * 
   * @param blockInfo The blockInfo of the block to read.
   * @return TachyonByteBuffer containing the block.
   */
  TachyonByteBuffer readRemoteByteBuffer(ClientBlockInfo blockInfo) {
    // We call into the remote block in stream class to read a remote byte buffer
    ByteBuffer buf = RemoteBlockInStream.readRemoteByteBuffer(mTachyonFS,
        blockInfo, 0, blockInfo.length, mTachyonConf);
    return (buf == null) ? null : new TachyonByteBuffer(mTachyonFS, buf, blockInfo.blockId, -1);
  }

  // TODO remove this method. do streaming cache. This is not a right API.
  public boolean recache() throws IOException {
    int numberOfBlocks = getNumberOfBlocks();
    if (numberOfBlocks == 0) {
      return true;
    }

    boolean succeed = true;
    for (int k = 0; k < numberOfBlocks; k ++) {
      succeed &= recache(k);
    }

    return succeed;
  }

  /**
   * Re-cache the block into memory
   *
   * @param blockIndex The block index of the current file.
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  boolean recache(int blockIndex) throws IOException {
    String path = getUfsPath();
    UnderFileSystem underFsClient = UnderFileSystem.get(path, mTachyonConf);

    InputStream inputStream = null;
    BlockOutStream bos = null;
    try {
      inputStream = underFsClient.open(path);

      long length = getBlockSizeByte();
      long offset = blockIndex * length;
      inputStream.skip(offset);

      int bufferBytes =
          (int) mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES, Constants.MB) * 4;
      byte[] buffer = new byte[bufferBytes];
      bos = new BlockOutStream(this, WriteType.TRY_CACHE, blockIndex, mTachyonConf);
      int limit;
      while (length > 0 && ((limit = inputStream.read(buffer)) >= 0)) {
        if (limit != 0) {
          if (length >= limit) {
            bos.write(buffer, 0, limit);
            length -= limit;
          } else {
            bos.write(buffer, 0, (int) length);
            length = 0;
          }
        }
      }
      bos.close();
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      if (bos != null) {
        bos.cancel();
      }
      return false;
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }

    return true;
  }

  /**
   * Rename this file
   * 
   * @param path the new name
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public boolean rename(TachyonURI path) throws IOException {
    return mTachyonFS.rename(mFileId, path);
  }

  /**
   * To set the configuration object for UnderFileSystem. The conf object is understood by the
   * concrete underfs' implementation.
   * 
   * @param conf The configuration object accepted by ufs.
   */
  public void setUFSConf(Object conf) {
    mUFSConf = conf;
  }

  @Override
  public String toString() {
    try {
      return getPath();
    } catch (IOException e) {
      throw new RuntimeException("File does not exist anymore: " + mFileId);
    }
  }
}
