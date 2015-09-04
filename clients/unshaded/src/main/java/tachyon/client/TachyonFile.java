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
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.underfs.UnderFileSystem;

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
   * Creates a new <code>TachyonFile</code>, based on file id.
   *
   * @param tfs the Tachyon file system client handler
   * @param fid the file id
   * @param tachyonConf the TachyonConf for this file
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
   * Returns the id of a block in the file, specified by blockIndex.
   *
   * @param blockIndex the index of the block in this file
   * @return the block id
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public long getBlockId(int blockIndex) throws IOException {
    return mTachyonFS.getBlockId(mFileId, blockIndex);
  }

  /**
   * Returns the block size of this file.
   *
   * @return the block size in bytes
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public long getBlockSizeByte() throws IOException {
    return getCachedFileStatus().getBlockSizeByte();
  }

  /**
   * Gets a ClientBlockInfo by the file id and block index
   *
   * @param blockIndex The index of the block in the file
   * @return the ClientBlockInfo of the specified block
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public synchronized ClientBlockInfo getClientBlockInfo(int blockIndex) throws IOException {
    return mTachyonFS.getClientBlockInfo(getBlockId(blockIndex));
  }

  /**
   * Returns the creation time of this file
   *
   * @return the creation time, in milliseconds
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public long getCreationTimeMs() throws IOException {
    return getCachedFileStatus().getCreationTimeMs();
  }

  /**
   * @return the replication factor.
   */
  public int getDiskReplication() {
    // TODO(hy): Implement it.
    return 3;
  }

  /**
   * Return the {@code InStream} of this file based on the specified read type. If it has no block,
   * return an {@code EmptyBlockInStream}; if it has only one block, return a {@code BlockInStream}
   * of the block; otherwise return a {@code FileInStream}.
   *
   * @param readType the InStream's read type
   * @return the <code>InStream</code>
   * @throws IOException when an event that prevents the operation from completing is encountered
   */
  public InStream getInStream(ReadType readType) throws IOException {
    if (readType == null) {
      throw new IOException("ReadType can not be null.");
    }

    if (!isComplete()) {
      throw new IOException("The file " + this + " is not complete.");
    }

    if (isDirectory()) {
      throw new IOException("Cannot open a directory for reading.");
    }

    ClientFileInfo fileStatus = getUnCachedFileStatus();
    List<Long> blocks = fileStatus.getBlockIds();

    if (blocks.size() == 0) {
      return new EmptyBlockInStream(this, readType, mTachyonConf);
    }

    if (blocks.size() == 1) {
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
   * @param blockIndex The index of the block in the file
   * @return filename on local file system or null if file not present on local file system
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
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
   * Returns the net address of all the location hosts
   *
   * @return the list of those net address, in String
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public List<String> getLocationHosts() throws IOException {
    List<String> ret = new ArrayList<String>();
    if (getNumberOfBlocks() > 0) {
      List<NetAddress> locations = getClientBlockInfo(0).getLocations();
      if (locations != null) {
        for (NetAddress location : locations) {
          ret.add(location.mHost);
        }
      }
    }

    return ret;
  }

  /**
   * Returns the number of blocks the file has.
   *
   * @return the number of blocks
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public int getNumberOfBlocks() throws IOException {
    return getUnCachedFileStatus().getBlockIds().size();
  }

  /**
   * Returns the {@code OutStream} of this file, use the specified write type. Always return a
   * {@code FileOutStream}.
   *
   * @param writeType the OutStream's write type
   * @return the OutStream
   * @throws IOException when an event that prevents the operation from completing is encountered
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
   * Returns the path of this file in the Tachyon file system.
   *
   * @return the path
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public String getPath() throws IOException {
    return getUnCachedFileStatus().getPath();
  }

  /**
   * Gets the configuration object for UnderFileSystem.
   *
   * @return configuration object used for concrete ufs instance
   */
  public Object getUFSConf() {
    return mUFSConf;
  }

  /**
   * Returns the under filesystem path in the under file system of this file
   *
   * @return the under filesystem path
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
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
   * Returns whether this file is complete or not
   *
   * @return true if this file is complete, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public boolean isComplete() throws IOException {
    return getCachedFileStatus().isComplete || getUnCachedFileStatus().isComplete;
  }

  /**
   * @return true if this is a directory, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public boolean isDirectory() throws IOException {
    return getCachedFileStatus().isFolder;
  }

  /**
   * @return true if this is a file, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public boolean isFile() throws IOException {
    return !isDirectory();
  }

  /**
   * Return whether the file is in memory or not. Note that a file may be partly in memory. This
   * value is true only if the file is fully in memory.
   *
   * @return true if the file is fully in memory, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public boolean isInMemory() throws IOException {
    return getUnCachedFileStatus().getInMemoryPercentage() == 100;
  }

  /**
   * @return the file size in bytes
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public long length() throws IOException {
    return getUnCachedFileStatus().getLength();
  }

  /**
   * @return true if this file is pinned, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public boolean needPin() throws IOException {
    return getUnCachedFileStatus().isPinned;
  }

  /**
   * Promotes block back to top layer after access.
   *
   * @param blockIndex the index of the block
   * @return true if success, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  public boolean promoteBlock(int blockIndex) throws IOException {
    ClientBlockInfo blockInfo = getClientBlockInfo(blockIndex);
    return mTachyonFS.promoteBlock(blockInfo.getBlockId());
  }

  /**
   * Advanced API.
   *
   * Returns a TachyonByteBuffer of the block specified by the blockIndex
   *
   * @param blockIndex The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  @Deprecated
  public TachyonByteBuffer readByteBuffer(int blockIndex) throws IOException {
    if (!isComplete()) {
      return null;
    }

    // TODO(hy): allow user to disable local read for this advanced API
    TachyonByteBuffer ret = readLocalByteBuffer(blockIndex);
    if (ret == null) {
      // TODO(hy): Make it local cache if the OpType is try cache.
      ret = readRemoteByteBuffer(getClientBlockInfo(blockIndex));
    }

    return ret;
  }

  /**
   * Gets the the whole block.
   *
   * @param blockIndex The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  TachyonByteBuffer readLocalByteBuffer(int blockIndex) throws IOException {
    return readLocalByteBuffer(blockIndex, 0, -1);
  }

  /**
   * Read local block return a TachyonByteBuffer
   *
   * @param blockIndex The id of the block
   * @param offset The start position to read
   * @param len The length to read. -1 represents read the whole block
   * @return <code>TachyonByteBuffer</code> containing the block
   * @throws IOException when the offset is negative is the length is less than -1
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
   * @param blockInfo The blockInfo of the block to read
   * @return TachyonByteBuffer containing the block
   * @throws IOException if the underlying stream throws IOException during close()
   */
  TachyonByteBuffer readRemoteByteBuffer(ClientBlockInfo blockInfo) throws IOException {
    // Create a dummy RemoteBlockInstream object.
    RemoteBlockInStream dummyStream = RemoteBlockInStream.getDummyStream();
    // Using the dummy stream to read remote buffer.
    ByteBuffer inBuf = dummyStream.readRemoteByteBuffer(mTachyonFS, blockInfo, 0,
        blockInfo.length, mTachyonConf);
    if (inBuf == null) {
      // Close the stream object.
      dummyStream.close();
      return null;
    }
    // Copy data in network buffer into client buffer.
    ByteBuffer outBuf = ByteBuffer.allocate((int) inBuf.capacity());
    outBuf.put(inBuf);
    // Close the stream object (must be called after buffer is copied)
    dummyStream.close();
    return new TachyonByteBuffer(mTachyonFS, outBuf, blockInfo.blockId, -1);
  }

  /**
   * Re-caches this file into memory.
   *
   * TODO(hy): remove this method. do streaming cache. This is not a right API.
   *
   * @return true if succeed, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
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
   * Re-caches the given block into memory.
   *
   * @param blockIndex The block index of the current file
   * @return true if succeed, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
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
          (int) mTachyonConf.getBytes(Constants.USER_FILE_BUFFER_BYTES);
      byte[] buffer = new byte[bufferBytes];
      bos = BlockOutStream.get(this, WriteType.TRY_CACHE, blockIndex, mTachyonConf);
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
   * Renames this file.
   *
   * @param path the new name
   * @return true if succeed, false otherwise
   * @throws IOException if an event that prevent the operation from completing is encountered
   */
  public boolean rename(TachyonURI path) throws IOException {
    return mTachyonFS.rename(mFileId, path);
  }

  /**
   * To set the configuration object for UnderFileSystem. The conf object is understood by the
   * concrete under file system implementation.
   *
   * @param conf The configuration object accepted by ufs
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
