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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.next.CacheType;
import tachyon.client.next.ClientOptions;
import tachyon.client.next.UnderStorageType;
import tachyon.client.next.file.FileInStream;
import tachyon.client.next.file.FileOutStream;
import tachyon.client.next.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileInfo;
import tachyon.thrift.NetAddress;

/**
 * Tachyon File.
 */
public class TachyonFile implements Comparable<TachyonFile> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonFileSystem mTFS;

  final TachyonFS mTachyonFS;
  final long mFileId;

  private Object mUFSConf = null;

  private final TachyonConf mTachyonConf;

  /**
   * Creates a new <code>TachyonFile</code>, based on file id.
   *
   * @param tfs the Tachyon file system client handler
   * @param fid the file id
   * @param tachyonConf the TachyonConf for this file
   */
  TachyonFile(TachyonFS tfs, long fid, TachyonConf tachyonConf) {
    mTachyonFS = tfs;
    mTFS = TachyonFileSystem.get();
    mFileId = fid;
    mTachyonConf = tachyonConf;
  }

  private FileInfo getCachedFileStatus() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, true);
  }

  private FileInfo getUnCachedFileStatus() throws IOException {
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
  public synchronized FileBlockInfo getClientBlockInfo(int blockIndex) throws IOException {
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
  public FileInStream getInStream(ReadType readType) throws IOException {
    if (readType == null) {
      throw new IOException("ReadType can not be null.");
    }

    if (!isComplete()) {
      throw new IOException("The file " + this + " is not complete.");
    }

    if (isDirectory()) {
      throw new IOException("Cannot open a directory for reading.");
    }

    FileInfo info = getUnCachedFileStatus();
    TachyonURI uri = new TachyonURI(info.getPath());
    ClientOptions.Builder optionsBuilder = new ClientOptions.Builder(mTachyonConf);
    optionsBuilder.setBlockSize(info.getBlockSizeByte());
    if (readType.isCache()) {
      optionsBuilder.setCacheType(CacheType.CACHE);
    } else {
      optionsBuilder.setCacheType(CacheType.NO_CACHE);
    }
    return mTFS.getInStream(mTFS.open(uri), optionsBuilder.build());
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
    FileBlockInfo blockInfo = getClientBlockInfo(blockIndex);
    long blockId = blockInfo.blockInfo.getBlockId();
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
    Set<String> ret = Sets.newHashSet();
    if (getNumberOfBlocks() > 0) {
      // under FS locations
      List<NetAddress> underFsLocations = getClientBlockInfo(0).getUnderFsLocations();
      if (underFsLocations != null) {
        for (NetAddress location : underFsLocations) {
          ret.add(location.mHost);
        }
      }
      List<BlockLocation> blockLocations = getClientBlockInfo(0).getBlockInfo().getLocations();
      if (blockLocations != null) {
        for (BlockLocation location : blockLocations) {
          ret.add(location.workerAddress.mHost);
        }
      }
    }

    return new ArrayList<String>(ret);
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
  public FileOutStream getOutStream(WriteType writeType) throws IOException {
    if (isComplete()) {
      throw new IOException("Overriding after completion not supported.");
    }

    if (writeType == null) {
      throw new IOException("WriteType can not be null.");
    }

    FileInfo info = getUnCachedFileStatus();
    ClientOptions.Builder optionsBuilder = new ClientOptions.Builder(mTachyonConf);
    optionsBuilder.setBlockSize(info.getBlockSizeByte());

    if (writeType.isCache()) {
      optionsBuilder.setCacheType(CacheType.CACHE);
    } else {
      optionsBuilder.setCacheType(CacheType.NO_CACHE);
    }
    if (writeType.isThrough()) {
      optionsBuilder.setUnderStorageType(UnderStorageType.PERSIST);
    } else {
      optionsBuilder.setUnderStorageType(UnderStorageType.NO_PERSIST);
    }
    return mTFS.getOutStream(mFileId, optionsBuilder.build());
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
    FileInfo info = getCachedFileStatus();

    if (!info.getUfsPath().isEmpty()) {
      return info.getUfsPath();
    }

    return getUnCachedFileStatus().getUfsPath();
  }

  @Override
  public int hashCode() {
    return Long.valueOf(mFileId).hashCode();
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
    FileBlockInfo blockInfo = getClientBlockInfo(blockIndex);
    return mTachyonFS.promoteBlock(blockInfo.blockInfo.getBlockId());
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
    throw new UnsupportedOperationException("ReadByteBuffer is not supported");
  }

  /**
   * Gets the the whole block.
   *
   * @param blockIndex The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  TachyonByteBuffer readLocalByteBuffer(int blockIndex) throws IOException {
    throw new UnsupportedOperationException("ReadLocalByteBuffer is not supported");
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
    throw new UnsupportedOperationException("ReadLocalByteBuffer is not supported");

  }

  /**
   * Get the the whole block from remote workers.
   *
   * @param blockInfo The blockInfo of the block to read
   * @return TachyonByteBuffer containing the block
   * @throws IOException if the underlying stream throws IOException during close()
   */
  TachyonByteBuffer readRemoteByteBuffer(FileBlockInfo blockInfo) throws IOException {
    throw new UnsupportedOperationException("ReadRemoteByteBuffer is not supported");
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
    throw new UnsupportedOperationException("Recache is not supported");
  }

  /**
   * Re-caches the given block into memory.
   *
   * @param blockIndex The block index of the current file
   * @return true if succeed, false otherwise
   * @throws IOException if the underlying file does not exist or its metadata is corrupted
   */
  boolean recache(int blockIndex) throws IOException {
    throw new UnsupportedOperationException("Recache is not supported");
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
