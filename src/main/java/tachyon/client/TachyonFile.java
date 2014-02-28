/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.worker.DataServerMessage;

/**
 * Tachyon File.
 */
public class TachyonFile implements Comparable<TachyonFile> {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final UserConf USER_CONF = UserConf.get();

  final TachyonFS TFS;
  final int FID;
  final boolean CACHE_ON_READ;

  TachyonFile(TachyonFS tfs, int fid, boolean cacheOnRead) {
    TFS = tfs;
    FID = fid;
    CACHE_ON_READ = cacheOnRead;
  }

  public InStream getInStream(ReadType readType) throws IOException {
    if (readType == null) {
      throw new IOException("ReadType can not be null.");
    }

    if (!isComplete()) {
      throw new IOException("The file " + this + " is not complete.");
    }

    LOG.info("#> CACHE_ON_READ = " + CACHE_ON_READ);

    boolean shouldCache = readType.isCache(CACHE_ON_READ);

    List<Long> blocks = TFS.getFileBlockIdList(FID);

    if (blocks.size() == 0) {
      return new EmptyBlockInStream(this, shouldCache);
    } else if (blocks.size() == 1) {
      return BlockInStream.get(this, readType, 0);
    }

    return new FileInStream(this, shouldCache);
  }

  public OutStream getOutStream(WriteType writeType) throws IOException {
    if (writeType == null) {
      throw new IOException("WriteType can not be null.");
    }

    return new FileOutStream(this, writeType);
  }

  public String getPath() {
    return TFS.getPath(FID);
  }

  public List<String> getLocationHosts() throws IOException {
    List<NetAddress> locations = TFS.getClientBlockInfo(FID, 0).getLocations();
    List<String> ret = new ArrayList<String>(locations.size());
    if (locations != null) {
      for (int k = 0; k < locations.size(); k ++) {
        ret.add(locations.get(k).mHost);
      }
    }

    return ret;
  }

  public boolean isFile() {
    return !TFS.isDirectory(FID);
  }

  public boolean isDirectory() {
    return TFS.isDirectory(FID);
  }

  public boolean isInLocalMemory() {
    throw new RuntimeException("Unsupported");
  }

  public boolean isInMemory() {
    return TFS.isInMemory(FID);
  }

  public boolean isComplete() {
    return TFS.isComplete(FID);
  }

  public long length() {
    return TFS.getFileLength(FID);
  }

  public int getNumberOfBlocks() throws IOException {
    return TFS.getNumberOfBlocks(FID);
  }

  public long getBlockSizeByte() {
    return TFS.getBlockSizeByte(FID);
  }

  public TachyonByteBuffer readByteBuffer() throws IOException {
    if (TFS.getNumberOfBlocks(FID) > 1) {
      throw new IOException("The file has more than one block. This API does not support this.");
    }

    return readByteBuffer(0);
  }

  TachyonByteBuffer readByteBuffer(int blockIndex) throws IOException {
    if (!isComplete()) {
      return null;
    }

    ClientBlockInfo blockInfo = TFS.getClientBlockInfo(FID, blockIndex);

    TachyonByteBuffer ret = readLocalByteBuffer(blockIndex);
    if (ret == null) {
      // TODO Make it local cache if the OpType is try cache.
      ret = readRemoteByteBuffer(blockInfo);
    }

    return ret;
  }

  /**
   * Returns the local filename for the block if that file exists on the local file system. This is
   * an alpha power-api feature for applications that want short-circuit-read files directly. There
   * is no guarantee that the file still exists after this call returns, as Tachyon may evict blocks
   * from memory at any time.
   * 
   * @param blockId
   *          The id of the block.
   * @return filename on local file system or null if file not present on local file system.
   * @throws IOException
   */
  public String getLocalFilename(int blockIndex) throws IOException {
    ClientBlockInfo blockInfo = TFS.getClientBlockInfo(FID, blockIndex);

    return TFS.getLocalFilename(blockInfo.getBlockId());
  }

  /**
   * Get the the whole block.
   * 
   * @param blockIndex
   *          The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException
   */
  TachyonByteBuffer readLocalByteBuffer(int blockIndex) throws IOException {
    return TFS.readLocalByteBuffer(TFS.getClientBlockInfo(FID, blockIndex).blockId);
  }

  TachyonByteBuffer readRemoteByteBuffer(ClientBlockInfo blockInfo) {
    ByteBuffer buf = null;

    LOG.info("Try to find and read from remote workers.");
    try {
      List<NetAddress> blockLocations = blockInfo.getLocations();
      LOG.info("readByteBufferFromRemote() " + blockLocations);

      for (int k = 0; k < blockLocations.size(); k ++) {
        String host = blockLocations.get(k).mHost;
        int port = blockLocations.get(k).mPort;

        // The data is not in remote machine's memory if port == -1.
        if (port == -1) {
          continue;
        }
        if (host.equals(InetAddress.getLocalHost().getHostName())
            || host.equals(InetAddress.getLocalHost().getHostAddress())) {
          String localFileName = TFS.getRootFolder() + "/" + FID;
          LOG.warn("Master thinks the local machine has data " + localFileName + "! But not!");
        } else {
          LOG.info(host + ":" + (port + 1) + " current host is "
              + InetAddress.getLocalHost().getHostName() + " "
              + InetAddress.getLocalHost().getHostAddress());

          try {
            buf = retrieveByteBufferFromRemoteMachine(new InetSocketAddress(host, port + 1),
                blockInfo);
            if (buf != null) {
              break;
            }
          } catch (IOException e) {
            LOG.error(e.getMessage());
            buf = null;
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get read data from remote " + e.getMessage());
    }

    return buf == null ? null : new TachyonByteBuffer(TFS, buf, blockInfo.blockId, -1);
  }

  // TODO remove this method. do streaming cache. This is not a right API.
  public boolean recache() throws IOException {
    int numberOfBlocks = TFS.getNumberOfBlocks(FID);
    if (numberOfBlocks == 0) {
      return true;
    }

    boolean succeed = true;
    for (int k = 0; k < numberOfBlocks; k ++) {
      succeed &= recache(k);
    }

    return succeed;
  }

  boolean recache(int blockIndex) {
    boolean succeed = true;
    String path = TFS.getCheckpointPath(FID);
    UnderFileSystem underFsClient = UnderFileSystem.get(path);

    try {
      InputStream inputStream = underFsClient.open(path);

      long length = TFS.getBlockSizeByte(FID);
      long offset = blockIndex * length;
      inputStream.skip(offset);

      byte buffer[] = new byte[USER_CONF.FILE_BUFFER_BYTES * 4];

      BlockOutStream bos = new BlockOutStream(this, WriteType.TRY_CACHE, blockIndex);
      int limit;
      while (length > 0 && ((limit = inputStream.read(buffer)) >= 0)) {
        if (limit != 0) {
          try {
            if (length >= limit) {
              bos.write(buffer, 0, limit);
              length -= limit;
            } else {
              bos.write(buffer, 0, (int) length);
              length = 0;
            }
          } catch (IOException e) {
            LOG.warn(e);
            succeed = false;
            break;
          }
        }
      }
      if (succeed) {
        bos.close();
      } else {
        bos.cancel();
      }
    } catch (IOException e) {
      LOG.info(e);
      return false;
    }

    return succeed;
  }

  public boolean rename(String path) throws IOException {
    return TFS.rename(FID, path);
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address,
      ClientBlockInfo blockInfo) throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.connect(address);

    LOG.info("Connected to remote machine " + address + " sent");
    long blockId = blockInfo.blockId;
    DataServerMessage sendMsg = DataServerMessage.createBlockRequestMessage(blockId);
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }

    LOG.info("Data " + blockId + " to remote machine " + address + " sent");

    DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId);
    while (!recvMsg.isMessageReady()) {
      int numRead = recvMsg.recv(socketChannel);
      if (numRead == -1) {
        break;
      }
    }
    LOG.info("Data " + blockId + " from remote machine " + address + " received");

    socketChannel.close();

    if (!recvMsg.isMessageReady()) {
      LOG.info("Data " + blockId + " from remote machine is not ready.");
      return null;
    }

    if (recvMsg.getBlockId() < 0) {
      LOG.info("Data " + recvMsg.getBlockId() + " is not in remote machine.");
      return null;
    }

    return recvMsg.getReadOnlyData();
  }

  @Override
  public int hashCode() {
    return getPath().hashCode() ^ 1234321;
  }

  @Override
  public boolean equals(Object obj) {
    if ((obj != null) && (obj instanceof TachyonFile)) {
      return compareTo((TachyonFile) obj) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(TachyonFile o) {
    return getPath().compareTo(o.getPath());
  }

  @Override
  public String toString() {
    return getPath();
  }

  public long getBlockId(int blockIndex) throws IOException {
    return TFS.getBlockId(FID, blockIndex);
  }

  public boolean needPin() {
    return TFS.isNeedPin(FID);
  }

  public int getDiskReplication() {
    // TODO Implement it.
    return 3;
  }

  public long getCreationTimeMs() {
    return TFS.getCreationTimeMs(FID);
  }

  String getCheckpointPath() {
    return TFS.getCheckpointPath(FID);
  }
}