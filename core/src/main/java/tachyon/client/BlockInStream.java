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

import tachyon.conf.TachyonConf;

/**
 * <code>InputStream</code> interface implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by the
 * client code.
 */
public abstract class BlockInStream extends InStream {
  /**
   * Get a new BlockInStream of the given block without under file system configuration. The block
   * is decided by the tachyonFile and blockIndex
   * 
   * @param tachyonFile the file the block belongs to
   * @param readType the InStream's read type
   * @param blockIndex the index of the block in the tachyonFile
   * @param tachyonConf the TachyonConf instance for this file output stream.
   * @return A new LocalBlockInStream or RemoteBlockInStream
   * @throws IOException
   */
  public static BlockInStream get(TachyonFile tachyonFile, ReadType readType, int blockIndex,
      TachyonConf tachyonConf) throws IOException {
    return get(tachyonFile, readType, blockIndex, tachyonFile.getUFSConf(), tachyonConf);
  }

  /**
   * Get a new BlockInStream of the given block with the under file system configuration. The block
   * is decided by the tachyonFile and blockIndex
   * 
   * @param tachyonFile the file the block belongs to
   * @param readType the InStream's read type
   * @param blockIndex the index of the block in the tachyonFile
   * @param ufsConf the under file system configuration
   * @return A new LocalBlockInStream or RemoteBlockInStream
   * @throws IOException
   */
  public static BlockInStream get(TachyonFile tachyonFile, ReadType readType, int blockIndex,
      Object ufsConf, TachyonConf tachyonConf) throws IOException {
    TachyonByteBuffer buf = tachyonFile.readLocalByteBuffer(blockIndex);
    if (buf != null) {
      if (readType.isPromote()) {
        tachyonFile.promoteBlock(blockIndex);
      }
      return new LocalBlockInStream(tachyonFile, readType, blockIndex, buf, tachyonConf);
    }

    return new RemoteBlockInStream(tachyonFile, readType, blockIndex, ufsConf, tachyonConf);
  }

  protected final int mBlockIndex;

  protected boolean mClosed = false;

  /**
   * @param file
   * @param readType
   * @param blockIndex
   * @param tachyonConf the TachyonConf instance for this file output stream.
   * @throws IOException
   */
  BlockInStream(TachyonFile file, ReadType readType, int blockIndex, TachyonConf tachyonConf)
      throws IOException {
    super(file, readType, tachyonConf);
    mBlockIndex = blockIndex;
  }
}
