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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * <code>BlockInStream</code> interface implementation of TachyonFile. To get an instance of this
 * class, one should call the method <code>getInStream</code> of
 * <code>tachyon.client.TachyonFile</code>, rather than constructing a new instance directly in the
 * client code.
 */
public abstract class BlockInStream extends InStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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
    if (tachyonConf.getBoolean(Constants.USER_ENABLE_LOCAL_READ)) {
      LOG.info("Reading with local stream.");
      TachyonByteBuffer buf = tachyonFile.readLocalByteBuffer(blockIndex);
      if (buf != null) {
//      TODO: Rethink this code path to work with the worker locking design
//      if (readType.isPromote()) {
//        tachyonFile.promoteBlock(blockIndex);
//      }
        return new LocalBlockInStream(tachyonFile, readType, blockIndex, buf, tachyonConf);
      }
    }

    LOG.info("Reading with remote stream.");
    return new RemoteBlockInStream(tachyonFile, readType, blockIndex, ufsConf, tachyonConf);
  }

  protected final int mBlockIndex;

  protected boolean mClosed = false;

  /**
   * @param file
   * @param readType
   * @param blockIndex
   * @param tachyonConf the TachyonConf instance for this file output stream.
   */
  BlockInStream(TachyonFile file, ReadType readType, int blockIndex, TachyonConf tachyonConf) {
    super(file, readType, tachyonConf);
    mBlockIndex = blockIndex;
  }
}
