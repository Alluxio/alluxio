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

package tachyon.worker.lineage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.conf.TachyonConf;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.underfs.UnderFileSystem;
import tachyon.worker.block.BlockDataManager;
import tachyon.worker.block.io.BlockReader;

/**
 * Responsible for managing the lineage storing into under file system.
 */
public final class LineageDataManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block data manager for access block info */
  private final BlockDataManager mBlockDataManager;
  private final List<Long> mPersistedFiles;
  private final TachyonConf mTachyonConf;

  public LineageDataManager(BlockDataManager blockDataManager, TachyonConf tachyonConf) {
    mBlockDataManager = Preconditions.checkNotNull(blockDataManager);
    mPersistedFiles = Lists.newArrayList();
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
  }

  /**
   * Persists the blocks of a file into the under file system.
   *
   * @param fileId the id of the file.
   * @param blockIds the list of block ids.
   * @param filePath the destination path in the under file system.
   * @throws IOException
   */
  public void persistFile(long fileId, List<Long> blockIds, String filePath) throws IOException {
    UnderFileSystem underFs = UnderFileSystem.get(filePath, mTachyonConf);
    OutputStream outputStream = underFs.create(filePath);

    for (long blockId : blockIds) {
      long lockId;
      try {
        lockId = mBlockDataManager.lockBlock(Sessions.CHECKPOINT_SESSION_ID, blockId);
      } catch (NotFoundException ioe) {
        LOG.error("Failed to lock block: " + blockId, ioe);
        throw new IOException(ioe);
      }

      // obtain block reader
      try {
        BlockReader reader;
        try {
          reader =
              mBlockDataManager.readBlockRemote(Sessions.CHECKPOINT_SESSION_ID, blockId, lockId);
        } catch (NotFoundException nfe) {
          throw new IOException(nfe);
        } catch (InvalidStateException ise) {
          throw new IOException(ise);
        }

        // write content out
        ByteBuffer buffer = reader.read(0, reader.getLength());
        outputStream.write(buffer.array());
      } finally {
        try {
          mBlockDataManager.unlockBlock(lockId);
        } catch (NotFoundException nfe) {
          throw new IOException(nfe);
        }
      }
    }

    outputStream.close();
    synchronized (mPersistedFiles) {
      mPersistedFiles.add(fileId);
    }
  }

  public List<Long> fetchPersistedFiles() {
    List<Long> toReturn = Lists.newArrayList();
    synchronized (mPersistedFiles) {
      toReturn.addAll(mPersistedFiles);
      mPersistedFiles.clear();
      return toReturn;
    }
  }
}
