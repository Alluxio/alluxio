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

package tachyon.client.lineage;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.UnderStorageType;
import tachyon.client.block.BufferedBlockOutStream;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.options.OutStreamOptions;

/**
 * A stream API to write a file when lineage is enabled. It supports asynchronous persistence of the
 * data to the under storage system.
 */
@PublicApi
public class LineageFileOutStream extends FileOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private LineageContext mContext;

  public LineageFileOutStream(long fileId, OutStreamOptions options) throws IOException {
    super(fileId, updateOutStreamOptions(options));
    mContext = LineageContext.INSTANCE;
  }

  private static OutStreamOptions updateOutStreamOptions(OutStreamOptions options) {
    // change the under storage type to async
    OutStreamOptions.Builder builder = new OutStreamOptions.Builder(ClientContext.getConf());
    builder.setBlockSizeBytes(options.getBlockSizeBytes());
    builder.setHostname(options.getHostname());
    builder.setTachyonStorageType(options.getTachyonStorageType());
    builder.setTTL(options.getTTL());
    builder.setUnderStorageType(UnderStorageType.ASYNC_PERSIST);
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    if (mCurrentBlockOutStream != null) {
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    boolean canComplete = false;
    if (mNativeStorageType.isStore()) {
      try {
        if (mCanceled) {
          for (BufferedBlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        } else {
          for (BufferedBlockOutStream bos : mPreviousBlockOutStreams) {
            bos.close();
          }
          canComplete = true;
        }
      } catch (IOException ioe) {
        handleCacheWriteException(ioe);
      }
    }

    if (canComplete) {
      LineageMasterClient masterClient = mContext.acquireMasterClient();
      try {
        LOG.info("async complete file" + mFileId);
        masterClient.asyncCompleteFile(mFileId);
      } finally {
        mContext.releaseMasterClient(masterClient);
      }
    } else {
      LOG.warn("Failed to store file " + mFileId + " in Tachyon Storage");
    }
  }
}
