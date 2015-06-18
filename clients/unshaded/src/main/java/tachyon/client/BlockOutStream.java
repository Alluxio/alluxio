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
 * <code>BlockOutStream</code> implementation of TachyonFile. This class is not client facing.
 */
public abstract class BlockOutStream extends OutStream {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static BlockOutStream get(TachyonFile tachyonFile, WriteType opType, int blockIndex,
      TachyonConf tachyonConf) throws IOException {
    return get(tachyonFile, opType, blockIndex,
        tachyonConf.getBytes(Constants.USER_QUOTA_UNIT_BYTES, 8 * Constants.MB), tachyonConf);
  }

  public static BlockOutStream get(TachyonFile tachyonFile, WriteType opType, int blockIndex,
      long initialBytes, TachyonConf tachyonConf) throws IOException {

    if (tachyonFile.mTachyonFS.hasLocalWorker()) {
      LOG.info("Writing with local stream.");
      return new LocalBlockOutStream(tachyonFile, opType, blockIndex, initialBytes, tachyonConf);
    }

    LOG.info("Writing with remote stream.");
    throw new IOException("Remote write not supported.");
  }

  public BlockOutStream(TachyonFile tachyonFile, WriteType opType, TachyonConf tachyonConf) {
    super(tachyonFile, opType, tachyonConf);
  }

  public abstract long getRemainingSpaceByte();
}
