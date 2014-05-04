/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;

/**
 * <code>InputStream</code> interface implementation of TachyonFile. It can only be gotten by
 * calling the methods in <code>tachyon.client.TachyonFile</code>, but can not be initialized by the
 * client code.
 */
public abstract class BlockInStream extends InStream {
  public static BlockInStream get(TachyonFile tachyonFile, ReadType readType, int blockIndex)
      throws IOException {
    return get(tachyonFile, readType, blockIndex, null);
  }
  
  public static BlockInStream get(TachyonFile tachyonFile, ReadType readType, int blockIndex, Object conf)
      throws IOException {
    TachyonByteBuffer buf = tachyonFile.readLocalByteBuffer(blockIndex);
    if (buf != null) {
      return new LocalBlockInStream(tachyonFile, readType, blockIndex, buf);
    }

    return new RemoteBlockInStream(tachyonFile, readType, blockIndex, conf);
  }

  protected final int BLOCK_INDEX;

  protected boolean mClosed = false;

  /**
   * @param file
   * @param readType
   * @param blockIndex
   * @throws IOException
   */
  BlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    super(file, readType);
    BLOCK_INDEX = blockIndex;
  }
}
